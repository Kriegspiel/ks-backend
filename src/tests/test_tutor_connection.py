from __future__ import annotations

import asyncio
import json
import os
import time
from urllib.parse import parse_qs, urlencode, urlsplit
from unittest.mock import Mock

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jose import jwk, jwt

from app import tutor_login
from app.services.tutor_connection import (
    API,
    MODEL,
    REQUIRED_SCOPES,
    TutorConnection,
    TutorError,
    read_credentials,
    save_credentials,
)


@pytest.fixture
def credential_path(tmp_path):
    path = str(tmp_path / "connection.json")
    save_credentials(
        path,
        {
            "client_id": "oaiapp_tutor",
            "subject": "fil",
            "email": "fil@example.test",
            "ext_agent_host_id": "urn:uuid:test",
            "access_token": "private-access",
            "refresh_token": "private-refresh",
            "scopes": list(REQUIRED_SCOPES),
            "expires_at": time.time() + 3600,
        },
    )
    return path


def factory(handler):
    transport = httpx.MockTransport(handler)
    return lambda **kwargs: httpx.AsyncClient(transport=transport, **kwargs)


def sse(event):
    return "data: " + json.dumps(event) + "\n\n"


def test_credentials_are_private_atomic_and_never_exposed_to_status(credential_path, tmp_path, monkeypatch):
    assert os.stat(credential_path).st_mode & 0o777 == 0o600
    connection = TutorConnection(credential_path)
    status = connection.status()
    assert status == {"connected": True, "account": "fil@example.test", "model": MODEL, "effort": "high"}
    assert "private-" not in str(status)
    os.chmod(credential_path, 0o644)
    assert connection.status()["connected"] is False
    with pytest.raises(TutorError):
        read_credentials(credential_path)
    os.chmod(credential_path, 0o600)
    link = tmp_path / "link.json"
    link.symlink_to(credential_path)
    with pytest.raises(TutorError):
        read_credentials(str(link))
    with pytest.raises(TutorError):
        read_credentials(str(tmp_path))
    value = read_credentials(credential_path)
    value["scopes"] = []
    save_credentials(credential_path, value)
    with pytest.raises(TutorError):
        read_credentials(credential_path)
    monkeypatch.setattr("app.services.tutor_connection.os.replace", Mock(side_effect=OSError("disk")))
    with pytest.raises(OSError):
        save_credentials(credential_path, value)
    assert not list(tmp_path.glob(".tutor-*"))


@pytest.mark.asyncio
async def test_subscription_stream_contract_and_only_completed_response_succeeds(credential_path):
    requests = []

    def handle(request):
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"models": [{"slug": MODEL}]})
        body = json.loads(request.content)
        assert body["model"] == MODEL and body["reasoning"] == {"effort": "high"}
        assert body["stream"] is True and body["store"] is False
        assert set(body) == {"model", "reasoning", "instructions", "input", "tools", "store", "stream"}
        assert str(request.url) == API + "/responses"
        return httpx.Response(
            200,
            text="event: response\n\n"
            + sse({"type": "response.output_text.delta", "delta": "part"})
            + sse({"type": "response.completed", "response": {"status": "completed", "output": []}}),
        )

    connection = TutorConnection(credential_path, client_factory=factory(handle))
    for _ in range(2):
        assert await connection.respond(instructions="Teach", inputs=[{"role": "user", "content": "help"}], tools=[]) == {
            "status": "completed",
            "output": [],
        }
    assert sum(request.method == "GET" for request in requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event,code",
    [
        (
            {"type": "response.failed", "response": {"error": {"code": "subscription_sharing_usage_limit_exceeded"}}},
            "TUTOR_PLAN_LIMIT",
        ),
        ({"type": "response.failed", "response": {"error": None}}, "TUTOR_CONNECTION_UNAVAILABLE"),
        ({"type": "response.incomplete"}, "TUTOR_RESPONSE_INCOMPLETE"),
        ({"type": "error"}, "TUTOR_RESPONSE_INCOMPLETE"),
        ({"type": "response.completed", "response": {"status": "incomplete"}}, "TUTOR_RESPONSE_INCOMPLETE"),
    ],
)
async def test_stream_failures_are_not_cached_or_retried(credential_path, event, code):
    calls = []

    def handle(request):
        calls.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"models": [{"slug": MODEL}]})
        return httpx.Response(200, text=sse(event))

    connection = TutorConnection(credential_path, client_factory=factory(handle))
    with pytest.raises(TutorError) as exc:
        await connection.respond(instructions="", inputs=[], tools=[])
    assert exc.value.code == code
    assert sum(request.method == "POST" for request in calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,models,expected", [(401, [], "TUTOR_CONNECTION_UNAVAILABLE"), (200, [], "TUTOR_MODEL_UNAVAILABLE")]
)
async def test_model_access_is_checked_without_generating_tokens(credential_path, status, models, expected):
    connection = TutorConnection(
        credential_path, client_factory=factory(lambda _request: httpx.Response(status, json={"models": models}))
    )
    with pytest.raises(TutorError) as exc:
        await connection.respond(instructions="", inputs=[], tools=[])
    assert exc.value.code == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("body,status", [("data: [DONE]\n\n", 200), ("x" * 2000001, 200), ("", 403)])
async def test_incomplete_oversize_and_http_failure(credential_path, body, status):
    def handle(request):
        if request.method == "GET":
            return httpx.Response(200, json={"models": [{"slug": MODEL}]})
        return httpx.Response(status, text=body)

    connection = TutorConnection(credential_path, client_factory=factory(handle))
    with pytest.raises(TutorError):
        await connection.respond(instructions="", inputs=[], tools=[])


@pytest.mark.asyncio
async def test_token_rotation_is_serialized_and_saved_atomically(credential_path):
    value = read_credentials(credential_path)
    value["expires_at"] = 0
    save_credentials(credential_path, value)
    refreshes = []

    def handle(request):
        if request.method == "POST":
            refreshes.append(request)
            assert b"grant_type=refresh_token" in request.content
            return httpx.Response(200, json={"access_token": "new-access", "refresh_token": "new-refresh", "expires_in": 3600})
        assert request.headers["Authorization"] == "Bearer new-access"
        return httpx.Response(200, json={"models": [{"slug": MODEL}]})

    connection = TutorConnection(credential_path, client_factory=factory(handle))
    assert await asyncio.gather(connection._token(), connection._token()) == ["new-access", "new-access"]
    assert len(refreshes) == 1 and read_credentials(credential_path)["refresh_token"] == "new-refresh"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,body", [(401, {}), (200, {}), (200, {"access_token": "a", "refresh_token": "r", "scope": "openid"})]
)
async def test_failed_refresh_never_uses_api_key_or_overwrites_registration(credential_path, status, body):
    value = read_credentials(credential_path)
    value["expires_at"] = 0
    save_credentials(credential_path, value)
    connection = TutorConnection(credential_path, client_factory=factory(lambda _request: httpx.Response(status, json=body)))
    with pytest.raises(TutorError):
        await connection._token()
    assert read_credentials(credential_path)["access_token"] == "private-access"


def login_factory(login, *, subject="fil", nonce=None, scopes=None, status=200, tokens=True):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    from cryptography.hazmat.primitives import serialization

    pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    public = jwk.construct(
        key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo), "RS256"
    ).to_dict()
    token = jwt.encode(
        {
            "iss": "https://auth.openai.com",
            "aud": "oaiapp_tutor",
            "sub": subject,
            "exp": time.time() + 3600,
            "nonce": login.nonce if nonce is None else nonce,
            "email": "fil@example.test",
        },
        pem,
        algorithm="RS256",
    )

    def handle(request):
        if request.method == "GET":
            return httpx.Response(200, json={"keys": [public]})
        return httpx.Response(
            status,
            json={
                "id_token": token,
                "access_token": "a",
                "refresh_token": "r" if tokens else None,
                "scope": " ".join(REQUIRED_SCOPES) if scopes is None else scopes,
            },
        )

    transport = httpx.MockTransport(handle)
    return lambda **kwargs: httpx.Client(transport=transport, **kwargs)


def callback(login, **values):
    return "/auth/callback?" + urlencode({"state": login.state, "code": "private-code", "client_id": "oaiapp_tutor", **values})


def test_official_login_validates_pkce_state_identity_and_scopes(tmp_path):
    path = str(tmp_path / "connection.json")
    login = tutor_login.Login(path, 1455)
    query = parse_qs(urlsplit(login.url()).query)
    assert query["client_id"] == ["dynamic_agent_client"] and query["agent_name_hint"] == ["Kriegspiel Tutor"]
    assert query["redirect_uri"] == ["http://127.0.0.1:1455/auth/callback"]
    assert query["code_challenge_method"] == ["S256"]
    login.complete(callback(login), login_factory(login))
    stored = read_credentials(path)
    assert stored["subject"] == "fil" and stored["client_id"] == "oaiapp_tutor"
    returning = tutor_login.Login(path, 1455)
    assert returning.host_id == login.host_id
    assert "agent_name_hint" not in parse_qs(urlsplit(returning.url()).query)
    returning.complete(callback(returning), login_factory(returning))
    with pytest.raises(ValueError):
        login.complete(callback(login))


@pytest.mark.parametrize(
    "change",
    [
        "state",
        "duplicate",
        "path",
        "expired",
        "declined",
        "client",
        "missing-client",
        "missing-code",
        "exchange",
        "nonce",
        "scopes",
        "refresh",
    ],
)
def test_login_rejects_invalid_or_partial_authorization_without_credentials(tmp_path, change):
    login = tutor_login.Login(str(tmp_path / "connection.json"), 1455)
    url = callback(login)
    client = login_factory(login)
    if change == "state":
        url = callback(login, state="wrong")
    elif change == "duplicate":
        url += "&state=other"
    elif change == "path":
        url = url.replace("/auth/callback", "/bad")
    elif change == "expired":
        login.expires = 0
    elif change == "declined":
        url = callback(login, error="access_denied")
    elif change == "client":
        url = callback(login, client_id="untrusted")
    elif change == "missing-client":
        url = "/auth/callback?" + urlencode({"state": login.state, "code": "code"})
    elif change == "missing-code":
        url = "/auth/callback?" + urlencode({"state": login.state, "client_id": "oaiapp_tutor"})
    elif change == "exchange":
        client = login_factory(login, status=400)
    elif change == "nonce":
        client = login_factory(login, nonce="wrong")
    elif change == "scopes":
        client = login_factory(login, scopes="openid")
    elif change == "refresh":
        client = login_factory(login, tokens=False)
    with pytest.raises(ValueError):
        login.complete(url, client)
    assert not (tmp_path / "connection.json").exists()


def test_returning_login_cannot_replace_account(credential_path):
    login = tutor_login.Login(credential_path, 1455)
    with pytest.raises(ValueError):
        login.complete(callback(login), login_factory(login, subject="someone-else"))
    assert read_credentials(credential_path)["subject"] == "fil"


def test_login_main_timeout_and_safe_loopback_output(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["tutor_login", "--credentials", str(tmp_path / "connection.json")])

    class Server:
        def __init__(self, address, handler):
            assert address == ("127.0.0.1", 1455)
            self.handler = handler

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def handle_request(self):
            monkeypatch.setattr(tutor_login.time, "monotonic", lambda: float("inf"))

    monkeypatch.setattr(tutor_login, "HTTPServer", Server)
    with pytest.raises(SystemExit, match="timed out"):
        tutor_login.main()
    assert "private-refresh" not in capsys.readouterr().out


@pytest.mark.parametrize("mode", ["host", "failure", "success"])
def test_login_loopback_handler_checks_host_and_redacts_failures(tmp_path, monkeypatch, mode):
    from io import BytesIO
    from types import SimpleNamespace

    login = SimpleNamespace(used=False, expires=float("inf"), url=lambda: "https://auth.openai.com/authorize")

    def complete(_path):
        login.used = True
        if mode == "failure":
            raise ValueError("never display private provider details")

    login.complete = complete
    monkeypatch.setattr(tutor_login, "Login", lambda *_args: login)
    monkeypatch.setattr("sys.argv", ["tutor_login", "--credentials", str(tmp_path / "connection.json")])
    handlers = []

    class Server:
        def __init__(self, _address, handler):
            self.handler = handler

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def handle_request(self):
            handler = self.handler.__new__(self.handler)
            handler.headers = {"Host": "wrong" if mode == "host" else "127.0.0.1:1455"}
            handler.path = "/auth/callback?private=never-log"
            handler.wfile = BytesIO()
            handler.send_error = Mock()
            handler.send_response = Mock()
            handler.send_header = Mock()
            handler.end_headers = Mock()
            handler.log_message("private=never-log")
            handler.do_GET()
            handlers.append(handler)
            login.used = True

    monkeypatch.setattr(tutor_login, "HTTPServer", Server)
    tutor_login.main()
    handler = handlers[0]
    if mode == "host":
        handler.send_error.assert_called_once_with(400)
    else:
        handler.send_response.assert_called_once_with(400 if mode == "failure" else 200)
        assert b"private" not in handler.wfile.getvalue()
        assert ("Cache-Control", "no-store") in [call.args for call in handler.send_header.call_args_list]
