"""Operator-only official ChatGPT authorization, using an SSH-forwarded loopback callback.

Run as the backend service user. Neither this helper nor its credentials are exposed as agent tools.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import secrets
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit
from uuid import uuid4

import httpx
from jose import jwt

from app.services.tutor_connection import API, REQUIRED_SCOPES, TOKEN_ENDPOINT, read_credentials, save_credentials, TutorError

ISSUER = "https://auth.openai.com"
JWKS = f"{ISSUER}/.well-known/jwks.json"
AUTHORIZE = f"{ISSUER}/api/accounts/authorize"


class Login:
    def __init__(self, path: str, port: int):
        self.path = path
        host_path = Path(path).with_name("host.json")
        if host_path.exists():
            self.host_id = json.loads(host_path.read_text())["ext_agent_host_id"]
        else:
            self.host_id = f"urn:uuid:{uuid4()}"
            save_credentials(str(host_path), {"ext_agent_host_id": self.host_id})
        try:
            self.previous = read_credentials(path)
        except TutorError:
            self.previous = None
        self.client_id = self.previous["client_id"] if self.previous else "dynamic_agent_client"
        self.state, self.nonce, self.verifier = (secrets.token_urlsafe(40) for _ in range(3))
        self.redirect = f"http://127.0.0.1:{port}/auth/callback"
        self.expires = time.monotonic() + 600
        self.used = False

    def url(self) -> str:
        params = {
            "client_id": self.client_id,
            "ext_agent_host_id": self.host_id,
            "response_type": "code",
            "redirect_uri": self.redirect,
            "scope": "openid profile email offline_access resource.invoke chatgpt.tokens.use.direct",
            "resource": API,
            "state": self.state,
            "nonce": self.nonce,
            "code_challenge_method": "S256",
            "code_challenge": base64.urlsafe_b64encode(hashlib.sha256(self.verifier.encode()).digest()).rstrip(b"=").decode(),
        }
        if self.previous is None:
            params["agent_name_hint"] = "Kriegspiel Tutor"
        return AUTHORIZE + "?" + urlencode(params)

    def complete(self, callback: str, client_factory=httpx.Client) -> None:
        parts = urlsplit(callback)
        values = parse_qs(parts.query)
        if self.used or time.monotonic() > self.expires or parts.path != "/auth/callback":
            raise ValueError("This sign-in attempt expired or was already used.")
        if any(len(value) != 1 for value in values.values()) or not hmac.compare_digest(
            values.get("state", [""])[0], self.state
        ):
            raise ValueError("Invalid sign-in state.")
        self.used = True
        if "error" in values:
            raise ValueError("ChatGPT authorization was declined. Start a new sign-in to retry.")
        issued = values.get("client_id", [self.client_id])[0]
        if not issued.startswith("oaiapp_") or (self.previous and issued != self.client_id) or not values.get("code"):
            raise ValueError("ChatGPT did not return a valid client registration.")
        with client_factory(timeout=25.0, follow_redirects=False) as client:
            response = client.post(
                TOKEN_ENDPOINT,
                data={
                    "grant_type": "authorization_code",
                    "client_id": issued,
                    "code": values["code"][0],
                    "code_verifier": self.verifier,
                    "redirect_uri": self.redirect,
                    "resource": API,
                },
            )
            if response.status_code != 200:
                raise ValueError("ChatGPT code exchange failed. Start a new sign-in.")
            token = response.json()
            key_response = client.get(JWKS)
            key_response.raise_for_status()
            identity = jwt.decode(
                token["id_token"],
                key_response.json(),
                algorithms=["RS256"],
                audience=issued,
                issuer=ISSUER,
                options={"require_exp": True, "require_sub": True},
            )
        if not hmac.compare_digest(str(identity.get("nonce", "")), self.nonce):
            raise ValueError("Invalid identity nonce.")
        if self.previous and identity["sub"] != self.previous["subject"]:
            raise ValueError("Sign in with the previously connected ChatGPT account.")
        scopes = token.get("scope", "").split()
        if not REQUIRED_SCOPES.issubset(scopes) or not token.get("refresh_token") or not token.get("access_token"):
            raise ValueError("ChatGPT plan usage and renewable-session permissions are required.")
        save_credentials(
            self.path,
            {
                "subject": identity["sub"],
                "email": identity.get("email", "ChatGPT"),
                "client_id": issued,
                "ext_agent_host_id": self.host_id,
                "issuer": ISSUER,
                "scopes": scopes,
                "id_token": token["id_token"],
                "access_token": token["access_token"],
                "refresh_token": token["refresh_token"],
                "expires_at": time.time() + float(token.get("expires_in", 3600)),
            },
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--credentials", required=True)
    parser.add_argument("--port", type=int, default=1455)
    args = parser.parse_args()
    login = Login(args.credentials, args.port)

    class Callback(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass  # OAuth codes and state must never enter access logs.

        def do_GET(self):
            if self.headers.get("Host") != f"127.0.0.1:{args.port}":
                self.send_error(400)
                return
            try:
                login.complete(self.path)
            except Exception:
                self.send_response(400)
                message = b"Sign-in failed. Restart the connection command to try again."
            else:
                self.send_response(200)
                message = b"Kriegspiel Tutor is connected. You may close this window."
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Referrer-Policy", "no-referrer")
            self.end_headers()
            self.wfile.write(message)

    with HTTPServer(("127.0.0.1", args.port), Callback) as server:
        server.timeout = 1
        print("Open this address in the browser on the computer with the SSH port forward:", flush=True)
        print(login.url(), flush=True)
        while not login.used and time.monotonic() <= login.expires:
            server.handle_request()
    if not login.used:
        raise SystemExit("Sign-in timed out. Run the command again when ready.")
    print("Sign-in attempt finished. Check Tutor connection status in the app.")


if __name__ == "__main__":  # pragma: no cover - exercised through main with a fake loopback server
    main()
