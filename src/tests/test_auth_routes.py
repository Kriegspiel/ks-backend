from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException, status
from fastapi.testclient import TestClient

from app.config import Settings
from app.db import get_db
from app.dependencies import get_current_user, get_session_service
from app.main import create_app
from app.models.user import UserModel
from app.services.session_service import SessionService


def _user_doc() -> dict:
    return {
        "_id": "507f1f77bcf86cd799439011",
        "username": "playerone",
        "username_display": "PlayerOne",
        "email": "player@example.com",
        "email_verified": False,
        "email_verification_sent_at": None,
        "email_verified_at": None,
        "password_hash": "hash",
        "auth_providers": ["local"],
        "profile": {"bio": "", "avatar_url": None, "country": None},
        "stats": {"games_played": 0, "games_won": 0, "games_lost": 0, "games_drawn": 0, "elo": 1200, "elo_peak": 1200},
        "settings": {"board_theme": "default", "piece_set": "cburnett", "sound_enabled": True, "auto_ask_any": False},
        "role": "user",
        "status": "active",
        "last_active_at": datetime.now(UTC),
        "created_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
    }


def _guest_doc() -> dict:
    payload = _user_doc()
    payload["_id"] = "507f1f77bcf86cd799439099"
    payload["username"] = "guest_adolf_adams"
    payload["username_display"] = "guest_adolf_adams"
    payload["email"] = "guest_adolf_adams@guests.kriegspiel.local"
    payload["email_verified"] = True
    payload["email_verified_at"] = datetime.now(UTC)
    payload["auth_providers"] = ["guest"]
    payload["role"] = "guest"
    return payload


@pytest.fixture
def app_no_db(monkeypatch: pytest.MonkeyPatch):
    fake_users = SimpleNamespace(find_one=AsyncMock(return_value=None), insert_one=AsyncMock())
    fake_db = SimpleNamespace(users=fake_users)

    from app.routers import auth as auth_router_module

    monkeypatch.setattr(auth_router_module, "require_db", lambda: fake_db)
    app = create_app(Settings(ENVIRONMENT="testing"))
    return app, fake_users


def test_register_and_login_set_cookie_and_errors(app_no_db, monkeypatch: pytest.MonkeyPatch) -> None:
    app, _fake_users = app_no_db

    from app.routers import auth as auth_router_module
    from app.services.user_service import UserConflictError

    created_user = UserModel.from_mongo(_user_doc())
    guest_user = UserModel.from_mongo(_guest_doc())
    service = SimpleNamespace(
        create_user=AsyncMock(return_value=created_user),
        create_guest_user=AsyncMock(return_value=guest_user),
        authenticate=AsyncMock(return_value=created_user),
    )

    class FakeUserService:
        def __init__(self, _users):
            self._users = _users

        async def create_user(self, payload):
            return await service.create_user(payload)

        async def create_guest_user(self):
            return await service.create_guest_user()

        async def authenticate(self, username, password):
            return await service.authenticate(username, password)

    monkeypatch.setattr(auth_router_module, "UserService", FakeUserService)

    session_service = SimpleNamespace(
        create_session=AsyncMock(return_value="sess123"),
        delete_session=AsyncMock(),
    )

    app.dependency_overrides[get_session_service] = lambda: session_service
    with TestClient(app) as client:
        register = client.post(
            "/api/auth/register",
            json={"username": "PlayerOne", "email": "player@example.com", "password": "abc12345"},
        )
        assert register.status_code == 201
        assert "session_id=sess123" in register.headers.get("set-cookie", "")

        login = client.post("/api/auth/login", json={"username": "playerone", "password": "abc12345"})
        assert login.status_code == 200

        guest = client.post("/api/auth/guest")
        assert guest.status_code == 201
        assert guest.json()["username"] == "guest_adolf_adams"
        guest_cookie = guest.headers.get("set-cookie", "")
        assert "session_id=sess123" in guest_cookie
        assert f"Max-Age={SessionService.GUEST_SESSION_MAX_AGE_SECONDS}" in guest_cookie

        service.authenticate = AsyncMock(return_value=None)
        invalid = client.post("/api/auth/login", json={"username": "playerone", "password": "wrong"})
        assert invalid.status_code == 401

        service.create_user = AsyncMock(
            side_effect=UserConflictError(field="username", code="USERNAME_TAKEN", message="Username already exists")
        )
        conflict = client.post(
            "/api/auth/register",
            json={"username": "PlayerOne", "email": "other@example.com", "password": "abc12345"},
        )
        assert conflict.status_code == 409

        logout = client.post("/api/auth/logout")
        assert logout.status_code == 200


def test_me_endpoint_uses_current_user_dependency(app_no_db) -> None:
    app, _ = app_no_db
    app.dependency_overrides[get_current_user] = lambda: UserModel.from_mongo(_user_doc())

    with TestClient(app) as client:
        me = client.get("/api/auth/me")

    assert me.status_code == 200
    body = me.json()
    assert body["username"] == "playerone"
    assert body["email"] == "player@example.com"
    assert body["is_guest"] is False


def test_cookie_secure_flag_in_production(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_users = SimpleNamespace(find_one=AsyncMock(return_value=None), insert_one=AsyncMock())
    fake_db = SimpleNamespace(users=fake_users)
    from app.routers import auth as auth_router_module

    monkeypatch.setattr(auth_router_module, "require_db", lambda: fake_db)

    created_user = UserModel.from_mongo(_user_doc())

    class FakeUserService:
        def __init__(self, _users):
            self._users = _users

        async def create_user(self, payload):
            return created_user

        async def authenticate(self, username, password):
            return created_user

    monkeypatch.setattr(auth_router_module, "UserService", FakeUserService)

    session_service = SimpleNamespace(create_session=AsyncMock(return_value="sess123"), delete_session=AsyncMock())

    app = create_app(Settings(ENVIRONMENT="production"))
    app.dependency_overrides[get_session_service] = lambda: session_service
    with TestClient(app) as client:
        register = client.post(
            "/api/auth/register",
            json={"username": "PlayerOne", "email": "player@example.com", "password": "abc12345"},
        )

    assert register.status_code == 201
    assert "Secure" in register.headers.get("set-cookie", "")


def test_auth_endpoints_return_503_when_db_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))

    from app.routers import auth as auth_router_module

    session_service = SimpleNamespace(
        create_session=AsyncMock(return_value="sess123"),
        delete_session=AsyncMock(),
    )

    app.dependency_overrides[get_session_service] = lambda: session_service
    monkeypatch.setattr(
        auth_router_module,
        "require_db",
        lambda: (_ for _ in ()).throw(
            HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Database unavailable")
        ),
    )

    with TestClient(app) as client:
        register = client.post(
            "/api/auth/register",
            json={"username": "PlayerOne", "email": "player@example.com", "password": "abc12345"},
        )
        assert register.status_code == 503
        assert register.json()["detail"] == "Database unavailable"

        login = client.post("/api/auth/login", json={"username": "playerone", "password": "abc12345"})
        assert login.status_code == 503
        assert login.json()["detail"] == "Database unavailable"

        guest = client.post("/api/auth/guest")
        assert guest.status_code == 503
        assert guest.json()["detail"] == "Database unavailable"


def test_bot_register_requires_matching_registration_key() -> None:
    app = create_app(Settings(ENVIRONMENT="testing", BOT_REGISTRATION_KEY="expected-key"))

    with TestClient(app) as client:
        missing = client.post(
            "/api/auth/bots/register",
            json={
                "username": "randobot",
                "display_name": "Random Bot",
                "owner_email": "owner@example.com",
                "description": "bot",
            },
        )
        invalid = client.post(
            "/api/auth/bots/register",
            headers={"x-bot-registration-key": "wrong-key"},
            json={
                "username": "randobot",
                "display_name": "Random Bot",
                "owner_email": "owner@example.com",
                "description": "bot",
            },
        )

    assert missing.status_code == 401
    assert invalid.status_code == 401


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("RUN_MONGO_INTEGRATION") != "1",
    reason="Set RUN_MONGO_INTEGRATION=1 to run auth route integration tests",
)
@pytest.mark.asyncio
async def test_register_requires_username_email_password(test_client) -> None:
    response = await test_client.post("/api/auth/register", json={"username": "playerone", "password": "abc12345"})

    assert response.status_code == 422


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("RUN_MONGO_INTEGRATION") != "1",
    reason="Set RUN_MONGO_INTEGRATION=1 to run auth route integration tests",
)
@pytest.mark.asyncio
async def test_register_creates_session_cookie_and_me_logout_flow(test_client) -> None:
    register = await test_client.post(
        "/api/auth/register",
        json={"username": "PlayerOne", "email": "player@example.com", "password": "abc12345"},
    )

    assert register.status_code == 201
    assert register.json()["username"] == "playerone"

    me = await test_client.get("/api/auth/me")
    assert me.status_code == 200
    assert me.json()["email"] == "player@example.com"

    logout = await test_client.post("/api/auth/logout")
    assert logout.status_code == 200
    assert logout.json() == {"message": "Logged out"}

    me_after = await test_client.get("/api/auth/me")
    assert me_after.status_code == 401


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("RUN_MONGO_INTEGRATION") != "1",
    reason="Set RUN_MONGO_INTEGRATION=1 to run auth route integration tests",
)
@pytest.mark.asyncio
async def test_register_conflict_and_login_failure_and_success(test_client) -> None:
    first = await test_client.post(
        "/api/auth/register",
        json={"username": "PlayerOne", "email": "player@example.com", "password": "abc12345"},
    )
    assert first.status_code == 201

    dup = await test_client.post(
        "/api/auth/register",
        json={"username": "playerone", "email": "other@example.com", "password": "abc12345"},
    )
    assert dup.status_code == 409

    bad_login = await test_client.post("/api/auth/login", json={"username": "playerone", "password": "wrongpass123"})
    assert bad_login.status_code == 401

    good_login = await test_client.post("/api/auth/login", json={"username": "PLAYERONE", "password": "abc12345"})
    assert good_login.status_code == 200


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("RUN_MONGO_INTEGRATION") != "1",
    reason="Set RUN_MONGO_INTEGRATION=1 to run auth route integration tests",
)
@pytest.mark.asyncio
async def test_me_returns_401_for_expired_session(test_client) -> None:
    register = await test_client.post(
        "/api/auth/register",
        json={"username": "PlayerTwo", "email": "playertwo@example.com", "password": "abc12345"},
    )
    assert register.status_code == 201

    db = get_db()
    user = await db.users.find_one({"username": "playertwo"})
    expired_id = "expired-session-id"
    await db.sessions.insert_one(
        {
            "_id": expired_id,
            "user_id": user["_id"],
            "username": user["username"],
            "ip": "127.0.0.1",
            "user_agent": "pytest",
            "created_at": datetime.now(UTC) - timedelta(days=31),
            "expires_at": datetime.now(UTC) - timedelta(minutes=1),
        }
    )

    test_client.cookies.set(SessionService.COOKIE_NAME, expired_id)
    me = await test_client.get("/api/auth/me")
    assert me.status_code == 401

    expired_doc = await db.sessions.find_one({"_id": expired_id})
    assert expired_doc is None
