from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.config import Settings
from app.dependencies import get_session_service
from app.main import create_app
from app.models.user import UserModel


def _user_doc() -> dict:
    now = datetime.now(UTC)
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
        "last_active_at": now,
        "created_at": now,
        "updated_at": now,
    }


def test_register_sets_cookie_flags(monkeypatch) -> None:
    from app.routers import auth as auth_module

    app = create_app(Settings(ENVIRONMENT="production"))
    created_user = UserModel.from_mongo(_user_doc())

    class FakeUserService:
        def __init__(self, _users):
            self._users = _users

        async def create_user(self, _payload):
            return created_user

        async def authenticate(self, _username, _password):
            return created_user

    monkeypatch.setattr(auth_module, "UserService", FakeUserService)
    monkeypatch.setattr(auth_module, "require_db", lambda: SimpleNamespace(users=object()))

    session_service = SimpleNamespace(create_session=AsyncMock(return_value="sess-secure"), delete_session=AsyncMock())
    app.dependency_overrides[get_session_service] = lambda: session_service

    with TestClient(app) as client:
        response = client.post(
            "/api/auth/register", json={"username": "PlayerOne", "email": "player@example.com", "password": "abc12345"}
        )

    assert response.status_code == 201
    cookie = response.headers.get("set-cookie", "")
    assert "session_id=sess-secure" in cookie
    assert "HttpOnly" in cookie
    assert "SameSite=lax" in cookie
    assert "Secure" in cookie


def test_invalid_or_expired_session_returns_401(monkeypatch) -> None:
    from app.routers import auth as auth_module

    app = create_app(Settings(ENVIRONMENT="testing"))
    monkeypatch.setattr(
        auth_module, "require_db", lambda: SimpleNamespace(users=SimpleNamespace(find_one=AsyncMock(return_value=None)))
    )

    session_service = SimpleNamespace(get_active_session=AsyncMock(return_value=None), delete_session=AsyncMock())
    app.dependency_overrides[get_session_service] = lambda: session_service

    with TestClient(app) as client:
        client.cookies.set("session_id", "expired-or-invalid")
        response = client.get("/api/auth/me")

    assert response.status_code == 401
    assert response.json()["detail"] == "Authentication required"


def test_session_endpoint_reissues_cookie_with_flags(monkeypatch) -> None:
    from app.dependencies import get_current_user

    app = create_app(Settings(ENVIRONMENT="testing"))
    app.dependency_overrides[get_current_user] = lambda: UserModel.from_mongo(_user_doc())

    with TestClient(app) as client:
        client.cookies.set("session_id", "sid-refresh")
        response = client.get("/api/auth/session")

    assert response.status_code == 200
    assert response.json()["authenticated"] is True
    cookie = response.headers.get("set-cookie", "")
    assert "session_id=sid-refresh" in cookie
    assert "HttpOnly" in cookie
    assert "SameSite=lax" in cookie
