from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from app import dependencies as deps
from app.dependencies import _bearer_token, get_current_user, get_session_service, require_db
from app.models.user import UserModel
from app.services.session_service import SessionService


@pytest.mark.asyncio
async def test_get_current_user_returns_user(monkeypatch: pytest.MonkeyPatch) -> None:
    user_doc = {
        "_id": "507f1f77bcf86cd799439011",
        "username": "playerone",
        "username_display": "PlayerOne",
        "email": "player@example.com",
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
    fake_db = SimpleNamespace(users=SimpleNamespace(find_one=AsyncMock(return_value=user_doc)))

    monkeypatch.setattr(deps, "get_db", lambda: fake_db)
    request = SimpleNamespace(cookies={SessionService.COOKIE_NAME: "sid"})
    session_service = SimpleNamespace(
        get_active_session=AsyncMock(return_value={"user_id": "507f1f77bcf86cd799439011"}),
        delete_session=AsyncMock(),
    )

    user = await get_current_user(request, session_service)

    assert user.username == "playerone"


@pytest.mark.asyncio
async def test_get_current_user_raises_401_without_cookie() -> None:
    request = SimpleNamespace(cookies={})
    session_service = SimpleNamespace(get_active_session=AsyncMock())

    with pytest.raises(HTTPException) as exc_info:
        await get_current_user(request, session_service)

    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_get_current_user_raises_401_for_expired_session() -> None:
    request = SimpleNamespace(cookies={SessionService.COOKIE_NAME: "sid"})
    session_service = SimpleNamespace(get_active_session=AsyncMock(return_value=None))

    with pytest.raises(HTTPException) as exc_info:
        await get_current_user(request, session_service)

    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_get_current_user_raises_401_when_user_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_db = SimpleNamespace(users=SimpleNamespace(find_one=AsyncMock(return_value=None)))

    monkeypatch.setattr(deps, "get_db", lambda: fake_db)
    request = SimpleNamespace(cookies={SessionService.COOKIE_NAME: "sid"})
    session_service = SimpleNamespace(
        get_active_session=AsyncMock(return_value={"user_id": "507f1f77bcf86cd799439011"}),
        delete_session=AsyncMock(),
    )

    with pytest.raises(HTTPException) as exc_info:
        await get_current_user(request, session_service)

    assert exc_info.value.status_code == 401
    session_service.delete_session.assert_awaited_once_with("sid")


def test_require_db_translates_runtime_error_to_503(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom():
        raise RuntimeError("db offline")

    monkeypatch.setattr(deps, "get_db", boom)

    with pytest.raises(HTTPException) as exc_info:
        require_db()

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "Database unavailable"


@pytest.mark.asyncio
async def test_get_session_service_uses_sessions_collection(monkeypatch: pytest.MonkeyPatch) -> None:
    sessions = object()
    monkeypatch.setattr(deps, "get_db", lambda: SimpleNamespace(sessions=sessions))

    service = await get_session_service()

    assert isinstance(service, SessionService)
    assert service._sessions is sessions  # noqa: SLF001


def test_bearer_token_parses_and_rejects_invalid_headers() -> None:
    assert _bearer_token(SimpleNamespace(headers={"authorization": "Bearer ksbot_token.secret"})) == "ksbot_token.secret"
    assert _bearer_token(SimpleNamespace(headers={"authorization": "Basic abc"})) is None
    assert _bearer_token(SimpleNamespace(headers={"authorization": "Bearer "})) is None
    assert _bearer_token(SimpleNamespace()) is None


@pytest.mark.asyncio
async def test_get_current_user_returns_authenticated_bot_for_bearer_token(monkeypatch: pytest.MonkeyPatch) -> None:
    bot_doc = {
        "_id": "507f1f77bcf86cd799439099",
        "username": "randobot",
        "username_display": "RandoBot",
        "email": "randobot@bots.kriegspiel.local",
        "password_hash": "hash",
        "auth_providers": ["bot_token"],
        "profile": {"bio": "bot", "avatar_url": None, "country": None},
        "bot_profile": {"display_name": "RandoBot", "owner_email": "bots@kriegspiel.org"},
        "stats": {"games_played": 0, "games_won": 0, "games_lost": 0, "games_drawn": 0, "elo": 1200, "elo_peak": 1200},
        "settings": {"board_theme": "default", "piece_set": "cburnett", "sound_enabled": False, "auto_ask_any": False},
        "role": "bot",
        "status": "active",
        "last_active_at": datetime.now(UTC),
        "created_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
    }
    bot_user = UserModel.from_mongo(bot_doc)

    class FakeUserService:
        def __init__(self, users) -> None:  # noqa: ANN001
            self.users = users

        async def authenticate_bot_token(self, token: str) -> UserModel | None:
            assert token == "ksbot_token.secret"
            return bot_user

    monkeypatch.setattr(deps, "get_db", lambda: SimpleNamespace(users=object()))
    monkeypatch.setattr(deps, "UserService", FakeUserService)

    user = await get_current_user(
        SimpleNamespace(headers={"authorization": "Bearer ksbot_token.secret"}, cookies={}),
        SimpleNamespace(get_active_session=AsyncMock(), delete_session=AsyncMock()),
    )

    assert user.username == "randobot"


@pytest.mark.asyncio
async def test_get_current_user_falls_back_to_session_when_bearer_token_is_not_a_bot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_doc = {
        "_id": "507f1f77bcf86cd799439011",
        "username": "playerone",
        "username_display": "PlayerOne",
        "email": "player@example.com",
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

    class FakeUserService:
        def __init__(self, users) -> None:  # noqa: ANN001
            self.users = users

        async def authenticate_bot_token(self, token: str) -> UserModel | None:
            assert token == "ksbot_token.secret"
            return None

    fake_db = SimpleNamespace(users=SimpleNamespace(find_one=AsyncMock(return_value=user_doc)))
    monkeypatch.setattr(deps, "get_db", lambda: fake_db)
    monkeypatch.setattr(deps, "UserService", FakeUserService)

    session_service = SimpleNamespace(
        get_active_session=AsyncMock(return_value={"user_id": "507f1f77bcf86cd799439011"}),
        delete_session=AsyncMock(),
    )

    user = await get_current_user(
        SimpleNamespace(
            headers={"authorization": "Bearer ksbot_token.secret"},
            cookies={SessionService.COOKIE_NAME: "sid"},
        ),
        session_service,
    )

    assert user.username == "playerone"
    session_service.get_active_session.assert_awaited_once_with("sid")
