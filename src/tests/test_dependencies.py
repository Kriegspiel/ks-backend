from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from app.dependencies import get_current_user
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

    from app import dependencies as deps

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

    from app import dependencies as deps

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
