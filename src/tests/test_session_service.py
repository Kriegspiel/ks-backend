from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.services.session_service import SessionService


@pytest.mark.asyncio
async def test_create_session_inserts_expected_document() -> None:
    sessions = SimpleNamespace(insert_one=AsyncMock())
    service = SessionService(sessions)
    user = SimpleNamespace(id="507f1f77bcf86cd799439011", username="playerone")

    session_id = await service.create_session(user=user, ip="127.0.0.1", user_agent="pytest")

    assert isinstance(session_id, str)
    assert len(session_id) == 64
    assert sessions.insert_one.await_count == 1
    payload = sessions.insert_one.await_args.args[0]
    assert payload["_id"] == session_id
    assert payload["username"] == "playerone"
    assert payload["ip"] == "127.0.0.1"
    assert payload["user_agent"] == "pytest"


@pytest.mark.asyncio
async def test_get_active_session_extends_expiry() -> None:
    now = datetime.now(UTC)
    session = {"_id": "sid", "expires_at": now + timedelta(minutes=5)}
    sessions = SimpleNamespace(find_one=AsyncMock(return_value=session), update_one=AsyncMock(), delete_one=AsyncMock())
    service = SessionService(sessions)

    active = await service.get_active_session("sid")

    assert active is not None
    assert sessions.update_one.await_count == 1
    assert sessions.delete_one.await_count == 0


@pytest.mark.asyncio
async def test_get_active_session_deletes_expired_session() -> None:
    now = datetime.now(UTC)
    session = {"_id": "sid", "expires_at": now - timedelta(seconds=1)}
    sessions = SimpleNamespace(find_one=AsyncMock(return_value=session), update_one=AsyncMock(), delete_one=AsyncMock())
    service = SessionService(sessions)

    active = await service.get_active_session("sid")

    assert active is None
    sessions.delete_one.assert_awaited_once_with({"_id": "sid"})
    assert sessions.update_one.await_count == 0


@pytest.mark.asyncio
async def test_get_active_session_returns_none_when_missing() -> None:
    sessions = SimpleNamespace(find_one=AsyncMock(return_value=None), update_one=AsyncMock(), delete_one=AsyncMock())
    service = SessionService(sessions)

    active = await service.get_active_session("missing")

    assert active is None
    assert sessions.update_one.await_count == 0
    assert sessions.delete_one.await_count == 0


@pytest.mark.asyncio
async def test_delete_session_deletes_by_id() -> None:
    sessions = SimpleNamespace(delete_one=AsyncMock())
    service = SessionService(sessions)

    await service.delete_session("sid")

    sessions.delete_one.assert_awaited_once_with({"_id": "sid"})


@pytest.mark.asyncio
async def test_get_active_session_supports_naive_datetime_from_mongo() -> None:
    now_naive = datetime.utcnow()
    session = {"_id": "sid", "expires_at": now_naive + timedelta(minutes=5)}
    sessions = SimpleNamespace(find_one=AsyncMock(return_value=session), update_one=AsyncMock(), delete_one=AsyncMock())
    service = SessionService(sessions)

    active = await service.get_active_session("sid")

    assert active is not None
    assert sessions.update_one.await_count == 1
