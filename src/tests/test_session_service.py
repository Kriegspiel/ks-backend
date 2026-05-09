from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.services.session_service import SessionService


class FrozenSessionService(SessionService):
    now = datetime(2026, 5, 9, tzinfo=UTC)

    @classmethod
    def utcnow(cls) -> datetime:
        return cls.now


@pytest.mark.asyncio
async def test_create_session_inserts_expected_document() -> None:
    sessions = SimpleNamespace(insert_one=AsyncMock())
    service = SessionService(sessions)
    user = SimpleNamespace(id="507f1f77bcf86cd799439011", username="playerone", role="user")

    session_id = await service.create_session(user=user, ip="127.0.0.1", user_agent="pytest")

    assert isinstance(session_id, str)
    assert len(session_id) == 64
    assert sessions.insert_one.await_count == 1
    payload = sessions.insert_one.await_args.args[0]
    assert payload["_id"] == session_id
    assert payload["username"] == "playerone"
    assert payload["ip"] == "127.0.0.1"
    assert payload["user_agent"] == "pytest"
    assert payload["max_age_seconds"] == SessionService.SESSION_MAX_AGE_SECONDS
    assert payload["cookie_max_age_seconds"] == SessionService.SESSION_MAX_AGE_SECONDS
    assert payload["last_touched_at"] == payload["created_at"]
    assert payload["expires_at"] - payload["created_at"] == timedelta(seconds=SessionService.SESSION_MAX_AGE_SECONDS)


@pytest.mark.asyncio
async def test_create_guest_session_uses_long_lived_expiry() -> None:
    sessions = SimpleNamespace(insert_one=AsyncMock())
    service = SessionService(sessions)
    user = SimpleNamespace(id="507f1f77bcf86cd799439011", username="guest_adolf_adams", role="guest")

    await service.create_session(user=user, ip="127.0.0.1", user_agent="pytest")

    payload = sessions.insert_one.await_args.args[0]
    assert payload["max_age_seconds"] == SessionService.GUEST_SESSION_MAX_AGE_SECONDS
    assert payload["cookie_max_age_seconds"] == SessionService.GUEST_COOKIE_MAX_AGE_SECONDS
    assert payload["expires_at"] - payload["created_at"] == timedelta(seconds=SessionService.GUEST_SESSION_MAX_AGE_SECONDS)
    assert payload["expires_at"] - payload["created_at"] > timedelta(days=365 * 4)
    assert SessionService.GUEST_COOKIE_MAX_AGE_SECONDS == 60 * 60 * 24 * 400


@pytest.mark.asyncio
async def test_get_active_session_extends_expiry() -> None:
    now = datetime.now(UTC)
    session = {
        "_id": "sid",
        "expires_at": now + timedelta(minutes=5),
        "max_age_seconds": SessionService.GUEST_SESSION_MAX_AGE_SECONDS,
    }
    sessions = SimpleNamespace(find_one=AsyncMock(return_value=session), update_one=AsyncMock(), delete_one=AsyncMock())
    service = SessionService(sessions)

    active = await service.get_active_session("sid")

    assert active is not None
    assert sessions.update_one.await_count == 1
    assert sessions.delete_one.await_count == 0
    update_payload = sessions.update_one.await_args.args[1]["$set"]
    assert update_payload["max_age_seconds"] == SessionService.GUEST_SESSION_MAX_AGE_SECONDS
    assert update_payload["last_touched_at"] >= now
    assert update_payload["expires_at"] - now > timedelta(days=(365 * 5) - 1)


@pytest.mark.asyncio
async def test_get_active_session_uses_warmed_cache_without_mongo_read_or_touch() -> None:
    FrozenSessionService.now = datetime(2026, 5, 9, tzinfo=UTC)
    sessions = SimpleNamespace(
        insert_one=AsyncMock(),
        find_one=AsyncMock(return_value=None),
        update_one=AsyncMock(),
        delete_one=AsyncMock(),
    )
    service = FrozenSessionService(sessions)
    user = SimpleNamespace(id="507f1f77bcf86cd799439011", username="playerone", role="user")

    session_id = await service.create_session(user=user, ip="127.0.0.1", user_agent="pytest")
    active = await service.get_active_session(session_id)

    assert active is not None
    assert active["username"] == "playerone"
    assert sessions.find_one.await_count == 0
    assert sessions.update_one.await_count == 0


@pytest.mark.asyncio
async def test_get_active_session_caches_db_reads_and_throttles_expiry_touch() -> None:
    FrozenSessionService.now = datetime(2026, 5, 9, tzinfo=UTC)
    session = {
        "_id": "sid",
        "expires_at": FrozenSessionService.now + timedelta(minutes=30),
        "last_touched_at": FrozenSessionService.now,
        "max_age_seconds": SessionService.SESSION_MAX_AGE_SECONDS,
    }
    sessions = SimpleNamespace(find_one=AsyncMock(return_value=session), update_one=AsyncMock(), delete_one=AsyncMock())
    service = FrozenSessionService(sessions)

    first = await service.get_active_session("sid")
    second = await service.get_active_session("sid")

    assert first is not None
    assert second is not None
    assert sessions.find_one.await_count == 1
    assert sessions.update_one.await_count == 0


@pytest.mark.asyncio
async def test_get_active_session_touches_cached_session_after_touch_interval() -> None:
    FrozenSessionService.now = datetime(2026, 5, 9, tzinfo=UTC)
    session = {
        "_id": "sid",
        "expires_at": FrozenSessionService.now + timedelta(hours=1),
        "last_touched_at": FrozenSessionService.now,
        "max_age_seconds": SessionService.SESSION_MAX_AGE_SECONDS,
    }
    sessions = SimpleNamespace(find_one=AsyncMock(return_value=session), update_one=AsyncMock(), delete_one=AsyncMock())
    service = FrozenSessionService(sessions, cache_ttl_seconds=600)

    await service.get_active_session("sid")
    FrozenSessionService.now = FrozenSessionService.now + timedelta(seconds=SessionService.SESSION_TOUCH_INTERVAL_SECONDS + 1)
    touched = await service.get_active_session("sid")

    assert touched is not None
    assert sessions.find_one.await_count == 1
    assert sessions.update_one.await_count == 1
    update_payload = sessions.update_one.await_args.args[1]["$set"]
    assert update_payload["last_touched_at"] == FrozenSessionService.now


@pytest.mark.asyncio
async def test_get_active_session_caches_missing_sessions_briefly() -> None:
    FrozenSessionService.now = datetime(2026, 5, 9, tzinfo=UTC)
    sessions = SimpleNamespace(find_one=AsyncMock(return_value=None), update_one=AsyncMock(), delete_one=AsyncMock())
    service = FrozenSessionService(sessions)

    first = await service.get_active_session("missing")
    second = await service.get_active_session("missing")

    assert first is None
    assert second is None
    assert sessions.find_one.await_count == 1
    assert sessions.update_one.await_count == 0
    assert sessions.delete_one.await_count == 0


@pytest.mark.asyncio
async def test_delete_session_evicts_cached_session() -> None:
    FrozenSessionService.now = datetime(2026, 5, 9, tzinfo=UTC)
    sessions = SimpleNamespace(
        insert_one=AsyncMock(),
        find_one=AsyncMock(return_value=None),
        update_one=AsyncMock(),
        delete_one=AsyncMock(),
    )
    service = FrozenSessionService(sessions)
    user = SimpleNamespace(id="507f1f77bcf86cd799439011", username="playerone", role="user")
    session_id = await service.create_session(user=user, ip=None, user_agent=None)

    await service.delete_session(session_id)
    active = await service.get_active_session(session_id)

    assert active is None
    sessions.delete_one.assert_awaited_once_with({"_id": session_id})
    assert sessions.find_one.await_count == 1


@pytest.mark.asyncio
async def test_update_session_for_user_refreshes_cached_identity() -> None:
    FrozenSessionService.now = datetime(2026, 5, 9, tzinfo=UTC)
    sessions = SimpleNamespace(find_one=AsyncMock(return_value=None), update_one=AsyncMock(), delete_one=AsyncMock())
    service = FrozenSessionService(sessions)
    user = SimpleNamespace(id="507f1f77bcf86cd799439011", username="converted", role="user")

    await service.update_session_for_user("sid", user)
    active = await service.get_active_session("sid")

    assert active is not None
    assert active["username"] == "converted"
    assert sessions.find_one.await_count == 0


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
async def test_update_session_for_user_rewrites_username_and_regular_lifetime() -> None:
    sessions = SimpleNamespace(update_one=AsyncMock())
    service = SessionService(sessions)
    user = SimpleNamespace(id="507f1f77bcf86cd799439011", username="adolf_adams", role="user")

    await service.update_session_for_user("sid", user)

    query, update = sessions.update_one.await_args.args
    assert query == {"_id": "sid"}
    assert update["$set"]["username"] == "adolf_adams"
    assert update["$set"]["max_age_seconds"] == SessionService.SESSION_MAX_AGE_SECONDS
    assert update["$set"]["cookie_max_age_seconds"] == SessionService.SESSION_MAX_AGE_SECONDS


@pytest.mark.asyncio
async def test_update_session_for_guest_uses_long_server_lifetime_and_browser_safe_cookie_lifetime() -> None:
    sessions = SimpleNamespace(update_one=AsyncMock())
    service = SessionService(sessions)
    user = SimpleNamespace(id="507f1f77bcf86cd799439011", username="guest_adolf_adams", role="guest")

    await service.update_session_for_user("sid", user)

    update = sessions.update_one.await_args.args[1]
    assert update["$set"]["username"] == "guest_adolf_adams"
    assert update["$set"]["max_age_seconds"] == SessionService.GUEST_SESSION_MAX_AGE_SECONDS
    assert update["$set"]["cookie_max_age_seconds"] == SessionService.GUEST_COOKIE_MAX_AGE_SECONDS


@pytest.mark.asyncio
async def test_get_active_session_supports_naive_datetime_from_mongo() -> None:
    now_naive = datetime.utcnow()
    session = {"_id": "sid", "expires_at": now_naive + timedelta(minutes=5)}
    sessions = SimpleNamespace(find_one=AsyncMock(return_value=session), update_one=AsyncMock(), delete_one=AsyncMock())
    service = SessionService(sessions)

    active = await service.get_active_session("sid")

    assert active is not None
    assert sessions.update_one.await_count == 1
