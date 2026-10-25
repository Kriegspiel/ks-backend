import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.services.rating_history_cache import RatingHistoryCache
from app.services.game_service import GameService
from app.services.user_service import UserService
from tests.test_user_service import FakeDB, FakeUsersCollection


@pytest.mark.asyncio
async def test_simultaneous_requests_share_one_scan_and_return_independent_payloads():
    cache = RatingHistoryCache()
    started, finish = asyncio.Event(), asyncio.Event()

    async def calculate():
        started.set()
        await finish.wait()
        return {"series": {"game": [1]}}

    loader = AsyncMock(side_effect=calculate)
    key = (1, "player", "overall", 100)
    first = asyncio.create_task(cache.get(key, loader))
    await started.wait()
    second = asyncio.create_task(cache.get(key, loader))
    await asyncio.sleep(0)
    finish.set()
    a, b = await asyncio.gather(first, second)
    a["series"]["game"].append(2)
    assert b == {"series": {"game": [1]}}
    assert await cache.get(key, loader) == b
    assert loader.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("clear_all", [False, True])
async def test_completion_during_fill_cannot_repopulate_invalidated_cache(clear_all):
    cache = RatingHistoryCache()
    started, finish = asyncio.Event(), asyncio.Event()
    key = (1, "player", "overall", 100)

    async def old_history():
        started.set()
        await finish.wait()
        return {"games": 1}

    old = asyncio.create_task(cache.get(key, old_history))
    await started.wait()
    if clear_all:
        cache.clear()
    else:
        cache.invalidate_user("player")
    fresh = AsyncMock(return_value={"games": 2})
    assert await cache.get(key, fresh) == {"games": 2}
    finish.set()
    assert await old == {"games": 1}
    assert await cache.get(key, fresh) == {"games": 2}
    assert fresh.await_count == 1
    assert not cache._inflight


@pytest.mark.asyncio
async def test_cancelled_client_does_not_cancel_shared_scan():
    cache = RatingHistoryCache()
    started, finish = asyncio.Event(), asyncio.Event()

    async def calculate():
        started.set()
        await finish.wait()
        return {"games": 1}

    loader = AsyncMock(side_effect=calculate)
    key = (1, "player", "overall", 100)
    first = asyncio.create_task(cache.get(key, loader))
    await started.wait()
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    second = asyncio.create_task(cache.get(key, loader))
    finish.set()
    assert await second == {"games": 1}
    assert loader.await_count == 1


@pytest.mark.asyncio
async def test_errors_are_not_cached_and_clear_invalidates_outstanding_work():
    cache = RatingHistoryCache()
    key = (1, "player", "overall", 100)
    loader = AsyncMock(side_effect=[RuntimeError("database unavailable"), {"ok": True}])
    with pytest.raises(RuntimeError):
        await cache.get(key, loader)
    assert not cache._inflight
    assert await cache.get(key, loader) == {"ok": True}
    cache.clear()
    assert cache._bytes == 0
    assert not cache._entries


@pytest.mark.asyncio
async def test_cache_has_entry_byte_and_expiry_bounds():
    a, b = (1, "a", "overall", 100), (1, "b", "overall", 100)
    loader = AsyncMock(return_value={"ok": True})
    cache = RatingHistoryCache(max_entries=1)
    await cache.get(a, loader)
    await cache.get(b, loader)
    await cache.get(a, loader)
    assert loader.await_count == 3
    assert len(cache._entries) == 1
    cache = RatingHistoryCache(max_bytes=20)
    await cache.get(a, loader)
    await cache.get(b, loader)
    assert cache._bytes <= 20
    assert len(cache._entries) == 1
    huge = AsyncMock(return_value={"text": "x" * 100})
    await cache.get(a, huge)
    await cache.get(a, huge)
    assert huge.await_count == 2
    cache = RatingHistoryCache(ttl=-1)
    await cache.get(a, loader)
    await cache.get(a, loader)
    assert loader.await_count == 7


@pytest.mark.asyncio
@pytest.mark.parametrize("track", ["overall", "vs_humans", "vs_bots"])
@pytest.mark.parametrize("limit", [10, 100])
async def test_cache_preserves_complete_history_and_stops_repeat_archive_scans(monkeypatch, track, limit):
    monkeypatch.setattr(UserService, "_rating_history_cache", RatingHistoryCache())
    users, archives = FakeUsersCollection(), FakeUsersCollection()
    db = FakeDB(users, archives)
    for i in range(205):
        matchup = "vs_bots" if i % 2 else "vs_humans"
        archives.docs.append({
            "white": {"user_id": "player", "role": "user"},
            "black": {"user_id": "opponent", "role": "bot" if i % 2 else "user"},
            "created_at": datetime(2026, 1, 1, tzinfo=UTC) + timedelta(days=i),
            "rating_snapshot": {
                "overall": {"white_after": 1200 + i, "white_delta": 1},
                "specific": {"white_after": 1200 + i, "white_delta": 1},
                "white_track": matchup, "black_track": "vs_humans",
            },
        })
    service = UserService(users)
    expected = await service._build_rating_history(db, "player", track=track, limit=limit)
    archives.find_calls.clear()
    first = await service.get_rating_history(db, "player", track=track, limit=limit)
    second = await UserService(users).get_rating_history(db, "player", track=track, limit=limit)
    assert first == second == expected
    assert len(archives.find_calls) == 1
    expected_games = 205 if track == "overall" else (102 if track == "vs_bots" else 103)
    assert first["series"]["game"][-1]["game_number"] == expected_games
    assert len(first["series"]["game"]) <= limit


@pytest.mark.asyncio
async def test_completion_hook_invalidates_both_players_all_tracks_and_limits(monkeypatch):
    cache = RatingHistoryCache()
    monkeypatch.setattr(UserService, "_rating_history_cache", cache)
    loader = AsyncMock(return_value={"games": 1})
    keys = [(1, user, track, limit) for user in ("white", "black", "other")
            for track in ("overall", "vs_bots") for limit in (10, 100)]
    for key in keys:
        await cache.get(key, loader)
    UserService.evict_profile_metrics_cache_for_user_ids(["white", "black"])
    assert {k[1] for k in cache._entries} == {"other"}
    UserService.clear_profile_metrics_cache()
    assert cache._bytes == 0


@pytest.mark.asyncio
async def test_parameter_and_database_isolation(monkeypatch):
    monkeypatch.setattr(UserService, "_rating_history_cache", RatingHistoryCache())
    service = UserService(None)
    service._build_rating_history = AsyncMock(return_value={"ok": True})
    a, b = SimpleNamespace(), SimpleNamespace()
    await service.get_rating_history(a, "player", track="overall", limit=100)
    await service.get_rating_history(b, "player", track="overall", limit=100)
    await service.get_rating_history(a, "player", track="vs_bots", limit=100)
    await service.get_rating_history(a, "player", track="overall", limit=10)
    assert service._build_rating_history.await_count == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("abort", [False, True])
async def test_game_completion_invalidates_only_after_successful_transaction(monkeypatch, abort):
    cache = RatingHistoryCache()
    monkeypatch.setattr(UserService, "_rating_history_cache", cache)
    loader = AsyncMock(return_value={"games": 0})
    for user in ("white", "black"):
        await cache.get((1, user, "overall", 100), loader)
    game = {"state": "completed", "white": {"user_id": "white"}, "black": {"user_id": "black"}}

    class Session:
        async def with_transaction(self, callback, **kwargs):
            result = await callback(self)
            assert {k[1] for k in cache._entries} == {"white", "black"}
            if abort:
                raise RuntimeError("transaction aborted")
            return result

    @asynccontextmanager
    async def session_context():
        yield Session()

    class Client:
        async def start_session(self):
            return session_context()

    service = GameService(None, mongo_client=Client())
    service._finalize_completed_game_in_session = AsyncMock(return_value=game)
    if abort:
        with pytest.raises(RuntimeError, match="aborted"):
            await service._finalize_completed_game(game)
        assert len(cache._entries) == 2
    else:
        assert await service._finalize_completed_game(game) == game
        assert not cache._entries


@pytest.mark.asyncio
async def test_invalidation_preserves_other_players_inflight_scan():
    cache = RatingHistoryCache()
    started, finish = asyncio.Event(), asyncio.Event()

    async def calculate():
        started.set()
        await finish.wait()
        return {"ok": True}

    task = asyncio.create_task(cache.get((1, "other", "overall", 100), calculate))
    await started.wait()
    cache.invalidate_user("player")
    finish.set()
    assert await task == {"ok": True}
    assert len(cache._entries) == 1
