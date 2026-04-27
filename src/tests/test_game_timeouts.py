from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from bson import ObjectId

from app.services.clock_service import ClockService
from app.services.engine_adapter import create_new_game, serialize_game_state
from app.services.game_service import GameForbiddenError, GameService, GameValidationError


class FakeGamesCollection:
    def __init__(self):
        self.docs: list[dict] = []

    async def find_one(self, query: dict, projection: dict | None = None):
        for doc in self.docs:
            if all(self._resolve(doc, k) == v for k, v in query.items()):
                return doc
        return None

    async def find_one_and_update(self, query: dict, update: dict, return_document=None):  # noqa: ANN001
        for doc in self.docs:
            if all(self._resolve(doc, k) == v for k, v in query.items()):
                for key, value in update.get("$set", {}).items():
                    doc[key] = value
                for key, value in update.get("$inc", {}).items():
                    doc[key] = doc.get(key, 0) + value
                for key, value in update.get("$push", {}).items():
                    doc.setdefault(key, []).append(value)
                return doc
        return None

    def find(self, query: dict):
        docs = [doc for doc in self.docs if all(self._resolve(doc, k) == v for k, v in query.items())]

        class Cursor:
            def __init__(self, rows):
                self._rows = list(rows)
                self._index = 0

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self._index >= len(self._rows):
                    raise StopAsyncIteration
                row = self._rows[self._index]
                self._index += 1
                return row

        return Cursor(docs)

    @staticmethod
    def _resolve(doc: dict, key: str):
        cur = doc
        for part in key.split("."):
            if not isinstance(cur, dict):
                return None
            cur = cur.get(part)
        return cur


def _active_doc(now: datetime, gid: ObjectId) -> dict:
    return {
        "_id": gid,
        "game_code": "A7K2M9",
        "rule_variant": "berkeley_any",
        "creator_color": "white",
        "white": {"user_id": "u1", "username": "w", "connected": True},
        "black": {"user_id": "u2", "username": "b", "connected": True},
        "state": "active",
        "turn": "white",
        "move_number": 1,
        "moves": [],
        "engine_state": serialize_game_state(create_new_game(any_rule=True)),
        "time_control": ClockService.default_time_control(now=now, active_color="white"),
        "created_at": now,
        "updated_at": now,
    }


@pytest.mark.asyncio
async def test_polling_adjudicates_timeout_and_completes_game() -> None:
    now = datetime.now(UTC)
    game_id = ObjectId()
    games = FakeGamesCollection()
    doc = _active_doc(now, game_id)
    doc["time_control"]["white_remaining"] = 0.5
    doc["time_control"]["last_updated_at"] = now - timedelta(seconds=2)
    games.docs.append(doc)

    service = GameService(games)
    state = await service.get_game_state(game_id=str(game_id), user_id="u2")

    assert state.state == "completed"
    assert state.result == {"winner": "black", "reason": "timeout"}
    assert state.clock.active_color is None


@pytest.mark.asyncio
async def test_move_rejected_if_timeout_already_elapsed() -> None:
    now = datetime.now(UTC)
    game_id = ObjectId()
    games = FakeGamesCollection()
    doc = _active_doc(now, game_id)
    doc["time_control"]["white_remaining"] = 0.1
    doc["time_control"]["last_updated_at"] = now - timedelta(seconds=1)
    games.docs.append(doc)

    service = GameService(games)

    with pytest.raises(GameValidationError) as exc:
        await service.execute_move(game_id=str(game_id), user_id="u1", uci="e2e4")

    assert exc.value.code == "GAME_NOT_ACTIVE"
    entry = await service._get_cached_entry(game_id)
    assert entry is not None
    assert entry.game["state"] == "completed"
    assert entry.game["result"] == {"winner": "black", "reason": "timeout"}

    await service._flush_entry(entry, reason="test")
    assert games.docs[0]["state"] == "completed"
    assert games.docs[0]["result"] == {"winner": "black", "reason": "timeout"}


@pytest.mark.asyncio
async def test_legal_move_updates_clock_payload() -> None:
    now = datetime.now(UTC)
    game_id = ObjectId()
    games = FakeGamesCollection()
    games.docs.append(_active_doc(now, game_id))

    class Frozen(GameService):
        @staticmethod
        def utcnow() -> datetime:
            return now + timedelta(seconds=3)

    service = Frozen(games)
    move = await service.execute_move(game_id=str(game_id), user_id="u1", uci="e2e4")

    assert move["clock"]["active_color"] == "black"
    assert move["clock"]["white_remaining"] == pytest.approx(1507.0)
    assert move["clock"]["black_remaining"] == pytest.approx(1500.0)


@pytest.mark.asyncio
async def test_first_legal_move_starts_clock_without_charging_opening_wait() -> None:
    now = datetime.now(UTC)
    game_id = ObjectId()
    games = FakeGamesCollection()
    doc = _active_doc(now, game_id)
    doc["time_control"] = ClockService.default_time_control(now=now)
    games.docs.append(doc)

    class Frozen(GameService):
        @staticmethod
        def utcnow() -> datetime:
            return now + timedelta(seconds=3)

    service = Frozen(games)
    move = await service.execute_move(game_id=str(game_id), user_id="u1", uci="e2e4")

    assert move["clock"]["active_color"] == "black"
    assert move["clock"]["white_remaining"] == pytest.approx(1510.0)
    assert move["clock"]["black_remaining"] == pytest.approx(1500.0)


@pytest.mark.asyncio
async def test_move_rejects_non_participant() -> None:
    now = datetime.now(UTC)
    gid = ObjectId()
    games = FakeGamesCollection()
    games.docs.append(_active_doc(now, gid))

    service = GameService(games)
    with pytest.raises(GameForbiddenError) as exc:
        await service.execute_move(game_id=str(gid), user_id="u3", uci="e2e4")
    assert exc.value.code == "FORBIDDEN"


@pytest.mark.asyncio
async def test_move_rejects_out_of_turn() -> None:
    now = datetime.now(UTC)
    gid = ObjectId()
    games = FakeGamesCollection()
    games.docs.append(_active_doc(now, gid))

    service = GameService(games)
    with pytest.raises(GameValidationError) as exc:
        await service.execute_move(game_id=str(gid), user_id="u2", uci="e7e5")
    assert exc.value.code == "NOT_YOUR_TURN"


@pytest.mark.asyncio
async def test_get_game_state_rejects_non_participant() -> None:
    now = datetime.now(UTC)
    gid = ObjectId()
    games = FakeGamesCollection()
    games.docs.append(_active_doc(now, gid))

    service = GameService(games)
    with pytest.raises(GameForbiddenError):
        await service.get_game_state(game_id=str(gid), user_id="u9")


@pytest.mark.asyncio
async def test_move_rejects_when_game_not_active() -> None:
    now = datetime.now(UTC)
    gid = ObjectId()
    games = FakeGamesCollection()
    doc = _active_doc(now, gid)
    doc["state"] = "completed"
    games.docs.append(doc)

    service = GameService(games)
    with pytest.raises(GameValidationError) as exc:
        await service.execute_move(game_id=str(gid), user_id="u1", uci="e2e4")
    assert exc.value.code == "GAME_NOT_ACTIVE"


@pytest.mark.asyncio
async def test_timeout_sweeper_completes_persisted_active_games_without_requests() -> None:
    now = datetime.now(UTC)
    gid = ObjectId()
    games = FakeGamesCollection()
    doc = _active_doc(now, gid)
    doc["time_control"]["white_remaining"] = 0.1
    doc["time_control"]["last_updated_at"] = now - timedelta(seconds=5)
    games.docs.append(doc)

    service = GameService(games)
    await service._sweep_timeouts(now=now)

    assert games.docs[0]["state"] == "completed"
    assert games.docs[0]["result"] == {"winner": "black", "reason": "timeout"}


@pytest.mark.asyncio
async def test_timeout_sweeper_completes_cached_active_games_without_requests() -> None:
    now = datetime.now(UTC)
    gid = ObjectId()
    games = FakeGamesCollection()
    doc = _active_doc(now, gid)
    doc["time_control"]["black_remaining"] = 0.1
    doc["time_control"]["active_color"] = "black"
    doc["time_control"]["last_updated_at"] = now - timedelta(seconds=5)
    games.docs.append(doc)

    service = GameService(games)
    entry = await service._prime_cache(doc, persisted=True)

    await service._sweep_timeouts(now=now)
    await service._flush_entry(entry, reason="test")

    assert entry.game["state"] == "completed"
    assert games.docs[0]["state"] == "completed"
    assert games.docs[0]["result"] == {"winner": "white", "reason": "timeout"}


@pytest.mark.asyncio
async def test_service_start_runs_timeout_sweep_immediately() -> None:
    service = GameService(FakeGamesCollection())
    calls = 0

    async def fake_sweep() -> None:
        nonlocal calls
        calls += 1

    service._maybe_sweep_timeouts = fake_sweep  # type: ignore[method-assign]

    await service.start()
    try:
        assert calls == 1
    finally:
        await service.shutdown()
