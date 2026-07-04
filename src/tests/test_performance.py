from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
import time

from bson import ObjectId
import pytest

from app.services.engine_adapter import create_new_game, serialize_game_state
from app.services.game_service import GAME_METADATA_PROJECTION, GameService
from app.services.session_service import SessionService

GAME_CODE_ALPHABET = "23456789ABCDEFGHJKMNPQRSTUVWXYZ"


class FrozenSessionService(SessionService):
    now = datetime(2026, 5, 9, tzinfo=UTC)

    @classmethod
    def utcnow(cls) -> datetime:
        return cls.now


class InstrumentedCursor:
    def __init__(self, docs: list[dict[str, Any]]):
        self._docs = docs
        self.sort_calls: list[tuple[str, int]] = []
        self.limit_calls: list[int] = []

    def sort(self, field: str, direction: int):
        self.sort_calls.append((field, direction))
        self._docs.sort(key=lambda doc: doc[field], reverse=direction < 0)
        return self

    def limit(self, count: int):
        self.limit_calls.append(count)
        self._docs = self._docs[:count]
        return self

    def __aiter__(self):
        self._idx = 0
        return self

    async def __anext__(self):
        if self._idx >= len(self._docs):
            raise StopAsyncIteration
        value = self._docs[self._idx]
        self._idx += 1
        return value


class InstrumentedGamesCollection:
    def __init__(self, docs: list[dict[str, Any]] | None = None):
        self.docs = docs or []
        self.find_calls: list[tuple[dict[str, Any], dict[str, int] | None]] = []
        self.find_one_calls: list[tuple[dict[str, Any], dict[str, int] | None]] = []
        self.cursors: list[InstrumentedCursor] = []

    async def find_one(self, query: dict[str, Any], projection: dict[str, int] | None = None):
        self.find_one_calls.append((query, projection))
        for doc in self.docs:
            if self._matches(doc, query):
                return self._project(doc, projection) if projection else doc
        return None

    def find(self, query: dict[str, Any], projection: dict[str, int] | None = None):
        self.find_calls.append((query, projection))
        docs = [self._project(doc, projection) if projection else doc for doc in self.docs if self._matches(doc, query)]
        cursor = InstrumentedCursor(docs)
        self.cursors.append(cursor)
        return cursor

    def _matches(self, doc: dict[str, Any], query: dict[str, Any]) -> bool:
        for key, expected in query.items():
            value = self._resolve(doc, key)
            if isinstance(expected, dict):
                if "$ne" in expected and value == expected["$ne"]:
                    return False
                continue
            if value != expected:
                return False
        return True

    @staticmethod
    def _resolve(doc: dict[str, Any], key: str):
        current = doc
        for part in key.split("."):
            if not isinstance(current, dict):
                return None
            current = current.get(part)
        return current

    @staticmethod
    def _set_nested(doc: dict[str, Any], key: str, value: Any) -> None:
        parts = key.split(".")
        current = doc
        for part in parts[:-1]:
            current = current.setdefault(part, {})
        current[parts[-1]] = value

    @classmethod
    def _project(cls, doc: dict[str, Any], projection: dict[str, int] | None):
        if projection is None:
            return dict(doc)
        result: dict[str, Any] = {}
        for key, include in projection.items():
            if not include:
                continue
            value = cls._resolve(doc, key)
            if value is not None:
                cls._set_nested(result, key, value)
        return result


def _active_game_doc(*, now: datetime, game_id: ObjectId | None = None) -> dict[str, Any]:
    engine = create_new_game(rule_variant="berkeley_any")
    return {
        "_id": game_id or ObjectId(),
        "game_code": "PERF01",
        "rule_variant": "berkeley_any",
        "white": {"user_id": "u1", "username": "white", "connected": True, "role": "user"},
        "black": {"user_id": "u2", "username": "black", "connected": True, "role": "user"},
        "state": "active",
        "turn": "white",
        "move_number": 1,
        "moves": [],
        "engine_state": serialize_game_state(engine),
        "time_control": {
            "base": 900.0,
            "increment": 10.0,
            "white_remaining": 900.0,
            "black_remaining": 900.0,
            "active_color": None,
            "last_updated_at": now,
        },
        "created_at": now,
        "updated_at": now,
    }


def _game_code(prefix: str, index: int) -> str:
    value = index
    suffix: list[str] = []
    for _ in range(5):
        suffix.append(GAME_CODE_ALPHABET[value % len(GAME_CODE_ALPHABET)])
        value //= len(GAME_CODE_ALPHABET)
    return prefix + "".join(suffix)


def _metadata_doc(*, code: str, user_field: str, created_at: datetime, large_moves: list[dict[str, Any]]) -> dict[str, Any]:
    white_user = "u1" if user_field == "white" else "u2"
    black_user = "u1" if user_field == "black" else "u3"
    return {
        "_id": ObjectId(),
        "game_code": code,
        "rule_variant": "berkeley_any",
        "white": {"user_id": white_user, "username": "white", "connected": True, "role": "user"},
        "black": {"user_id": black_user, "username": "black", "connected": True, "role": "user"},
        "state": "completed" if code.startswith("A") else "active",
        "turn": None if code.startswith("A") else "white",
        "move_number": len(large_moves),
        "created_at": created_at,
        "updated_at": created_at,
        "result": {"winner": "white", "reason": "resignation"} if code.startswith("A") else None,
        "moves": large_moves,
        "engine_state": {"large": "ignored"},
    }


@pytest.mark.asyncio
@pytest.mark.performance
async def test_active_game_state_cached_reads_stay_under_half_second() -> None:
    now = datetime(2026, 5, 9, tzinfo=UTC)
    game_id = ObjectId()
    games = InstrumentedGamesCollection([_active_game_doc(now=now, game_id=game_id)])
    service = GameService(games)
    service.utcnow = lambda: now

    first = await service.get_game_state(game_id=str(game_id), user_id="u1")
    assert first.your_color == "white"

    start = time.perf_counter()
    for _ in range(50):
        response = await service.get_game_state(game_id=str(game_id), user_id="u1")
        assert response.your_color == "white"
    elapsed = time.perf_counter() - start

    assert elapsed < 0.5
    assert games.find_one_calls == [({"_id": game_id}, None)]


@pytest.mark.asyncio
@pytest.mark.performance
async def test_my_games_uses_projection_and_bounded_queries_for_large_game_documents() -> None:
    now = datetime(2026, 5, 9, tzinfo=UTC)
    large_moves = [{"ply": index, "payload": "x" * 64} for index in range(2_000)]
    live_docs = [
        _metadata_doc(
            code=_game_code("P", index),
            user_field="white" if index % 2 == 0 else "black",
            created_at=now,
            large_moves=large_moves,
        )
        for index in range(40)
    ]
    archived_docs = [
        _metadata_doc(
            code=_game_code("A", index),
            user_field="black" if index % 2 == 0 else "white",
            created_at=now,
            large_moves=large_moves,
        )
        for index in range(40)
    ]
    games = InstrumentedGamesCollection(live_docs)
    archives = InstrumentedGamesCollection(archived_docs)
    service = GameService(games, archives_collection=archives)

    mine = await service.get_my_games(user_id="u1", limit=20)

    assert len(mine) == 20
    assert [projection for _, projection in games.find_calls + archives.find_calls] == [GAME_METADATA_PROJECTION] * 4
    assert [query for query, _ in games.find_calls] == [{"white.user_id": "u1"}, {"black.user_id": "u1"}]
    assert [query for query, _ in archives.find_calls] == [{"white.user_id": "u1"}, {"black.user_id": "u1"}]
    assert [cursor.limit_calls for cursor in games.cursors + archives.cursors] == [[20], [20], [20], [20]]
    assert [cursor.sort_calls for cursor in games.cursors + archives.cursors] == [
        [("created_at", -1)],
        [("created_at", -1)],
        [("created_at", -1)],
        [("created_at", -1)],
    ]
    projected_docs = [doc for cursor in games.cursors + archives.cursors for doc in cursor._docs]
    assert all("moves" not in doc and "engine_state" not in doc for doc in projected_docs)


@pytest.mark.asyncio
@pytest.mark.performance
async def test_repeated_session_cache_reads_avoid_mongo_and_stay_under_half_second() -> None:
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

    start = time.perf_counter()
    for _ in range(500):
        active = await service.get_active_session(session_id)
        assert active is not None
        assert active["username"] == "playerone"
    elapsed = time.perf_counter() - start

    assert elapsed < 0.5
    assert sessions.find_one.await_count == 0
    assert sessions.update_one.await_count == 0
    assert sessions.delete_one.await_count == 0
