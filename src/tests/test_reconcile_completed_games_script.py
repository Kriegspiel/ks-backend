from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
import sys

import pytest
from bson import ObjectId

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.reconcile_completed_games_from_live import reconcile_completed_games  # noqa: E402


class FakeDeleteResult:
    def __init__(self, deleted_count: int):
        self.deleted_count = deleted_count


class FakeCursor:
    def __init__(self, docs: list[dict]):
        self._docs = docs

    def limit(self, count: int):
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


class FakeCollection:
    def __init__(self, docs: list[dict]):
        self.docs = docs
        self.delete_queries: list[dict] = []
        self.replace_calls: list[tuple[dict, dict, bool]] = []

    def find(self, query: dict):
        return FakeCursor([doc for doc in self.docs if self._matches(doc, query)])

    async def find_one(self, query: dict):
        for doc in self.docs:
            if self._matches(doc, query):
                return deepcopy(doc)
        return None

    async def replace_one(self, query: dict, replacement: dict, *, upsert: bool):
        self.replace_calls.append((query, deepcopy(replacement), upsert))
        for index, doc in enumerate(self.docs):
            if self._matches(doc, query):
                self.docs[index] = deepcopy(replacement)
                return
        if upsert:
            self.docs.append(deepcopy(replacement))

    async def delete_one(self, query: dict):
        self.delete_queries.append(query)
        for index, doc in enumerate(self.docs):
            if self._matches(doc, query):
                self.docs.pop(index)
                return FakeDeleteResult(1)
        return FakeDeleteResult(0)

    @staticmethod
    def _matches(doc: dict, query: dict) -> bool:
        return all(doc.get(key) == expected for key, expected in query.items())


class FakeDB:
    def __init__(self, *, games: list[dict], game_archives: list[dict]):
        self.games = FakeCollection(games)
        self.game_archives = FakeCollection(game_archives)


def _completed_game(*, game_id: ObjectId, game_code: str, updated_at: datetime) -> dict:
    return {
        "_id": game_id,
        "game_code": game_code,
        "state": "completed",
        "white": {"user_id": "u1", "role": "user"},
        "black": {"user_id": "b1", "role": "bot"},
        "moves": [{"next_turn_pawn_try_squares": (8, 9)}],
        "result": {"winner": "white", "reason": "checkmate"},
        "updated_at": updated_at,
    }


@pytest.mark.asyncio
async def test_reconcile_completed_games_dry_run_reports_actions_without_writes() -> None:
    now = datetime(2026, 5, 18, 12, 0, 0, 456789, tzinfo=UTC)
    stored_now = datetime(2026, 5, 18, 12, 0, 0, 456000)
    identical_id = ObjectId()
    missing_id = ObjectId()
    mismatch_id = ObjectId()
    active_id = ObjectId()
    identical = _completed_game(game_id=identical_id, game_code="SAME01", updated_at=now)
    missing = _completed_game(game_id=missing_id, game_code="MISS01", updated_at=now)
    mismatch = _completed_game(game_id=mismatch_id, game_code="DIFF01", updated_at=now)
    active = {"_id": active_id, "game_code": "LIVE01", "state": "active", "updated_at": now}
    db = FakeDB(
        games=[identical, missing, mismatch, active],
        game_archives=[
            {**identical, "moves": [{"next_turn_pawn_try_squares": [8, 9]}], "updated_at": stored_now},
            {**mismatch, "updated_at": datetime(2026, 5, 19, tzinfo=UTC)},
        ],
    )

    summary = await reconcile_completed_games(db, apply=False, repair_mismatched=True)

    assert summary["scanned"] == 3
    assert summary["identical"] == 1
    assert summary["missing_archive"] == 1
    assert summary["mismatched_archive"] == 1
    assert summary["would_delete_identical"] == 1
    assert summary["would_archive_missing"] == 1
    assert summary["would_repair_mismatched"] == 1
    assert summary["deleted"] == 0
    assert db.games.delete_queries == []
    assert db.game_archives.replace_calls == []


@pytest.mark.asyncio
async def test_reconcile_completed_games_backfills_missing_archive_before_deleting_live_doc() -> None:
    now = datetime(2026, 5, 18, tzinfo=UTC)
    missing_id = ObjectId()
    mismatch_id = ObjectId()
    missing = _completed_game(game_id=missing_id, game_code="MISS01", updated_at=now)
    mismatch = _completed_game(game_id=mismatch_id, game_code="DIFF01", updated_at=now)
    db = FakeDB(
        games=[missing, mismatch],
        game_archives=[{**mismatch, "updated_at": datetime(2026, 5, 19, tzinfo=UTC)}],
    )

    summary = await reconcile_completed_games(db, apply=True)

    assert summary["archived_missing"] == 1
    assert summary["mismatched_archive"] == 1
    assert summary["repaired_mismatched"] == 0
    assert summary["deleted"] == 1
    assert db.games.delete_queries == [{"_id": missing_id, "state": "completed"}]
    assert [doc["_id"] for doc in db.games.docs] == [mismatch_id]
    assert any(doc["_id"] == missing_id and doc["game_code"] == "MISS01" for doc in db.game_archives.docs)


@pytest.mark.asyncio
async def test_reconcile_completed_games_repairs_mismatched_archive_when_requested() -> None:
    now = datetime(2026, 5, 18, tzinfo=UTC)
    mismatch_id = ObjectId()
    mismatch = _completed_game(game_id=mismatch_id, game_code="DIFF01", updated_at=now)
    db = FakeDB(
        games=[mismatch],
        game_archives=[{**mismatch, "updated_at": datetime(2026, 5, 19, tzinfo=UTC)}],
    )

    summary = await reconcile_completed_games(db, apply=True, repair_mismatched=True)

    assert summary["mismatched_archive"] == 1
    assert summary["repaired_mismatched"] == 1
    assert summary["deleted"] == 1
    assert db.games.docs == []
    assert db.game_archives.docs == [mismatch]
