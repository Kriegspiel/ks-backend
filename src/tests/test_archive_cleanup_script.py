from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import sys

import pytest
from bson import ObjectId

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.remove_archived_completed_games_from_live import cleanup_completed_games  # noqa: E402


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

    def find(self, query: dict):
        return FakeCursor([doc for doc in self.docs if self._matches(doc, query)])

    async def find_one(self, query: dict):
        for doc in self.docs:
            if self._matches(doc, query):
                return doc
        return None

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


@pytest.mark.asyncio
async def test_cleanup_completed_games_dry_run_reports_only_identical_archive_matches() -> None:
    now = datetime(2026, 4, 30, 12, 0, 0, 456789, tzinfo=UTC)
    stored_now = datetime(2026, 4, 30, 12, 0, 0, 456000)
    identical_id = ObjectId()
    missing_id = ObjectId()
    mismatch_id = ObjectId()
    active_id = ObjectId()
    identical = {"_id": identical_id, "game_code": "SAME01", "state": "completed", "updated_at": now}
    missing = {"_id": missing_id, "game_code": "MISS01", "state": "completed", "updated_at": now}
    mismatch = {"_id": mismatch_id, "game_code": "DIFF01", "state": "completed", "updated_at": now}
    active = {"_id": active_id, "game_code": "LIVE01", "state": "active", "updated_at": now}
    db = FakeDB(
        games=[identical, missing, mismatch, active],
        game_archives=[
            {**identical, "updated_at": stored_now},
            {**mismatch, "updated_at": datetime(2026, 5, 1, tzinfo=UTC)},
        ],
    )

    summary = await cleanup_completed_games(db, apply=False)

    assert summary["scanned"] == 3
    assert summary["identical"] == 1
    assert summary["would_delete"] == 1
    assert summary["missing_archive"] == 1
    assert summary["mismatched_archive"] == 1
    assert db.games.delete_queries == []
    assert [doc["_id"] for doc in db.games.docs] == [identical_id, missing_id, mismatch_id, active_id]


@pytest.mark.asyncio
async def test_cleanup_completed_games_apply_deletes_identical_archived_docs_by_id_only() -> None:
    now = datetime(2026, 4, 30, tzinfo=UTC)
    identical_id = ObjectId()
    mismatch_id = ObjectId()
    identical = {"_id": identical_id, "game_code": "SAME01", "state": "completed", "updated_at": now}
    mismatch = {"_id": mismatch_id, "game_code": "DIFF01", "state": "completed", "updated_at": now}
    db = FakeDB(
        games=[identical, mismatch],
        game_archives=[
            identical.copy(),
            {**mismatch, "updated_at": datetime(2026, 5, 1, tzinfo=UTC)},
        ],
    )

    summary = await cleanup_completed_games(db, apply=True)

    assert summary["deleted"] == 1
    assert summary["mismatched_archive"] == 1
    assert db.games.delete_queries == [{"_id": identical_id}]
    assert [doc["_id"] for doc in db.games.docs] == [mismatch_id]
