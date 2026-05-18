from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import sys

import pytest
from bson import ObjectId
from pymongo import UpdateOne

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.archive_turn_counts import (  # noqa: E402
    archive_count_fields,
    backfill_archive_turn_counts,
    run_archive_turn_count_migration_once,
)


class FakeBulkResult:
    def __init__(self, modified_count: int):
        self.modified_count = modified_count


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


class FakeArchiveCollection:
    def __init__(self, docs: list[dict]):
        self.docs = docs
        self.find_calls: list[tuple[dict, dict | None]] = []
        self.bulk_calls: list[list[UpdateOne]] = []

    def find(self, query: dict, projection: dict | None = None):
        self.find_calls.append((query, projection))
        rows = [self._project(doc, projection) for doc in self.docs]
        return FakeCursor(rows)

    async def bulk_write(self, updates: list[UpdateOne], *, ordered: bool):  # noqa: ARG002
        self.bulk_calls.append(updates)
        modified = 0
        for update in updates:
            query = update._filter
            values = update._doc.get("$set", {})
            for doc in self.docs:
                if doc.get("_id") == query.get("_id"):
                    before = {key: doc.get(key) for key in values}
                    doc.update(values)
                    modified += 1 if before != values else 0
                    break
        return FakeBulkResult(modified)

    @staticmethod
    def _project(doc: dict, projection: dict | None):
        if projection is None:
            return deepcopy(doc)
        return {key: deepcopy(doc[key]) for key, include in projection.items() if include and key in doc}


class FakeDB:
    def __init__(self, docs: list[dict]):
        self.game_archives = FakeArchiveCollection(docs)
        self.maintenance_state = FakeMaintenanceCollection()


class FakeMaintenanceCollection:
    def __init__(self):
        self.docs: dict[str, dict] = {}
        self.update_calls: list[tuple[dict, dict, bool]] = []

    async def find_one(self, query: dict):
        doc = self.docs.get(query.get("_id"))
        return deepcopy(doc) if doc is not None else None

    async def update_one(self, query: dict, update: dict, *, upsert: bool):
        self.update_calls.append((query, update, upsert))
        doc = self.docs.setdefault(query["_id"], {"_id": query["_id"]})
        for key, value in update.get("$set", {}).items():
            doc[key] = value


def test_archive_count_fields_counts_completed_plies_as_turns() -> None:
    assert archive_count_fields(
        {
            "moves": [
                {"move_done": True},
                {"move_done": False},
                {"move_done": True},
                {"move_done": True},
                "legacy-ply",
            ]
        }
    ) == {"move_count": 5, "turn_count": 2}


@pytest.mark.asyncio
async def test_backfill_archive_turn_counts_dry_run_reports_updates_without_writes() -> None:
    needs_update = {"_id": ObjectId(), "game_code": "ABC123", "moves": [{"move_done": True}, {"move_done": True}]}
    current = {"_id": ObjectId(), "game_code": "CUR123", "moves": [], "move_count": 0, "turn_count": 0}
    missing_moves = {"_id": ObjectId(), "game_code": "MISS12"}
    db = FakeDB([needs_update, current, missing_moves])

    summary = await backfill_archive_turn_counts(db, apply=False)

    assert summary["scanned"] == 3
    assert summary["already_current"] == 1
    assert summary["would_update"] == 2
    assert summary["updated"] == 0
    assert summary["missing_moves_list"] == 1
    assert summary["missing_moves_examples"] == [{"_id": str(missing_moves["_id"]), "game_code": "MISS12"}]
    assert db.game_archives.bulk_calls == []


@pytest.mark.asyncio
async def test_backfill_archive_turn_counts_apply_updates_in_batches() -> None:
    first = {"_id": ObjectId(), "game_code": "ABC123", "moves": [{"move_done": True}, {"move_done": True}]}
    second = {"_id": ObjectId(), "game_code": "DEF456", "moves": [{"move_done": True}], "move_count": 99, "turn_count": 99}
    db = FakeDB([first, second])

    summary = await backfill_archive_turn_counts(db, apply=True, batch_size=1)

    assert summary["would_update"] == 2
    assert summary["updated"] == 2
    assert len(db.game_archives.bulk_calls) == 2
    assert db.game_archives.docs[0]["move_count"] == 2
    assert db.game_archives.docs[0]["turn_count"] == 1
    assert db.game_archives.docs[1]["move_count"] == 1
    assert db.game_archives.docs[1]["turn_count"] == 1


@pytest.mark.asyncio
async def test_run_archive_turn_count_migration_once_records_completion_marker() -> None:
    game = {"_id": ObjectId(), "game_code": "ABC123", "moves": [{"move_done": True}, {"move_done": True}]}
    db = FakeDB([game])

    summary = await run_archive_turn_count_migration_once(db, batch_size=10)
    second_summary = await run_archive_turn_count_migration_once(db, batch_size=10)

    assert summary["updated"] == 1
    assert summary["skipped"] is False
    marker = next(iter(db.maintenance_state.docs.values()))
    assert marker["status"] == "completed"
    assert marker["summary"]["updated"] == 1
    assert second_summary["skipped"] is True
