from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime

import pytest
from bson import ObjectId

from app.services.bot_usage_game_stats import (
    BOT_USAGE_GAME_STATS_MIGRATION_ID,
    backfill_bot_usage_records_to_game_stats,
    run_bot_usage_game_stats_migration_once,
)


class FakeCursor:
    def __init__(self, docs: list[dict]) -> None:
        self.docs = docs

    def limit(self, count: int):
        self.docs = self.docs[:count]
        return self

    def batch_size(self, _count: int):
        return self

    def __aiter__(self):
        self.index = 0
        return self

    async def __anext__(self):
        if self.index >= len(self.docs):
            raise StopAsyncIteration
        doc = self.docs[self.index]
        self.index += 1
        return deepcopy(doc)


class FakeCollection:
    def __init__(self, docs: list[dict] | None = None) -> None:
        self.docs = docs or []

    def find(self, _query: dict, _projection: dict | None = None):
        return FakeCursor(self.docs)

    async def find_one(self, query: dict, _projection: dict | None = None):
        for doc in self.docs:
            if self._matches(doc, query):
                return deepcopy(doc)
        return None

    async def update_one(self, query: dict, update: dict, *, upsert: bool = False):
        for doc in self.docs:
            if self._matches(doc, query):
                self._apply_update(doc, update)
                return type("UpdateResult", (), {"matched_count": 1, "modified_count": 1})()
        if upsert:
            doc = {"_id": query.get("_id")}
            self._apply_update(doc, update)
            self.docs.append(doc)
        return type("UpdateResult", (), {"matched_count": 0, "modified_count": 0})()

    @classmethod
    def _matches(cls, doc: dict, query: dict) -> bool:
        for key, expected in query.items():
            current = cls._resolve(doc, key)
            if isinstance(expected, dict) and "$ne" in expected:
                disallowed = expected["$ne"]
                if isinstance(current, list) and disallowed in current:
                    return False
                if current == disallowed:
                    return False
                continue
            if current != expected:
                return False
        return True

    @staticmethod
    def _resolve(doc: dict, key: str):
        current = doc
        for part in key.split("."):
            if not isinstance(current, dict):
                return None
            current = current.get(part)
        return current

    @classmethod
    def _set_nested(cls, doc: dict, key: str, value) -> None:  # noqa: ANN001
        current = doc
        parts = key.split(".")
        for part in parts[:-1]:
            current = current.setdefault(part, {})
        current[parts[-1]] = value

    @classmethod
    def _apply_update(cls, doc: dict, update: dict) -> None:
        for key, value in update.get("$set", {}).items():
            cls._set_nested(doc, key, value)
        for key, value in update.get("$inc", {}).items():
            cls._set_nested(doc, key, (cls._resolve(doc, key) or 0) + value)
        for key, value in update.get("$addToSet", {}).items():
            current = cls._resolve(doc, key)
            if not isinstance(current, list):
                current = []
                cls._set_nested(doc, key, current)
            if value not in current:
                current.append(value)
        for key, value in update.get("$min", {}).items():
            current = cls._resolve(doc, key)
            if current is None or value < current:
                cls._set_nested(doc, key, value)


class FakeMaintenanceCollection(FakeCollection):
    async def find_one(self, query: dict, _projection: dict | None = None):
        for doc in self.docs:
            if doc.get("_id") == query.get("_id"):
                return deepcopy(doc)
        return None


class FakeDB:
    def __init__(self, *, usage_records: list[dict], archive_docs: list[dict]) -> None:
        self.bot_usage_records = FakeCollection(usage_records)
        self.games = FakeCollection([])
        self.game_archives = FakeCollection(archive_docs)
        self.maintenance_state = FakeMaintenanceCollection([])


@pytest.mark.asyncio
async def test_backfill_bot_usage_records_to_game_stats_maps_legacy_model_usage() -> None:
    game_id = ObjectId()
    usage_id = ObjectId()
    db = FakeDB(
        usage_records=[
            {
                "_id": usage_id,
                "game_id": str(game_id),
                "bot_username": "openrouterbot",
                "model": "meta-llama/llama-3.1-8b-instruct",
                "provider": "openrouter",
                "input_tokens": 1000,
                "cached_input_tokens": 200,
                "output_tokens": 50,
                "total_tokens": 1050,
                "cost_usd": 0.00123,
                "recorded_at": datetime(2026, 7, 5, 11, tzinfo=UTC),
            }
        ],
        archive_docs=[
            {
                "_id": game_id,
                "game_code": "LLAMA1",
                "white": {"user_id": "haiku-id", "username": "llm_haiku", "role": "bot"},
                "black": {"user_id": "llama-id", "username": "llm_llama31_8b", "role": "bot"},
            }
        ],
    )

    summary = await backfill_bot_usage_records_to_game_stats(db)
    second_summary = await backfill_bot_usage_records_to_game_stats(db)

    usage = db.game_archives.docs[0]["stats"]["llm_usage"]["black"]
    assert summary["scanned"] == 1
    assert summary["stored"] == 1
    assert second_summary["stored"] == 1
    assert usage["username"] == "llm_llama31_8b"
    assert usage["calls"] == 1
    assert usage["input_tokens"] == 1000
    assert usage["cached_input_tokens"] == 200
    assert usage["output_tokens"] == 50
    assert usage["total_tokens"] == 1050
    assert usage["cost_usd"] == pytest.approx(0.00123)
    assert usage["response_ids"] == [f"legacy:{usage_id}"]


@pytest.mark.asyncio
async def test_run_bot_usage_game_stats_migration_once_records_completion_marker() -> None:
    game_id = ObjectId()
    db = FakeDB(
        usage_records=[
            {
                "_id": ObjectId(),
                "game_id": str(game_id),
                "bot_user_id": "bot1",
                "bot_username": "llm_gptnano",
                "provider": "openai",
                "model": "gpt-5.4-nano",
                "response_id": "resp1",
                "input_tokens": 10,
                "output_tokens": 2,
                "total_tokens": 12,
                "cost_usd": 0.0001,
            }
        ],
        archive_docs=[
            {
                "_id": game_id,
                "game_code": "NANO12",
                "white": {"user_id": "bot1", "username": "llm_gptnano", "role": "bot"},
                "black": {"user_id": "human1", "username": "playerone", "role": "user"},
            }
        ],
    )

    summary = await run_bot_usage_game_stats_migration_once(db)
    second_summary = await run_bot_usage_game_stats_migration_once(db)

    marker = await db.maintenance_state.find_one({"_id": BOT_USAGE_GAME_STATS_MIGRATION_ID})
    assert summary["stored"] == 1
    assert summary["skipped"] is False
    assert marker["status"] == "completed"
    assert marker["summary"]["stored"] == 1
    assert second_summary["skipped"] is True
