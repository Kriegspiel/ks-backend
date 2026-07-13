from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from bson import ObjectId

from app.models.bot import BotUsageReportRequest
from app.services.game_usage_stats import (
    LlmUsageReport,
    _find_one,
    _game_queries,
    _apply_update_to_document,
    _update_one,
    apply_llm_usage_to_game_stats,
    game_llm_usage_for_color,
    game_usage_report_summary,
    store_llm_usage_in_game_stats,
    usage_color_for_game,
    usage_token_split,
)


def _report(**overrides) -> LlmUsageReport:
    payload = {
        "game_id": "ABC123",
        "game_code": None,
        "bot_user_id": "bot1",
        "bot_username": "gptnano",
        "provider": "openai",
        "model": "gpt-5.4-nano",
        "response_id": "resp1",
        "input_tokens": 100,
        "cached_input_tokens": 10,
        "output_tokens": 30,
        "cache_read_input_tokens": 5,
        "cache_creation_input_tokens": 7,
        "total_tokens": 142,
        "cost_usd": 0.002,
    }
    payload.update(overrides)
    return LlmUsageReport(**payload)


class FindFallbackCollection:
    def __init__(self, doc: dict | None) -> None:
        self.doc = doc
        self.calls: list[tuple] = []

    async def find_one(self, *args):
        self.calls.append(args)
        if len(args) == 2:
            raise TypeError("projection unsupported")
        return self.doc


class UpdateOneCollection:
    def __init__(self, matched_count: int) -> None:
        self.matched_count = matched_count
        self.calls: list[tuple[dict, dict]] = []

    async def update_one(self, query: dict, update: dict):
        self.calls.append((query, update))
        return type("UpdateResult", (), {"matched_count": self.matched_count})()


class FindOneAndUpdateCollection:
    def __init__(self, updated: dict | None) -> None:
        self.updated = updated
        self.calls: list[tuple[dict, dict]] = []

    async def find_one_and_update(self, query: dict, update: dict):
        self.calls.append((query, update))
        return self.updated


class GameCollection:
    def __init__(self, docs: list[dict]) -> None:
        self.docs = docs
        self.update_matches = True

    async def find_one(self, query: dict, projection: dict | None = None):  # noqa: ARG002
        for doc in self.docs:
            if self._matches(doc, query):
                return doc
        return None

    async def update_one(self, query: dict, update: dict):
        if not self.update_matches:
            return type("UpdateResult", (), {"matched_count": 0})()
        for doc in self.docs:
            if self._matches(doc, query):
                self._apply_update(doc, update)
                return type("UpdateResult", (), {"matched_count": 1})()
        return type("UpdateResult", (), {"matched_count": 0})()

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
        for part in key.split(".")[:-1]:
            current = current.setdefault(part, {})
        current[key.split(".")[-1]] = value

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


def test_usage_report_from_record_normalizes_legacy_and_bad_values() -> None:
    report = LlmUsageReport.from_record(
        {
            "_id": ObjectId("507f1f77bcf86cd799439011"),
            "game_id": "507f1f77bcf86cd799439012",
            "game_code": " abc123 ",
            "bot_user_id": " bot1 ",
            "bot_username": " gptnano ",
            "provider": " OPENAI ",
            "model": "",
            "input_tokens": True,
            "cached_input_tokens": -5,
            "output_tokens": "bad",
            "cache_read_input_tokens": "7",
            "cache_creation_input_tokens": None,
            "total_tokens": 0,
            "cost_usd": float("nan"),
        }
    )

    assert report.response_id == "legacy:507f1f77bcf86cd799439011"
    assert report.game_code == "ABC123"
    assert report.provider == "openai"
    assert report.model == "unknown"
    assert report.input_tokens == 0
    assert report.cache_read_input_tokens == 7
    assert report.total_tokens == 7
    assert report.cost_usd == 0.0

    explicit = LlmUsageReport.from_record({"response_id": " resp ", "provider": "", "model": "llama-3.1-8b-instant"})
    assert explicit.response_id == "resp"
    assert explicit.provider == "unknown"


def test_usage_helpers_handle_aliases_empty_usage_and_token_splits() -> None:
    game = {
        "white": {"user_id": "bot1", "username": "someone"},
        "black": {"user_id": "other", "username": "llm_llama31_8b"},
        "stats": {"llm_usage": {"white": {"calls": 0, "total_tokens": 0, "cost_usd": 0}}},
    }

    assert usage_color_for_game(game, _report(bot_user_id="bot1")) == "white"
    assert usage_color_for_game(game, _report(bot_user_id="", bot_username="", model="missing")) is None
    assert usage_color_for_game(game, _report(bot_user_id="", bot_username="openrouterbot", model="llama 8b")) == "black"
    assert usage_color_for_game({"white": "bad"}, _report(bot_user_id="", bot_username="missing", model="missing")) is None
    assert game_llm_usage_for_color({"stats": {"llm_usage": {}}}, "white") is None
    assert game_llm_usage_for_color(game, "white") is None
    assert game_llm_usage_for_color({"stats": {"llm_usage": {"white": {"cost_usd": True}}}}, "white") is None
    assert game_llm_usage_for_color({"stats": {"llm_usage": {"white": {"cost_usd": "bad"}}}}, "white") is None
    assert game_llm_usage_for_color({"stats": {"llm_usage": {"black": {"cost_usd": 0.5}}}}, "black") == {"cost_usd": 0.5}

    assert usage_token_split(
        {"input_tokens": 5, "cached_input_tokens": 9, "cache_read_input_tokens": True, "output_tokens": "7"}
    ) == (0, 9, 7, 16)
    assert game_usage_report_summary(
        {"calls": True, "input_tokens": "bad", "total_tokens": 25, "cost_usd": "-1"}
    ) == {"calls": 0, "tokens": 25, "input_tokens": 0, "cache_tokens": 0, "output_tokens": 0, "cost": 0.0}


def test_usage_report_from_payload_and_direct_update_branches() -> None:
    payload = BotUsageReportRequest(
        game_id=" game1 ",
        game_code=" abc123 ",
        provider=" OPENAI ",
        model=" gpt-5.4-nano ",
        response_id=" resp1 ",
        input_tokens=10,
        cached_input_tokens=2,
        output_tokens=3,
        cache_read_input_tokens=4,
        cache_creation_input_tokens=5,
        total_tokens=99,
        cost_usd=0.01,
    )

    report = LlmUsageReport.from_payload(user_id="bot1", username=" llm_gptnano ", payload=payload)

    assert report.game_id == "game1"
    assert report.game_code == "ABC123"
    assert report.provider == "openai"
    assert report.model == "gpt-5.4-nano"
    assert report.response_id == "resp1"
    assert report.total_tokens == 99

    doc = {"stats": {"llm_usage": {"white": {"providers": ["openai"], "first_recorded_at": 1}}}}
    _apply_update_to_document(
        doc,
        {
            "$addToSet": {"stats.llm_usage.white.providers": "openai"},
            "$min": {"stats.llm_usage.white.first_recorded_at": 2},
        },
    )
    assert doc["stats"]["llm_usage"]["white"]["providers"] == ["openai"]
    assert doc["stats"]["llm_usage"]["white"]["first_recorded_at"] == 1

    with pytest.raises(TypeError):
        _apply_update_to_document({"stats": 1}, {"$addToSet": {"stats.llm_usage": "openai"}})


def test_apply_llm_usage_updates_documents_and_skips_duplicates() -> None:
    now = datetime(2026, 7, 5, 12, tzinfo=UTC)
    game = {
        "white": {"user_id": "", "username": ""},
        "black": {"user_id": "bot1", "username": "llm_gptnano"},
        "stats": {
            "llm_usage": {
                "black": {
                    "response_ids": ["resp0"],
                    "calls": 1,
                    "providers": "legacy",
                    "models": ["old-model"],
                    "first_recorded_at": now + timedelta(hours=1),
                }
            }
        },
    }

    assert apply_llm_usage_to_game_stats(game, _report(response_id="resp0"), now=now) is True
    assert game["stats"]["llm_usage"]["black"]["calls"] == 1

    assert apply_llm_usage_to_game_stats(game, _report(response_id="resp1"), now=now) is True
    stored = game["stats"]["llm_usage"]["black"]
    assert stored["calls"] == 2
    assert stored["providers"] == ["openai"]
    assert stored["models"] == ["old-model", "gpt-5.4-nano"]
    assert stored["response_ids"] == ["resp0", "resp1"]
    assert stored["first_recorded_at"] == now
    assert apply_llm_usage_to_game_stats({"white": {"username": "other"}}, _report(bot_user_id=""), now=now) is False


@pytest.mark.asyncio
async def test_collection_helpers_cover_projection_and_update_fallbacks() -> None:
    doc = {"game_code": "ABC123"}
    collection = FindFallbackCollection(doc)

    assert await _find_one(collection, {"game_code": "ABC123"}) == doc
    assert collection.calls == [
        ({"game_code": "ABC123"}, {"white": 1, "black": 1, "stats.llm_usage": 1, "game_code": 1}),
        ({"game_code": "ABC123"},),
    ]

    update_one = UpdateOneCollection(2)
    assert await _update_one(update_one, {"a": 1}, {"$set": {"b": 2}}) == 2
    fallback = FindOneAndUpdateCollection({"ok": True})
    assert await _update_one(fallback, {"a": 1}, {"$set": {"b": 2}}) == 1
    missing = FindOneAndUpdateCollection(None)
    assert await _update_one(missing, {"a": 1}, {"$set": {"b": 2}}) == 0
    assert await _update_one(object(), {"a": 1}, {"$set": {"b": 2}}) == 0


@pytest.mark.asyncio
async def test_store_llm_usage_in_game_stats_covers_query_and_race_paths() -> None:
    now = datetime(2026, 7, 5, 12, tzinfo=UTC)
    oid = ObjectId()
    report = _report(game_id=str(oid), game_code="ABC123", response_id="resp1")
    game = {
        "_id": oid,
        "game_code": "ABC123",
        "white": {"user_id": "bot1", "username": "llm_gptnano"},
        "black": {"user_id": "u2", "username": "player"},
    }
    collection = GameCollection([game])

    assert await store_llm_usage_in_game_stats((), report, now=now) is False
    assert await store_llm_usage_in_game_stats((None,), report, now=now) is False
    assert await store_llm_usage_in_game_stats((collection,), report, now=now) is True
    assert game["stats"]["llm_usage"]["white"]["response_ids"] == ["resp1"]

    assert await store_llm_usage_in_game_stats((collection,), report, now=now) is True
    assert game["stats"]["llm_usage"]["white"]["calls"] == 1

    collection.update_matches = False
    assert await store_llm_usage_in_game_stats((collection,), _report(game_id=str(oid), response_id="resp2"), now=now) is False

    no_color = GameCollection(
        [{"_id": "ABC123", "game_code": "ABC123", "white": {"username": "other"}, "black": {"username": "also-other"}}]
    )
    assert await store_llm_usage_in_game_stats((no_color,), _report(bot_user_id="", game_id="ABC123"), now=now) is False

    queries = _game_queries(_report(game_id="ABC123", game_code="abc123"))
    assert queries == [{"_id": "ABC123"}, {"game_code": "ABC123"}]
    oid = ObjectId()
    assert _game_queries(_report(game_id=str(oid), game_code=None)) == [{"_id": oid}, {"_id": str(oid)}]
    assert _game_queries(_report(game_id="", game_code="code12")) == [{"game_code": "CODE12"}]

    no_response_id_game = {
        "_id": "no-response",
        "white": {"user_id": "bot1", "username": "llm_gptnano"},
        "black": {"user_id": "u2", "username": "player"},
    }
    no_response_id_collection = GameCollection([no_response_id_game])
    assert await store_llm_usage_in_game_stats(
        (no_response_id_collection,),
        _report(game_id="no-response", game_code=None, response_id=None),
        now=now,
    ) is True
    assert "response_ids" not in no_response_id_game["stats"]["llm_usage"]["white"]
