from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from bson import ObjectId
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.config import Settings
from app.llm_bot_policy import KNOWN_LLM_BOT_USERNAMES, bot_required_tier_for_username
from app.main import create_app
from app.models.bot import BotAvailabilityReportRequest, BotProfileSyncRequest, BotUsageReportRequest
from app.models.game import CreateGameRequest
from app.routers.bot import report_bot_availability, report_bot_usage, sync_bot_profile
from app.services.bot_service import BotService
from app.services.game_service import GameConflictError, GameForbiddenError, GameService, GameValidationError
from app.services.user_service import UserService
from tests.test_game_service import FakeGamesCollection

T2_LLM_BOT_USERNAMES = (
    "llm_gptnano",
    "llm_gpt45nano",
    "llm_haiku",
    "llm_deepseekv4_flash",
    "llm_gemini25_lite",
    "llm_gemini31_lite",
    "llm_gptoss120b",
    "llm_llama31_8b",
    "llm_llama4_scout",
    "llm_llama4_maverick",
    "llm_mistral_nemo",
    "llm_mistral_small32",
    "llm_gemma3_4b",
    "llm_gemma3_27b",
    "llm_gemma4_31b",
    "llm_glm47_flash",
    "llm_glm45_air",
    "llm_nemotron_nano",
    "llm_nemotron_super",
    "llm_kimi_k25",
    "llm_hermes4_70b",
    "llm_phi4",
)

T4_LLM_BOT_PROVIDERS = {
    "llm_opus48": "anthropic",
    "bot_deepseekv4_pro": "openai",
    "llm_gemini31_pro_preview": "openai",
    "llm_glm52": "openai",
    "llm_kimi_k27_code": "openai",
    "llm_hermes4_405b": "openai",
}

T3_LLM_BOT_PROVIDERS = {
    "llm_gpt55": "openai",
    "llm_sonnet5": "anthropic",
    "llm_gemini25_flash": "openai",
    "llm_mistral_large3": "openai",
    "llm_nemotron_ultra": "openai",
    "llm_qwen36_flash": "openai",
    "llm_qwen_plus": "openai",
    "llm_kimi_k2_thinking": "openai",
    "llm_hermes3_70b": "openai",
}


class FakeUsersCollection:
    def __init__(self):
        self.docs: list[dict] = []

    async def find_one(self, query: dict, projection: dict | None = None):
        for doc in self.docs:
            if self._matches(doc, query):
                return doc
        return None

    async def insert_one(self, document: dict):
        doc = dict(document)
        doc["_id"] = ObjectId()
        self.docs.append(doc)
        return type("InsertResult", (), {"inserted_id": doc["_id"]})

    async def find_one_and_update(self, query: dict, update: dict, return_document=None):  # noqa: ANN001
        for doc in self.docs:
            if self._matches(doc, query):
                for key, value in update.get("$set", {}).items():
                    current = doc
                    parts = key.split(".")
                    for part in parts[:-1]:
                        current = current.setdefault(part, {})
                    current[parts[-1]] = value
                return doc
        return None

    def find(self, query: dict):
        rows = [doc for doc in self.docs if self._matches(doc, query)]

        class Cursor:
            def __init__(self, items):
                self.items = items

            def sort(self, field, direction):
                self.items.sort(key=lambda row: row[field], reverse=direction < 0)
                return self

            def __aiter__(self):
                self.index = 0
                return self

            async def __anext__(self):
                if self.index >= len(self.items):
                    raise StopAsyncIteration
                item = self.items[self.index]
                self.index += 1
                return item

        return Cursor(rows)

    @staticmethod
    def _matches(doc: dict, query: dict) -> bool:
        for key, expected in query.items():
            current = doc
            for part in key.split("."):
                if not isinstance(current, dict):
                    return False
                current = current.get(part)
            if isinstance(expected, dict):
                if "$ne" in expected and current == expected["$ne"]:
                    return False
                continue
            if current != expected:
                return False
        return True


class FakeReferenceCollection:
    def __init__(self, docs: list[dict] | None = None) -> None:
        self.docs = docs or []

    async def find_one(self, query: dict, projection: dict | None = None):
        for doc in self.docs:
            if self._matches(doc, query):
                return doc
        return None

    async def update_one(self, query: dict, update: dict):
        for doc in self.docs:
            if self._matches(doc, query):
                self._apply_update(doc, update)
                return type("UpdateResult", (), {"matched_count": 1, "modified_count": 1})()
        return type("UpdateResult", (), {"matched_count": 0, "modified_count": 0})()

    async def update_many(self, query: dict, update: dict):
        matched_count = 0
        for doc in self.docs:
            if self._matches(doc, query):
                matched_count += 1
                self._apply_update(doc, update)
        return type("UpdateResult", (), {"matched_count": matched_count, "modified_count": matched_count})()

    @classmethod
    def _matches(cls, doc: dict, query: dict) -> bool:
        for key, expected in query.items():
            current = cls._resolve(doc, key)
            if isinstance(expected, dict) and "$ne" in expected:
                disallowed = expected["$ne"]
                if isinstance(current, list):
                    if disallowed in current:
                        return False
                elif current == disallowed:
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

    @staticmethod
    def _set_nested(doc: dict, key: str, value) -> None:  # noqa: ANN001
        current = doc
        parts = key.split(".")
        for part in parts[:-1]:
            current = current.setdefault(part, {})
        current[parts[-1]] = value

    @classmethod
    def _inc_nested(cls, doc: dict, key: str, value) -> None:  # noqa: ANN001
        current = cls._resolve(doc, key) or 0
        cls._set_nested(doc, key, current + value)

    @classmethod
    def _add_to_set_nested(cls, doc: dict, key: str, value) -> None:  # noqa: ANN001
        current = cls._resolve(doc, key)
        if not isinstance(current, list):
            current = []
            cls._set_nested(doc, key, current)
        if value not in current:
            current.append(value)

    @classmethod
    def _min_nested(cls, doc: dict, key: str, value) -> None:  # noqa: ANN001
        current = cls._resolve(doc, key)
        if current is None or value < current:
            cls._set_nested(doc, key, value)

    @classmethod
    def _apply_update(cls, doc: dict, update: dict) -> None:
        for key, value in update.get("$set", {}).items():
            cls._set_nested(doc, key, value)
        for key, value in update.get("$inc", {}).items():
            cls._inc_nested(doc, key, value)
        for key, value in update.get("$addToSet", {}).items():
            cls._add_to_set_nested(doc, key, value)
        for key, value in update.get("$min", {}).items():
            cls._min_nested(doc, key, value)


@pytest.mark.asyncio
async def test_create_game_with_bot_immediately_activates() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    bot_id = ObjectId()
    users.docs.append(
        {
            "_id": bot_id,
            "username": "randobot",
            "username_display": "Random Bot",
            "role": "bot",
            "status": "active",
            "bot_profile": {
                "display_name": "Random Bot",
                "owner_email": "owner@example.com",
                "description": "Plays random moves",
                "supported_rule_variants": ["berkeley", "berkeley_any"],
            },
            "stats": {"elo": 1315},
        }
    )
    service = GameService(games, users_collection=users, site_origin="https://kriegspiel.org")

    response = await service.create_game(
        user_id="u1",
        username="creator",
        request=CreateGameRequest(opponent_type="bot", bot_id=str(bot_id), play_as="white", time_control="rapid"),
    )

    assert response.state == "active"
    assert response.bot == {"bot_id": str(bot_id), "username": "randobot"}
    assert games.docs[0]["black"]["role"] == "bot"
    assert games.docs[0]["state"] == "active"


@pytest.mark.asyncio
async def test_create_llm_bot_game_stores_viewer_tier_without_human_cap() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    bot_id = ObjectId()
    now = datetime(2026, 6, 1, tzinfo=UTC)
    users.docs.append(
        {
            "_id": bot_id,
            "username": "llm_gptnano",
            "username_display": "LLM GPT-4.5 Nano (bot)",
            "role": "bot",
            "status": "active",
            "bot_profile": {
                "display_name": "LLM GPT-4.5 Nano (bot)",
                "owner_email": "owner@example.com",
                "description": "Model bot",
                "supported_rule_variants": ["berkeley", "berkeley_any"],
                "model_availability": {"provider": "openai", "ready": True, "reason": "ok", "checked_at": now},
            },
        }
    )
    service = GameService(games, users_collection=users, site_origin="https://kriegspiel.org")
    service.utcnow = lambda: now  # type: ignore[method-assign]

    response = await service.create_game(
        user_id="u1",
        username="creator",
        request=CreateGameRequest(opponent_type="bot", bot_id=str(bot_id), play_as="white", time_control="rapid"),
        llm_bot_tier="tier3",
    )

    assert response.state == "active"
    assert games.docs[0]["llm_bot_tier"] == "tier3"
    assert games.docs[0]["llm_bot_ply_limit"] is None
    assert games.docs[0]["llm_bot_user_id"] == str(bot_id)


@pytest.mark.asyncio
async def test_guest_cannot_create_llm_bot_game() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    bot_id = ObjectId()
    now = datetime(2026, 6, 1, tzinfo=UTC)
    users.docs.append(
        {
            "_id": bot_id,
            "username": "llm_gptnano",
            "username_display": "LLM GPT-4.5 Nano (bot)",
            "role": "bot",
            "status": "active",
            "bot_profile": {
                "display_name": "LLM GPT-4.5 Nano (bot)",
                "owner_email": "owner@example.com",
                "description": "Model bot",
                "model_availability": {"provider": "openai", "ready": True, "reason": "ok", "checked_at": now},
            },
        }
    )
    service = GameService(games, users_collection=users, site_origin="https://kriegspiel.org")
    service.utcnow = lambda: now  # type: ignore[method-assign]

    with pytest.raises(GameForbiddenError) as exc:
        await service.create_game(
            user_id="guest1",
            username="guest_player",
            request=CreateGameRequest(opponent_type="bot", bot_id=str(bot_id), play_as="white", time_control="rapid"),
            role="guest",
        )

    assert exc.value.code == "LLM_BOT_TIER_REQUIRED"


@pytest.mark.asyncio
async def test_tier_one_cannot_create_higher_tier_bot_game() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    bot_id = ObjectId()
    now = datetime(2026, 6, 1, tzinfo=UTC)
    users.docs.append(
        {
            "_id": bot_id,
            "username": "llm_gptnano",
            "username_display": "LLM GPT-4.5 Nano (bot)",
            "role": "bot",
            "status": "active",
            "bot_profile": {
                "display_name": "LLM GPT-4.5 Nano (bot)",
                "owner_email": "owner@example.com",
                "description": "Model bot",
                "model_availability": {"provider": "openai", "ready": True, "reason": "ok", "checked_at": now},
            },
        }
    )
    service = GameService(games, users_collection=users, site_origin="https://kriegspiel.org")
    service.utcnow = lambda: now  # type: ignore[method-assign]

    with pytest.raises(GameForbiddenError) as exc:
        await service.create_game(
            user_id="u1",
            username="player",
            request=CreateGameRequest(opponent_type="bot", bot_id=str(bot_id), play_as="white", time_control="rapid"),
        )

    assert exc.value.code == "LLM_BOT_TIER_REQUIRED"


@pytest.mark.asyncio
async def test_guest_cannot_create_tier_one_bot_game() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    bot_id = ObjectId()
    users.docs.append(
        {
            "_id": bot_id,
            "username": "simpleheuristics",
            "username_display": "Simple Heuristics Bot",
            "role": "bot",
            "status": "active",
            "bot_profile": {
                "display_name": "Simple Heuristics Bot",
                "owner_email": "owner@example.com",
                "description": "Casual bot",
                "supported_rule_variants": ["berkeley", "berkeley_any"],
            },
        }
    )
    service = GameService(games, users_collection=users, site_origin="https://kriegspiel.org")

    with pytest.raises(GameForbiddenError) as exc:
        await service.create_game(
            user_id="guest1",
            username="guest_player",
            request=CreateGameRequest(opponent_type="bot", bot_id=str(bot_id), play_as="white", time_control="rapid"),
            role="guest",
        )

    assert exc.value.code == "BOT_TIER_REQUIRED"


@pytest.mark.asyncio
async def test_guest_bot_list_shows_unavailable_higher_tier_bots_and_user_list_shows_limit() -> None:
    users = FakeUsersCollection()
    now = datetime(2026, 6, 1, tzinfo=UTC)
    users.docs.extend(
        [
            {
                "_id": ObjectId(),
                "username": "llm_gptnano",
                "username_display": "LLM GPT-4.5 Nano (bot)",
                "role": "bot",
                "status": "active",
                "bot_profile": {
                    "display_name": "LLM GPT-4.5 Nano (bot)",
                    "description": "Model bot",
                    "model_availability": {"provider": "openai", "ready": True, "reason": "ok", "checked_at": now},
                },
            },
            {
                "_id": ObjectId(),
                "username": "randobot",
                "username_display": "Random Bot",
                "role": "bot",
                "status": "active",
                "bot_profile": {"display_name": "Random Bot", "description": "Random bot"},
            },
            {
                "_id": ObjectId(),
                "username": "simpleheuristics",
                "username_display": "Simple Heuristics Bot",
                "role": "bot",
                "status": "active",
                "bot_profile": {"display_name": "Simple Heuristics Bot", "description": "Casual bot"},
            },
        ]
    )
    service = BotService(users, now_factory=lambda: now)

    guest_listing = await service.list_bots(viewer_role="guest")
    user_listing = await service.list_bots(viewer_role="user", viewer_llm_bot_tier="tier2")

    assert [bot.username for bot in guest_listing.bots] == ["llm_gptnano", "randobot", "simpleheuristics"]
    guest_bots = {bot.username: bot for bot in guest_listing.bots}
    assert guest_bots["llm_gptnano"].required_tier == "tier2"
    assert guest_bots["llm_gptnano"].available_for_viewer is False
    assert guest_bots["randobot"].required_tier == "guest"
    assert guest_bots["randobot"].available_for_viewer is True
    assert guest_bots["simpleheuristics"].required_tier == "tier1"
    assert guest_bots["simpleheuristics"].available_for_viewer is False
    llm_gptnano = next(bot for bot in user_listing.bots if bot.username == "llm_gptnano")
    assert llm_gptnano.llm_backed is True
    assert llm_gptnano.required_tier == "tier2"
    assert llm_gptnano.available_for_viewer is True
    assert llm_gptnano.llm_bot_tier == "tier2"
    assert llm_gptnano.llm_bot_ply_limit is None
    assert llm_gptnano.llm_bot_limit_label == "No ply limit"


def test_bot_service_datetime_and_query_helpers_cover_invalid_inputs() -> None:
    naive = datetime(2026, 6, 1, 12, 0, 0)

    assert BotService._normalize_utc_datetime(None) is None
    assert BotService._normalize_utc_datetime(naive) == naive.replace(tzinfo=UTC)
    assert BotService._active_bot_queries("not-an-object-id") == [
        {"_id": "not-an-object-id", "role": "bot", "status": "active"}
    ]


def test_model_bot_availability_rejects_missing_wrong_stale_or_unready_reports() -> None:
    now = datetime(2026, 6, 1, tzinfo=UTC)

    assert BotService.bot_can_start_games({"username": "randobot"}, now=now) is True
    assert BotService.bot_can_start_games({"username": "llm_gptnano"}, now=now) is False
    assert (
        BotService.bot_can_start_games(
            {
                "username": "llm_gptnano",
                "bot_profile": {"model_availability": {"provider": "anthropic", "ready": True, "checked_at": now}},
            },
            now=now,
        )
        is False
    )
    assert (
        BotService.bot_can_start_games(
            {
                "username": "llm_gptnano",
                "bot_profile": {"model_availability": {"provider": "openai", "ready": False, "checked_at": now}},
            },
            now=now,
        )
        is False
    )
    assert (
        BotService.bot_can_start_games(
            {"username": "llm_gptnano", "bot_profile": {"model_availability": {"provider": "openai", "ready": True}}},
            now=now,
        )
        is False
    )
    assert (
        BotService.bot_can_start_games(
            {
                "username": "llm_gptnano",
                "bot_profile": {
                    "model_availability": {
                        "provider": "openai",
                        "ready": True,
                        "checked_at": now - timedelta(seconds=121),
                    }
                },
            },
            now=now,
        )
        is False
    )


def test_t2_llm_catalog_bots_are_known_and_availability_gated() -> None:
    now = datetime(2026, 7, 7, tzinfo=UTC)

    for username in T2_LLM_BOT_USERNAMES:
        provider = "anthropic" if username == "llm_haiku" else "openai"
        assert username in KNOWN_LLM_BOT_USERNAMES
        assert bot_required_tier_for_username(username) == "tier2"
        assert BotService.model_availability_required_provider({"username": username}) == provider
        assert BotService.bot_can_start_games({"username": username}, now=now) is False
        assert (
            BotService.bot_can_start_games(
                {
                    "username": username,
                    "bot_profile": {
                        "model_availability": {
                            "provider": provider,
                            "ready": True,
                            "checked_at": now,
                        }
                    },
                },
                now=now,
            )
            is True
        )


def test_t3_llm_catalog_bots_are_known_and_availability_gated() -> None:
    now = datetime(2026, 7, 11, tzinfo=UTC)

    for username, provider in T3_LLM_BOT_PROVIDERS.items():
        assert username in KNOWN_LLM_BOT_USERNAMES
        assert bot_required_tier_for_username(username) == "tier3"
        assert BotService.model_availability_required_provider({"username": username}) == provider
        assert BotService.bot_can_start_games({"username": username}, now=now) is False
        assert (
            BotService.bot_can_start_games(
                {
                    "username": username,
                    "bot_profile": {
                        "model_availability": {
                            "provider": provider,
                            "ready": True,
                            "checked_at": now,
                        }
                    },
                },
                now=now,
            )
            is True
        )


def test_t4_llm_catalog_bots_are_known_and_availability_gated() -> None:
    now = datetime(2026, 7, 11, tzinfo=UTC)

    for username, provider in T4_LLM_BOT_PROVIDERS.items():
        assert username in KNOWN_LLM_BOT_USERNAMES
        assert BotService.model_availability_required_provider({"username": username}) == provider
        assert BotService.bot_can_start_games({"username": username}, now=now) is False
        assert (
            BotService.bot_can_start_games(
                {
                    "username": username,
                    "bot_profile": {
                        "model_availability": {
                            "provider": provider,
                            "ready": True,
                            "checked_at": now,
                        }
                    },
                },
                now=now,
            )
            is True
        )


@pytest.mark.asyncio
async def test_bot_profile_updates_return_none_when_no_active_bot_matches() -> None:
    service = BotService(FakeUsersCollection(), now_factory=lambda: datetime(2026, 6, 1, tzinfo=UTC))

    assert await service.report_model_availability(user_id="missing", provider="openai", ready=True, reason="ok") is None
    assert await service.sync_supported_rule_variants(user_id="missing", supported_rule_variants=["berkeley"]) is None


@pytest.mark.asyncio
async def test_bot_routes_reject_non_bot_users_and_missing_bot_updates() -> None:
    user = type("User", (), {"id": "u1", "role": "user"})()
    bot_user = type("User", (), {"id": "bot1", "username": "llm_gptnano", "role": "bot"})()
    availability = BotAvailabilityReportRequest(provider="openai", ready=True, reason="ok")
    profile = BotProfileSyncRequest(supported_rule_variants=["berkeley"])
    usage = BotUsageReportRequest(
        game_id="game1",
        provider="openai",
        model="gpt-nano",
        response_id="resp1",
        input_tokens=100,
        output_tokens=20,
        total_tokens=120,
        cost_usd=0.001,
    )
    bot_service = type(
        "BotServiceStub",
        (),
        {
            "report_model_availability": AsyncMock(return_value=None),
            "sync_supported_rule_variants": AsyncMock(return_value=None),
            "record_usage": AsyncMock(return_value=False),
        },
    )()

    with pytest.raises(HTTPException) as availability_forbidden:
        await report_bot_availability(availability, user=user, bot_service=bot_service)
    with pytest.raises(HTTPException) as usage_forbidden:
        await report_bot_usage(usage, user=user, bot_service=bot_service)
    with pytest.raises(HTTPException) as profile_forbidden:
        await sync_bot_profile(profile, user=user, bot_service=bot_service)
    with pytest.raises(HTTPException) as availability_missing:
        await report_bot_availability(availability, user=bot_user, bot_service=bot_service)
    with pytest.raises(HTTPException) as usage_unavailable:
        await report_bot_usage(usage, user=bot_user, bot_service=bot_service)
    with pytest.raises(HTTPException) as profile_missing:
        await sync_bot_profile(profile, user=bot_user, bot_service=bot_service)

    assert availability_forbidden.value.status_code == 403
    assert usage_forbidden.value.status_code == 403
    assert profile_forbidden.value.status_code == 403
    assert availability_missing.value.status_code == 404
    assert usage_unavailable.value.status_code == 503
    assert profile_missing.value.status_code == 404


@pytest.mark.asyncio
async def test_bot_usage_report_stores_idempotent_game_stats() -> None:
    game_id = ObjectId("507f1f77bcf86cd799439011")
    games = FakeReferenceCollection(
        [
            {
                "_id": game_id,
                "game_code": "ABC123",
                "white": {"user_id": "bot1", "username": "llm_gptnano", "role": "bot"},
                "black": {"user_id": "human1", "username": "playerone", "role": "user"},
            }
        ]
    )
    service = BotService(
        FakeUsersCollection(),
        game_collections=(games,),
        now_factory=lambda: datetime(2026, 7, 5, tzinfo=UTC),
    )
    payload = BotUsageReportRequest(
        game_id="507f1f77bcf86cd799439011",
        game_code="abc123",
        provider="openai",
        model="gpt-nano",
        response_id="resp1",
        input_tokens=100,
        cached_input_tokens=20,
        output_tokens=30,
        total_tokens=130,
        cost_usd=0.002,
    )

    assert await service.record_usage(user_id="bot1", username="llm_gptnano", payload=payload) is True
    assert await service.record_usage(user_id="bot1", username="llm_gptnano", payload=payload) is True

    stored = games.docs[0]["stats"]["llm_usage"]["white"]
    assert stored["username"] == "llm_gptnano"
    assert stored["calls"] == 1
    assert stored["input_tokens"] == 100
    assert stored["cached_input_tokens"] == 20
    assert stored["output_tokens"] == 30
    assert stored["total_tokens"] == 130
    assert stored["cost_usd"] == 0.002
    assert stored["providers"] == ["openai"]
    assert stored["models"] == ["gpt-nano"]
    assert stored["response_ids"] == ["resp1"]
    assert stored["first_recorded_at"] == datetime(2026, 7, 5, tzinfo=UTC)
    assert games.docs[0]["stats"]["llm_usage"]["updated_at"] == datetime(2026, 7, 5, tzinfo=UTC)


@pytest.mark.asyncio
async def test_create_game_with_bot_rejects_unsupported_ruleset() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    bot_id = ObjectId()
    users.docs.append(
        {
            "_id": bot_id,
            "username": "randobotany",
            "username_display": "Random Any Bot",
            "role": "bot",
            "status": "active",
            "bot_profile": {
                "display_name": "Random Any Bot",
                "owner_email": "owner@example.com",
                "description": "Asks any first",
                "supported_rule_variants": ["berkeley_any"],
            },
        }
    )
    service = GameService(games, users_collection=users, site_origin="https://kriegspiel.org")

    with pytest.raises(GameValidationError) as exc:
        await service.create_game(
            user_id="u1",
            username="creator",
            request=CreateGameRequest(
                rule_variant="berkeley",
                opponent_type="bot",
                bot_id=str(bot_id),
                play_as="white",
                time_control="rapid",
            ),
        )

    assert exc.value.code == "BOT_RULE_VARIANT_UNSUPPORTED"


@pytest.mark.asyncio
async def test_bot_can_create_one_open_lobby_game_only() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    bot_id = ObjectId()
    users.docs.append(
        {
            "_id": bot_id,
            "username": "randobot",
            "role": "bot",
            "status": "active",
            "bot_profile": {"display_name": "Random Bot", "description": "Plays random moves"},
        }
    )
    service = GameService(games, users_collection=users, site_origin="https://kriegspiel.org")

    response = await service.create_game(
        user_id=str(bot_id),
        username="randobot",
        request=CreateGameRequest(opponent_type="human", play_as="white", time_control="rapid"),
        role="bot",
    )

    assert response.state == "waiting"
    assert games.docs[0]["white"]["role"] == "bot"
    assert games.docs[0]["expires_at"] - games.docs[0]["created_at"] == timedelta(minutes=10)

    with pytest.raises(GameConflictError) as exc:
        await service.create_game(
            user_id=str(bot_id),
            username="randobot",
            request=CreateGameRequest(opponent_type="human", play_as="black", time_control="rapid"),
            role="bot",
        )

    assert exc.value.code == "BOT_ALREADY_HAS_OPEN_GAME"


@pytest.mark.asyncio
async def test_bot_cannot_create_selected_bot_game() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    creator_id = ObjectId()
    opponent_id = ObjectId()
    users.docs.extend(
        [
            {
                "_id": creator_id,
                "username": "botcreator",
                "role": "bot",
                "status": "active",
                "bot_profile": {"display_name": "Bot Creator", "description": "Creates games"},
            },
            {
                "_id": opponent_id,
                "username": "otherbot",
                "role": "bot",
                "status": "active",
                "bot_profile": {"display_name": "Other Bot", "description": "Other bot"},
            },
        ]
    )
    service = GameService(games, users_collection=users)

    with pytest.raises(GameValidationError) as exc:
        await service.create_game(
            user_id=str(creator_id),
            username="botcreator",
            request=CreateGameRequest(opponent_type="bot", bot_id=str(opponent_id), time_control="rapid"),
            role="bot",
        )

    assert exc.value.code == "BOT_CREATE_REQUIRES_HUMAN_OPPONENT"


@pytest.mark.asyncio
async def test_join_rejects_bot_reserved_game() -> None:
    games = FakeGamesCollection()
    now = datetime.now(UTC)
    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "A7K2M9",
            "rule_variant": "berkeley_any",
            "creator_color": "white",
            "opponent_type": "bot",
            "selected_bot_id": "507f1f77bcf86cd799439012",
            "white": {"user_id": "u1", "username": "creator", "connected": True, "role": "user"},
            "black": None,
            "state": "waiting",
            "turn": None,
            "move_number": 1,
            "created_at": now,
            "updated_at": now,
        }
    )
    service = GameService(games)

    with pytest.raises(GameConflictError) as exc:
        await service.join_game(user_id="u2", username="joiner", game_code="A7K2M9")

    assert exc.value.code == "GAME_RESERVED_FOR_BOT"


@pytest.mark.asyncio
async def test_join_llm_bot_created_lobby_stores_joiner_tier_without_human_cap() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    bot_id = ObjectId()
    now = datetime.now(UTC)
    users.docs.append(
        {
            "_id": bot_id,
            "username": "llm_gptnano",
            "username_display": "LLM GPT-4.5 Nano (bot)",
            "role": "bot",
            "status": "active",
            "bot_profile": {
                "display_name": "LLM GPT-4.5 Nano (bot)",
                "description": "Model bot",
                "model_availability": {"provider": "openai", "ready": True, "reason": "ok", "checked_at": now},
            },
        }
    )
    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "G7K2M9",
            "rule_variant": "berkeley_any",
            "creator_color": "white",
            "opponent_type": "human",
            "selected_bot_id": None,
            "white": {"user_id": str(bot_id), "username": "llm_gptnano", "connected": True, "role": "bot"},
            "black": None,
            "state": "waiting",
            "turn": None,
            "move_number": 1,
            "created_at": now,
            "updated_at": now,
        }
    )
    service = GameService(games, users_collection=users)

    with pytest.raises(GameForbiddenError) as exc:
        await service.join_game(user_id="guest1", username="guest_player", game_code="G7K2M9", role="guest")

    assert exc.value.code == "LLM_BOT_TIER_REQUIRED"

    joined = await service.join_game(
        user_id="u1",
        username="player",
        game_code="G7K2M9",
        role="user",
        llm_bot_tier="tier2",
    )

    assert joined.state == "active"
    assert games.docs[0]["llm_bot_tier"] == "tier2"
    assert games.docs[0]["llm_bot_ply_limit"] is None
    assert games.docs[0]["llm_bot_user_id"] == str(bot_id)


@pytest.mark.asyncio
async def test_bot_vs_bot_join_assigns_distinct_random_llm_caps() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    creator_id = ObjectId()
    joiner_id = ObjectId()
    now = datetime.now(UTC)
    for bot_id, username in ((creator_id, "llm_gptnano"), (joiner_id, "llm_haiku")):
        users.docs.append(
            {
                "_id": bot_id,
                "username": username,
                "username_display": username,
                "role": "bot",
                "status": "active",
                "bot_profile": {
                    "display_name": username,
                    "description": "Model bot",
                    "model_availability": {"provider": "openai", "ready": True, "reason": "ok", "checked_at": now},
                },
            }
        )
    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "B7K2M9",
            "rule_variant": "berkeley_any",
            "creator_color": "white",
            "opponent_type": "human",
            "selected_bot_id": None,
            "white": {"user_id": str(creator_id), "username": "llm_gptnano", "connected": True, "role": "bot"},
            "black": None,
            "state": "waiting",
            "turn": None,
            "move_number": 1,
            "created_at": now,
            "updated_at": now,
        }
    )

    class Rng:
        values = [140, 220]

        def randint(self, lower: int, upper: int) -> int:
            assert (lower, upper) == (128, 256)
            return self.values.pop(0)

    service = GameService(games, users_collection=users, rng=Rng())

    joined = await service.join_game(
        user_id=str(joiner_id),
        username="llm_haiku",
        game_code="B7K2M9",
        role="bot",
    )

    assert joined.state == "active"
    assert games.docs[0]["llm_bot_turn_limits"] == {"white": 140, "black": 220}
    assert "llm_bot_ply_limits" not in games.docs[0]

    white_state = await service.get_game_state(game_id=str(games.docs[0]["_id"]), user_id=str(creator_id))
    black_state = await service.get_game_state(game_id=str(games.docs[0]["_id"]), user_id=str(joiner_id))

    assert white_state.llm_bot_ply_limit is None
    assert black_state.llm_bot_ply_limit is None
    assert white_state.llm_bot_turn_limit == 140
    assert black_state.llm_bot_turn_limit == 220


@pytest.mark.asyncio
async def test_bot_cannot_join_human_waiting_game() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    bot_id = ObjectId()
    users.docs.append(
        {
            "_id": bot_id,
            "username": "randobot",
            "role": "bot",
            "status": "active",
            "bot_profile": {"display_name": "Random Bot", "description": "Bot"},
        }
    )
    now = datetime.now(UTC)
    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "H7K2M9",
            "rule_variant": "berkeley_any",
            "creator_color": "white",
            "opponent_type": "human",
            "white": {"user_id": "u1", "username": "creator", "connected": True, "role": "user"},
            "black": None,
            "state": "waiting",
            "turn": None,
            "move_number": 1,
            "created_at": now,
            "updated_at": now,
        }
    )
    service = GameService(games, users_collection=users)

    with pytest.raises(GameForbiddenError) as exc:
        await service.join_game(user_id=str(bot_id), username="randobot", game_code="H7K2M9", role="bot")

    assert exc.value.code == "FORBIDDEN"


@pytest.mark.asyncio
async def test_bot_can_join_another_bot_game_once_per_minute_but_not_its_own() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    creator_id = ObjectId()
    joiner_id = ObjectId()
    users.docs.extend(
        [
            {
                "_id": creator_id,
                "username": "creatorbot",
                "role": "bot",
                "status": "active",
                "bot_profile": {"display_name": "Creator Bot", "description": "Creates"},
            },
            {
                "_id": joiner_id,
                "username": "joinerbot",
                "role": "bot",
                "status": "active",
                "bot_profile": {"display_name": "Joiner Bot", "description": "Joins"},
            },
        ]
    )
    now = datetime.now(UTC)
    games.docs.extend(
        [
            {
                "_id": ObjectId(),
                "game_code": "J7K2M9",
                "rule_variant": "berkeley_any",
                "creator_color": "white",
                "opponent_type": "human",
                "white": {"user_id": str(creator_id), "username": "creatorbot", "connected": True, "role": "bot"},
                "black": None,
                "state": "waiting",
                "turn": None,
                "move_number": 1,
                "created_at": now,
                "updated_at": now,
            },
            {
                "_id": ObjectId(),
                "game_code": "K7K2M9",
                "rule_variant": "berkeley_any",
                "creator_color": "white",
                "opponent_type": "human",
                "white": {"user_id": str(joiner_id), "username": "joinerbot", "connected": True, "role": "bot"},
                "black": None,
                "state": "waiting",
                "turn": None,
                "move_number": 1,
                "created_at": now,
                "updated_at": now,
            },
        ]
    )
    service = GameService(games, users_collection=users)
    service.utcnow = lambda: now  # type: ignore[method-assign]

    joined = await service.join_game(user_id=str(joiner_id), username="joinerbot", game_code="J7K2M9", role="bot")
    assert joined.state == "active"
    assert users.docs[1]["bot_profile"]["last_bot_game_joined_at"] == now

    with pytest.raises(GameConflictError) as own_exc:
        await service.join_game(user_id=str(joiner_id), username="joinerbot", game_code="K7K2M9", role="bot")
    assert own_exc.value.code == "CANNOT_JOIN_OWN_GAME"

    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "L7K2M9",
            "rule_variant": "berkeley_any",
            "creator_color": "white",
            "opponent_type": "human",
            "white": {"user_id": str(creator_id), "username": "creatorbot", "connected": True, "role": "bot"},
            "black": None,
            "state": "waiting",
            "turn": None,
            "move_number": 1,
            "created_at": now,
            "updated_at": now,
        }
    )

    with pytest.raises(GameConflictError) as cooldown_exc:
        await service.join_game(user_id=str(joiner_id), username="joinerbot", game_code="L7K2M9", role="bot")
    assert cooldown_exc.value.code == "BOT_JOIN_COOLDOWN"

    service.utcnow = lambda: now + timedelta(minutes=1, seconds=1)  # type: ignore[method-assign]
    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "M7K2M9",
            "rule_variant": "berkeley_any",
            "creator_color": "white",
            "opponent_type": "human",
            "white": {"user_id": str(creator_id), "username": "creatorbot", "connected": True, "role": "bot"},
            "black": None,
            "state": "waiting",
            "turn": None,
            "move_number": 1,
            "created_at": now,
            "updated_at": now,
        }
    )
    joined_again = await service.join_game(user_id=str(joiner_id), username="joinerbot", game_code="M7K2M9", role="bot")
    assert joined_again.state == "active"


@pytest.mark.asyncio
async def test_bot_join_cooldown_accepts_naive_stored_datetime() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    creator_id = ObjectId()
    joiner_id = ObjectId()
    now = datetime.now(UTC)
    users.docs.extend(
        [
            {
                "_id": creator_id,
                "username": "creatorbot",
                "role": "bot",
                "status": "active",
                "bot_profile": {"display_name": "Creator Bot", "description": "Creates"},
            },
            {
                "_id": joiner_id,
                "username": "joinerbot",
                "role": "bot",
                "status": "active",
                "bot_profile": {
                    "display_name": "Joiner Bot",
                    "description": "Joins",
                    "last_bot_game_joined_at": now.replace(tzinfo=None),
                },
            },
        ]
    )
    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "N7K2M9",
            "rule_variant": "berkeley_any",
            "creator_color": "white",
            "opponent_type": "human",
            "white": {"user_id": str(creator_id), "username": "creatorbot", "connected": True, "role": "bot"},
            "black": None,
            "state": "waiting",
            "turn": None,
            "move_number": 1,
            "created_at": now,
            "updated_at": now,
        }
    )
    service = GameService(games, users_collection=users)
    service.utcnow = lambda: now  # type: ignore[method-assign]

    with pytest.raises(GameConflictError) as exc:
        await service.join_game(user_id=str(joiner_id), username="joinerbot", game_code="N7K2M9", role="bot")

    assert exc.value.code == "BOT_JOIN_COOLDOWN"


@pytest.mark.asyncio
async def test_user_service_can_issue_and_authenticate_bot_token() -> None:
    users = FakeUsersCollection()
    service = UserService(users)
    user, token = await service.create_bot(
        type(
            "Payload",
            (),
            {
                "username": "randobot",
                "display_name": "Random Bot",
                "owner_email": "Owner@Example.com",
                "description": "Plays random moves",
            },
        )()
    )

    assert user.role == "bot"
    assert user.bot_profile is not None
    assert user.bot_profile.owner_email == "owner@example.com"
    assert user.bot_profile.supported_rule_variants == [
        "berkeley",
        "berkeley_any",
        "cincinnati",
        "wild16",
        "rand",
        "english",
        "crazykrieg",
    ]
    authenticated = await service.authenticate_bot_token(token)
    assert authenticated is not None
    assert authenticated.username == "randobot"


@pytest.mark.asyncio
async def test_bot_service_lists_active_bots() -> None:
    users = FakeUsersCollection()
    users.docs.extend(
        [
            {
                "_id": ObjectId(),
                "username": "randobot",
                "username_display": "Random Bot",
                "role": "bot",
                "status": "active",
                "bot_profile": {
                    "display_name": "Random Bot",
                    "owner_email": "owner@example.com",
                    "description": "Plays random moves",
                    "listed": True,
                    "supported_rule_variants": ["berkeley", "berkeley_any"],
                },
            },
            {
                "_id": ObjectId(),
                "username": "sleepybot",
                "username_display": "Sleepy Bot",
                "role": "bot",
                "status": "inactive",
                "bot_profile": {
                    "display_name": "Sleepy Bot",
                    "owner_email": "owner@example.com",
                    "description": "Offline",
                    "listed": True,
                },
            },
        ]
    )
    service = BotService(users)

    listed = await service.list_bots()
    assert [bot.username for bot in listed.bots] == ["randobot"]
    assert listed.bots[0].elo == 1200
    assert listed.bots[0].supported_rule_variants == ["berkeley", "berkeley_any"]


@pytest.mark.asyncio
async def test_bot_service_hides_catalog_suppressed_bots_but_keeps_direct_lookup() -> None:
    now = datetime(2026, 7, 11, tzinfo=UTC)
    users = FakeUsersCollection()
    hidden_bot_specs = [
        ("llm_gemma3_4b", "LLM Gemma 3 4B (bot)", "Gemma 3 4B model bot"),
        ("llm_gemma3_27b", "LLM Gemma 3 27B (bot)", "Gemma 3 27B model bot"),
        ("llm_llama31_8b", "LLM Llama 3.1 8B (bot)", "Llama 3.1 8B model bot"),
        ("llm_llama4_scout", "LLM Llama 4 Scout (bot)", "Llama 4 Scout model bot"),
        ("llm_mistral_nemo", "LLM Mistral Nemo (bot)", "Mistral Nemo model bot"),
        ("openrouter_llama31_8b", "OpenRouter Llama 3.1 8B (bot)", "Legacy Llama 3.1 8B model bot"),
    ]
    hidden_bot_ids = {username: ObjectId() for username, _, _ in hidden_bot_specs}
    users.docs.extend(
        [
            *[
                {
                    "_id": hidden_bot_ids[username],
                    "username": username,
                    "username_display": display_name,
                    "role": "bot",
                    "status": "active",
                    "bot_profile": {
                        "display_name": display_name,
                        "description": description,
                        "listed": True,
                        "model_availability": {"provider": "openai", "ready": True, "reason": "ok", "checked_at": now},
                    },
                }
                for username, display_name, description in hidden_bot_specs
            ],
            {
                "_id": ObjectId(),
                "username": "llm_mistral_small32",
                "username_display": "LLM Mistral Small 3.2 (bot)",
                "role": "bot",
                "status": "active",
                "bot_profile": {
                    "display_name": "LLM Mistral Small 3.2 (bot)",
                    "description": "Mistral Small 3.2 model bot",
                    "listed": True,
                    "model_availability": {"provider": "openai", "ready": True, "reason": "ok", "checked_at": now},
                },
            },
        ]
    )
    service = BotService(users, now_factory=lambda: now)

    listed = await service.list_bots(viewer_role="user", viewer_llm_bot_tier="tier2")
    profile_listed = await service.list_bots(
        viewer_role="user",
        viewer_llm_bot_tier="tier2",
        profile_username=" llm_gemma3_27b ",
    )
    direct = await service.get_bot_by_id(str(hidden_bot_ids["llm_gemma3_27b"]))

    assert [bot.username for bot in listed.bots] == ["llm_mistral_small32"]
    assert [bot.username for bot in profile_listed.bots] == ["llm_gemma3_27b", "llm_mistral_small32"]
    assert direct is not None
    assert direct["username"] == "llm_gemma3_27b"
    assert BotService.bot_can_start_games(direct, now=now) is True


@pytest.mark.asyncio
async def test_bot_service_hides_model_bots_without_fresh_ready_status() -> None:
    now = datetime(2026, 5, 13, 12, tzinfo=UTC)
    users = FakeUsersCollection()
    users.docs.extend(
        [
            {
                "_id": ObjectId(),
                "username": "llm_gptnano",
                "username_display": "LLM GPT-4.5 Nano (bot)",
                "role": "bot",
                "status": "active",
                "bot_profile": {
                    "display_name": "LLM GPT-4.5 Nano (bot)",
                    "description": "OpenAI bot",
                    "listed": True,
                    "model_availability": {
                        "provider": "openai",
                        "ready": True,
                        "reason": "ok",
                        "checked_at": now - timedelta(seconds=30),
                    },
                },
            },
            {
                "_id": ObjectId(),
                "username": "llm_haiku",
                "username_display": "LLM Haiku (bot)",
                "role": "bot",
                "status": "active",
                "bot_profile": {
                    "display_name": "LLM Haiku (bot)",
                    "description": "Anthropic bot",
                    "listed": True,
                    "model_availability": {
                        "provider": "anthropic",
                        "ready": True,
                        "reason": "ok",
                        "checked_at": now - timedelta(minutes=5),
                    },
                },
            },
            {
                "_id": ObjectId(),
                "username": "randobot",
                "username_display": "Random Bot",
                "role": "bot",
                "status": "active",
                "bot_profile": {"display_name": "Random Bot", "description": "ready", "listed": True},
            },
        ]
    )
    service = BotService(users, now_factory=lambda: now)

    listed = await service.list_bots()

    assert [bot.username for bot in listed.bots] == ["llm_gptnano", "randobot"]


@pytest.mark.asyncio
async def test_bot_service_records_model_availability_for_authenticated_bot() -> None:
    now = datetime(2026, 5, 13, 12, tzinfo=UTC)
    users = FakeUsersCollection()
    bot_id = ObjectId()
    users.docs.append(
        {
            "_id": bot_id,
            "username": "llm_gptnano",
            "username_display": "LLM GPT-4.5 Nano (bot)",
            "role": "bot",
            "status": "active",
            "bot_profile": {"display_name": "LLM GPT-4.5 Nano (bot)", "description": "OpenAI bot", "listed": True},
        }
    )
    service = BotService(users, now_factory=lambda: now)

    updated = await service.report_model_availability(
        user_id=str(bot_id),
        provider="openai",
        ready=False,
        reason="http_429: insufficient_quota",
    )

    assert updated is users.docs[0]
    assert users.docs[0]["bot_profile"]["model_availability"] == {
        "provider": "openai",
        "ready": False,
        "reason": "http_429: insufficient_quota",
        "checked_at": now,
    }


@pytest.mark.asyncio
async def test_bot_service_syncs_supported_rule_variants_for_authenticated_bot() -> None:
    now = datetime(2026, 5, 18, 22, tzinfo=UTC)
    users = FakeUsersCollection()
    bot_id = ObjectId()
    users.docs.append(
        {
            "_id": bot_id,
            "username": "simpleheuristics",
            "username_display": "Simple Heuristics",
            "role": "bot",
            "status": "active",
            "bot_profile": {
                "display_name": "Simple Heuristics",
                "description": "Heuristic bot",
                "supported_rule_variants": ["berkeley", "berkeley_any"],
            },
        }
    )
    service = BotService(users, now_factory=lambda: now)

    updated = await service.sync_supported_rule_variants(
        user_id=str(bot_id),
        supported_rule_variants=["berkeley", "berkeley_any", "wild16"],
    )

    assert updated is users.docs[0]
    assert users.docs[0]["bot_profile"]["supported_rule_variants"] == ["berkeley", "berkeley_any", "wild16"]
    assert users.docs[0]["updated_at"] == now


@pytest.mark.asyncio
async def test_bot_service_syncs_username_display_and_references_for_authenticated_bot() -> None:
    now = datetime(2026, 7, 6, 2, tzinfo=UTC)
    users = FakeUsersCollection()
    bot_id = ObjectId()
    users.docs.append(
        {
            "_id": bot_id,
            "username": "llm_gpt45nano",
            "username_display": "LLM GPT-4.5 Nano (bot)",
            "role": "bot",
            "status": "active",
            "profile": {"bio": "Old profile"},
            "bot_profile": {
                "display_name": "LLM GPT-4.5 Nano (bot)",
                "description": "Old profile",
                "supported_rule_variants": ["berkeley", "berkeley_any"],
            },
        }
    )
    games = FakeReferenceCollection(
        [
            {
                "white": {"user_id": str(bot_id), "username": "llm_gpt45nano", "role": "bot"},
                "black": {"username": "randobot", "role": "bot"},
                "created_by": "llm_gpt45nano",
                "stats": {
                    "llm_usage": {
                        "white": {
                            "user_id": str(bot_id),
                            "username": "llm_gpt45nano",
                            "calls": 1,
                        }
                    }
                },
            }
        ]
    )
    archives = FakeReferenceCollection(
        [
            {
                "white": {"username": "randobot", "role": "bot"},
                "black": {"user_id": str(bot_id), "username": "llm_gpt45nano", "role": "bot"},
                "created_by": "randobot",
                "stats": {
                    "llm_usage": {
                        "black": {
                            "user_id": str(bot_id),
                            "username": "llm_gpt45nano",
                            "calls": 1,
                        }
                    }
                },
            }
        ]
    )
    service = BotService(
        users,
        game_collections=(games, archives),
        now_factory=lambda: now,
    )

    updated = await service.sync_supported_rule_variants(
        user_id=str(bot_id),
        username="llm_gptnano",
        display_name="LLM GPT-4.5 Nano (bot)",
        description="LLM GPT-4.5 Nano (bot) Kriegspiel model bot.",
        supported_rule_variants=["berkeley", "berkeley_any", "wild16"],
    )

    assert updated is users.docs[0]
    assert users.docs[0]["username"] == "llm_gptnano"
    assert users.docs[0]["username_display"] == "LLM GPT-4.5 Nano (bot)"
    assert users.docs[0]["bot_profile"]["display_name"] == "LLM GPT-4.5 Nano (bot)"
    assert users.docs[0]["bot_profile"]["description"] == "LLM GPT-4.5 Nano (bot) Kriegspiel model bot."
    assert users.docs[0]["profile"]["bio"] == "LLM GPT-4.5 Nano (bot) Kriegspiel model bot."
    assert users.docs[0]["bot_profile"]["supported_rule_variants"] == ["berkeley", "berkeley_any", "wild16"]
    assert games.docs[0]["white"]["username"] == "llm_gptnano"
    assert games.docs[0]["created_by"] == "llm_gptnano"
    assert games.docs[0]["stats"]["llm_usage"]["white"]["username"] == "llm_gptnano"
    assert archives.docs[0]["black"]["username"] == "llm_gptnano"
    assert archives.docs[0]["stats"]["llm_usage"]["black"]["username"] == "llm_gptnano"


@pytest.mark.asyncio
async def test_bot_service_hides_unlisted_bots() -> None:
    users = FakeUsersCollection()
    users.docs.extend(
        [
            {
                "_id": ObjectId(),
                "username": "randobot",
                "username_display": "Random Bot",
                "role": "bot",
                "status": "active",
                "bot_profile": {
                    "display_name": "Random Bot",
                    "owner_email": "owner@example.com",
                    "description": "Plays random moves",
                    "listed": True,
                },
            },
            {
                "_id": ObjectId(),
                "username": "randobot_e2eabcd",
                "username_display": "Random Bot e2eabcd",
                "role": "bot",
                "status": "active",
                "bot_profile": {
                    "display_name": "Random Bot e2eabcd",
                    "owner_email": "owner@example.com",
                    "description": "E2E random bot",
                    "listed": False,
                },
            },
        ]
    )
    service = BotService(users)

    listed = await service.list_bots()
    assert [bot.username for bot in listed.bots] == ["randobot"]


@pytest.mark.asyncio
async def test_bot_registration_marks_e2e_bots_unlisted() -> None:
    users = FakeUsersCollection()
    service = UserService(users)

    user, _token = await service.create_bot(
        type(
            "Payload",
            (),
            {
                "username": "randobot_e2eabcd",
                "display_name": "Random Bot e2eabcd",
                "owner_email": "owner@example.com",
                "description": "E2E random bot",
                "listed": None,
            },
        )()
    )

    assert user.bot_profile is not None
    assert user.bot_profile.listed is False


def test_bot_registration_route_returns_api_token() -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    users = FakeUsersCollection()

    from app.routers import auth as auth_router_module

    auth_router_module.require_db = lambda: type("Db", (), {"users": users})()

    with TestClient(app) as client:
        registered = client.post(
            "/api/auth/bots/register",
            json={
                "username": "randobot",
                "display_name": "Random Bot",
                "owner_email": "owner@example.com",
                "description": "Plays random moves",
            },
        )

    assert registered.status_code == 201
    assert registered.json()["api_token"].startswith("ksbot_")
    assert registered.json()["owner_email"] == "owner@example.com"


def test_bot_registration_route_requires_owner_email() -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    users = FakeUsersCollection()

    from app.routers import auth as auth_router_module

    auth_router_module.require_db = lambda: type("Db", (), {"users": users})()

    with TestClient(app) as client:
        registered = client.post(
            "/api/auth/bots/register",
            json={"username": "randobot", "display_name": "Random Bot", "description": "Plays random moves"},
        )

    assert registered.status_code == 422


@pytest.mark.asyncio
async def test_user_service_authenticates_legacy_bot_without_owner_email() -> None:
    users = FakeUsersCollection()
    service = UserService(users)
    _user, token = await service.create_bot(
        type(
            "Payload",
            (),
            {
                "username": "legacybot",
                "display_name": "Legacy Bot",
                "owner_email": "owner@example.com",
                "description": "Legacy registration",
            },
        )()
    )
    users.docs[0]["bot_profile"].pop("owner_email", None)

    authenticated = await service.authenticate_bot_token(token)

    assert authenticated is not None
    assert authenticated.username == "legacybot"
    assert authenticated.bot_profile is not None
    assert authenticated.bot_profile.owner_email == "bots@kriegspiel.org"


@pytest.mark.asyncio
async def test_probe_bots_default_to_unlisted() -> None:
    users = FakeUsersCollection()
    service = UserService(users)

    user, _token = await service.create_bot(
        type(
            "Payload",
            (),
            {
                "username": "probebot",
                "display_name": "Probe Bot",
                "owner_email": "owner@example.com",
                "description": "Probe runner",
                "listed": None,
            },
        )()
    )

    assert user.bot_profile is not None
    assert user.bot_profile.listed is False


@pytest.mark.asyncio
async def test_random_any_defaults_to_berkeley_any_only() -> None:
    users = FakeUsersCollection()
    service = UserService(users)

    user, _token = await service.create_bot(
        type(
            "Payload",
            (),
            {
                "username": "randobotany",
                "display_name": "Random Any Bot",
                "owner_email": "owner@example.com",
                "description": "Asks any first",
                "listed": True,
                "supported_rule_variants": None,
            },
        )()
    )

    assert user.bot_profile is not None
    assert user.bot_profile.supported_rule_variants == ["berkeley_any"]


@pytest.mark.asyncio
async def test_bot_service_get_bot_by_id_rejects_invalid_and_filters_inactive() -> None:
    users = FakeUsersCollection()
    active_id = ObjectId()
    inactive_id = ObjectId()
    users.docs.extend(
        [
            {
                "_id": active_id,
                "username": "randobot",
                "username_display": "Random Bot",
                "role": "bot",
                "status": "active",
                "bot_profile": {"display_name": "Random Bot", "description": "ready"},
            },
            {
                "_id": inactive_id,
                "username": "sleepybot",
                "username_display": "Sleepy Bot",
                "role": "bot",
                "status": "inactive",
                "bot_profile": {"display_name": "Sleepy Bot", "description": "offline"},
            },
        ]
    )
    service = BotService(users)

    assert await service.get_bot_by_id("not-an-object-id") is None
    assert await service.get_bot_by_id(str(inactive_id)) is None
    assert (await service.get_bot_by_id(str(active_id)))["username"] == "randobot"


@pytest.mark.asyncio
async def test_create_game_rejects_unavailable_model_bot() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    bot_id = ObjectId()
    now = datetime(2026, 5, 13, 12, tzinfo=UTC)
    users.docs.append(
        {
            "_id": bot_id,
            "username": "llm_haiku",
            "username_display": "LLM Haiku (bot)",
            "role": "bot",
            "status": "active",
            "bot_profile": {
                "display_name": "LLM Haiku (bot)",
                "supported_rule_variants": ["berkeley_any"],
                "model_availability": {
                    "provider": "anthropic",
                    "ready": False,
                    "reason": "quota",
                    "checked_at": now,
                },
            },
        }
    )
    service = GameService(games, users_collection=users)
    service.utcnow = lambda: now  # type: ignore[method-assign]

    with pytest.raises(GameValidationError) as exc:
        await service.create_game(
            user_id="u1",
            username="fil",
            request=CreateGameRequest(
                opponent_type="bot",
                bot_id=str(bot_id),
                rule_variant="berkeley_any",
                play_as="white",
                time_control="rapid",
            ),
        )

    assert exc.value.code == "BOT_UNAVAILABLE"


def test_bot_service_supported_rule_variants_fallbacks_cover_randobotany() -> None:
    assert BotService._supported_rule_variants({"username": "randobotany", "bot_profile": {}}) == ["berkeley_any"]
    assert BotService._supported_rule_variants({"username": "randobot", "bot_profile": {}}) == [
        "berkeley",
        "berkeley_any",
        "cincinnati",
        "wild16",
        "rand",
        "english",
        "crazykrieg",
    ]
    assert BotService._supported_rule_variants({"username": "llm_gptnano", "bot_profile": {}}) == [
        "berkeley",
        "berkeley_any",
    ]
    assert BotService._supported_rule_variants({"username": "simpleheuristics", "bot_profile": {}}) == [
        "berkeley",
        "berkeley_any",
    ]
    assert BotService._supported_rule_variants(
        {
            "username": "simpleheuristics",
            "bot_profile": {"supported_rule_variants": ["berkeley", "berkeley_any", "wild16"]},
        }
    ) == [
        "berkeley",
        "berkeley_any",
        "wild16",
    ]
    assert BotService._supported_rule_variants(
        {"username": "custombot", "bot_profile": {"supported_rule_variants": ["crazykrieg"]}}
    ) == ["crazykrieg"]
    assert BotService._supported_rule_variants(
        {"username": "custombot", "bot_profile": {"supported_rule_variants": ["unknown"]}}
    ) == [
        "berkeley",
        "berkeley_any",
    ]


def test_bot_router_uses_db_users_and_lists_bots() -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    users = FakeUsersCollection()
    users.docs.append(
        {
            "_id": ObjectId(),
            "username": "randobot",
            "username_display": "Random Bot",
            "role": "bot",
            "status": "active",
            "bot_profile": {
                "display_name": "Random Bot",
                "description": "ready",
                "listed": True,
                "supported_rule_variants": ["berkeley", "berkeley_any"],
            },
        }
    )

    from app.routers import bot as bot_router_module

    bot_router_module.get_db = lambda: type("Db", (), {"users": users})()
    service = bot_router_module.get_bot_service()
    assert service._users is users  # noqa: SLF001

    app.dependency_overrides[bot_router_module.get_current_user] = lambda: type("User", (), {"id": "u1"})()
    with TestClient(app) as client:
        response = client.get("/api/bots")

    assert response.status_code == 200
    assert response.json()["bots"][0]["username"] == "randobot"


def test_bot_router_records_model_availability_for_authenticated_bot() -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    users = FakeUsersCollection()
    bot_id = ObjectId()
    users.docs.append(
        {
            "_id": bot_id,
            "username": "llm_gptnano",
            "username_display": "LLM GPT-4.5 Nano (bot)",
            "role": "bot",
            "status": "active",
            "bot_profile": {"display_name": "LLM GPT-4.5 Nano (bot)", "description": "OpenAI bot", "listed": True},
        }
    )

    from app.routers import bot as bot_router_module

    bot_router_module.get_db = lambda: type("Db", (), {"users": users})()
    app.dependency_overrides[bot_router_module.get_current_user] = lambda: type(
        "User", (), {"id": str(bot_id), "role": "bot"}
    )()

    with TestClient(app) as client:
        response = client.post(
            "/api/bots/availability",
            json={"provider": "openai", "ready": False, "reason": "http_429: insufficient_quota"},
        )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    assert users.docs[0]["bot_profile"]["model_availability"]["provider"] == "openai"
    assert users.docs[0]["bot_profile"]["model_availability"]["ready"] is False


def test_bot_router_syncs_supported_rule_variants_for_authenticated_bot() -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    users = FakeUsersCollection()
    bot_id = ObjectId()
    users.docs.append(
        {
            "_id": bot_id,
            "username": "simpleheuristics",
            "username_display": "Simple Heuristics",
            "role": "bot",
            "status": "active",
            "bot_profile": {
                "display_name": "Simple Heuristics",
                "description": "Heuristic bot",
                "listed": True,
                "supported_rule_variants": ["berkeley", "berkeley_any"],
            },
        }
    )

    from app.routers import bot as bot_router_module

    bot_router_module.get_db = lambda: type("Db", (), {"users": users})()
    app.dependency_overrides[bot_router_module.get_current_user] = lambda: type(
        "User", (), {"id": str(bot_id), "role": "bot"}
    )()

    with TestClient(app) as client:
        response = client.post(
            "/api/bots/profile",
            json={"supported_rule_variants": ["berkeley", "berkeley_any", "wild16", "wild16"]},
        )

    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "username": "simpleheuristics",
        "display_name": "Simple Heuristics",
        "description": "Heuristic bot",
        "supported_rule_variants": ["berkeley", "berkeley_any", "wild16"],
    }
    assert users.docs[0]["bot_profile"]["supported_rule_variants"] == ["berkeley", "berkeley_any", "wild16"]


def test_bot_router_profile_sync_rejects_non_bot_user() -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    users = FakeUsersCollection()

    from app.routers import bot as bot_router_module

    bot_router_module.get_db = lambda: type("Db", (), {"users": users})()
    app.dependency_overrides[bot_router_module.get_current_user] = lambda: type("User", (), {"id": "user-1", "role": "user"})()

    with TestClient(app) as client:
        response = client.post("/api/bots/profile", json={"supported_rule_variants": ["wild16"]})

    assert response.status_code == 403
