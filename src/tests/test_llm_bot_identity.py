from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from bson import ObjectId
from fastapi.testclient import TestClient
from pydantic import ValidationError

import app.dependencies as dependencies
from app.config import Settings
from app.llm_bot_policy import KNOWN_LLM_BOT_USERNAMES, bot_required_tier_for_username
from app.main import create_app
from app.models.auth import BotRegisterRequest, ConvertGuestRequest, RegisterRequest
from app.models.bot import BotAvailabilityReportRequest, BotProfile
from app.models.user import default_user_stats_payload
from app.routers.user import get_user_service
from app.services.bot_service import BotProfileConflictError, BotService
from app.services.game_usage_stats import usage_color_for_game
from app.services.user_service import UserConflictError, UserService
from tests.test_game_usage_stats import _report
from tests.test_user_service import FakeDB, FakeUsersCollection


RENAMED_BOTS = (
    ("llm_gpt56_luna", "llm_gpt_luna", "tier2", "openai", "gpt-6-luna"),
    ("llm_sonnet5", "llm_sonnet", "tier3", "anthropic", "claude-sonnet-5-5"),
    ("llm_gemini35_flash", "llm_gemini_flash", "tier3", "openai", "google/gemini-3.8-flash"),
    ("llm_qwen36_flash", "llm_qwen_flash", "tier3", "openai", "qwen/qwen3.8-flash"),
    ("llm_opus48", "llm_opus", "tier4", "anthropic", "claude-opus-5-5"),
    ("llm_gpt56_sol", "llm_gpt_sol", "tier4", "openai", "gpt-6.1-sol"),
    ("llm_grok45", "llm_grok", "tier5", "openrouter", "x-ai/grok-4.7"),
)


@pytest.mark.parametrize("legacy,canonical,tier,provider,model", RENAMED_BOTS)
def test_renamed_bots_keep_tier_and_provider_readiness(legacy, canonical, tier, provider, model) -> None:
    now = datetime.now(UTC)
    for username in (legacy, canonical):
        assert username in KNOWN_LLM_BOT_USERNAMES
        assert bot_required_tier_for_username(username) == tier
        assert BotService.model_availability_required_provider({"username": username}) == provider
        assert BotService.bot_can_start_games({"username": username}, now=now) is False
        doc = {
            "username": username,
            "bot_profile": {"model_availability": {"provider": provider, "ready": True, "checked_at": now}},
        }
        assert BotService.bot_can_start_games(doc, now=now) is True
        doc["bot_profile"]["model_availability"]["provider"] = "wrong-provider"
        assert BotService.bot_can_start_games(doc, now=now) is False


@pytest.mark.parametrize("legacy,canonical,tier,provider,model", RENAMED_BOTS)
@pytest.mark.asyncio
async def test_public_alias_resolves_same_bot_before_and_after_rename(legacy, canonical, tier, provider, model) -> None:
    users = FakeUsersCollection()
    bot_id = ObjectId()
    users.docs.append({"_id": bot_id, "username": legacy, "role": "bot", "status": "inactive"})
    db = FakeDB(users=users, game_archives=FakeUsersCollection())
    assert (await UserService.get_public_user_document(db, legacy.upper()))["_id"] == bot_id
    users.docs[0]["username"] = canonical
    resolved = await UserService.get_public_user_document(db, legacy)
    assert resolved["_id"] == bot_id
    assert resolved["username"] == canonical
    assert resolved["status"] == "inactive"
    users.docs[0]["role"] = "user"
    assert await UserService.get_public_user_document(db, legacy) is None


@pytest.mark.parametrize("legacy,canonical,tier,provider,model", RENAMED_BOTS)
def test_usage_reports_match_legacy_and_canonical_players_without_ids(legacy, canonical, tier, provider, model) -> None:
    for player_name in (legacy, canonical):
        game = {"black": {"username": player_name}}
        for report_name in (legacy, canonical, "openrouterbot"):
            report = _report(bot_user_id="", bot_username=report_name, model=model, provider=provider)
            assert usage_color_for_game(game, report) == "black"


@pytest.mark.parametrize("legacy,canonical,tier,provider,model", RENAMED_BOTS)
@pytest.mark.asyncio
async def test_legacy_names_cannot_be_claimed_by_new_users_or_bots(legacy, canonical, tier, provider, model) -> None:
    users = FakeUsersCollection()
    service = UserService(users)
    with pytest.raises(UserConflictError, match="reserved") as user_error:
        await service.create_user(RegisterRequest(username=legacy.upper(), email="new@example.com", password="abc12345"))
    assert user_error.value.code == "USERNAME_TAKEN"
    with pytest.raises(UserConflictError, match="reserved"):
        await service.create_bot(BotRegisterRequest(username=legacy, display_name="New Bot", owner_email="bot@example.com"))
    assert users.docs == []


@pytest.mark.asyncio
async def test_guest_conversion_cannot_claim_legacy_profile_alias() -> None:
    users = FakeUsersCollection()
    service = UserService(users)
    guest = (await service.create_guest_user()).model_copy(update={"username": "guest_llm_gpt56_luna"})
    with pytest.raises(UserConflictError, match="reserved"):
        await service.convert_guest_to_user(
            FakeDB(users=users, game_archives=FakeUsersCollection()),
            guest,
            ConvertGuestRequest(email="converted@example.com", password="abc12345"),
        )
    assert users.docs[0]["role"] == "guest"


@pytest.mark.asyncio
async def test_bot_profile_refresh_preserves_reasoning_and_rejects_alias_claim() -> None:
    users = FakeUsersCollection()
    bot_id = ObjectId()
    users.docs.append(
        {
            "_id": bot_id,
            "username": "llm_gpt56_sol",
            "role": "bot",
            "status": "active",
            "bot_profile": {"llm_reasoning_level": "xhigh"},
        }
    )
    service = BotService(users)
    await service.sync_supported_rule_variants(user_id=str(bot_id), supported_rule_variants=["wild16"])
    assert users.docs[0]["bot_profile"]["llm_reasoning_level"] == "xhigh"
    await service.sync_supported_rule_variants(user_id=str(bot_id), supported_rule_variants=["wild16"], username="llm_gpt_sol")
    with pytest.raises(BotProfileConflictError, match="reserved"):
        await service.sync_supported_rule_variants(
            user_id=str(bot_id), supported_rule_variants=["wild16"], username="llm_gpt56_sol"
        )
    assert users.docs[0]["_id"] == bot_id
    assert users.docs[0]["username"] == "llm_gpt_sol"
    assert users.docs[0]["bot_profile"]["llm_reasoning_level"] == "xhigh"


@pytest.mark.parametrize("level", ["none", "enabled", "low", "medium", "high", "xhigh", "max", None])
@pytest.mark.asyncio
async def test_reasoning_metadata_is_authoritative_in_profiles_and_catalog(level) -> None:
    now = datetime.now(UTC)
    users = FakeUsersCollection()
    users.docs.append(
        {
            "_id": ObjectId(),
            "username": "llm_gpt_sol",
            "role": "bot",
            "status": "active",
            "stats": default_user_stats_payload(),
            "created_at": now,
            "bot_profile": {
                "display_name": "GPT Sol",
                "llm_reasoning_level": level,
                "model_availability": {"provider": "openai", "ready": True, "checked_at": now},
            },
        }
    )
    db = FakeDB(users=users, game_archives=FakeUsersCollection())
    profile = await UserService(users).get_public_profile(db, "llm_gpt56_sol")
    assert profile["username"] == "llm_gpt_sol"
    assert profile["llm_reasoning_level"] == level
    catalog = await BotService(users, now_factory=lambda: now).list_bots(viewer_llm_bot_tier="tier4")
    assert catalog.bots[0].llm_reasoning_level == level
    assert catalog.bots[0].bot_id == str(users.docs[0]["_id"])
    assert catalog.bots[0].required_tier == "tier4"
    assert BotProfile(display_name="GPT Sol", llm_reasoning_level=level).llm_reasoning_level == level


def test_reasoning_metadata_rejects_unknown_level_and_accepts_openrouter() -> None:
    with pytest.raises(ValidationError):
        BotProfile(display_name="GPT Sol", llm_reasoning_level="invented")
    assert BotAvailabilityReportRequest(provider="openrouter", ready=True).provider == "openrouter"


def test_legacy_profile_and_all_history_routes_keep_the_same_identity(monkeypatch) -> None:
    users = FakeUsersCollection()
    now = datetime.now(UTC)
    bot_id = ObjectId()
    users.docs.append(
        {
            "_id": bot_id,
            "username": "llm_gpt_luna",
            "role": "bot",
            "status": "inactive",
            "stats": default_user_stats_payload(),
            "created_at": now,
            "bot_profile": {"display_name": "GPT Luna", "llm_reasoning_level": "high"},
        }
    )
    db = FakeDB(users=users, game_archives=FakeUsersCollection())
    service = UserService(users)
    service.get_game_history = AsyncMock(return_value=([{"game_code": "OLD123"}], 1, {}))
    service.get_game_history_filter_options = AsyncMock(return_value={"opponent": []})
    service.get_rating_history = AsyncMock(return_value={"track": "overall", "series": {}})
    app = create_app(Settings(ENVIRONMENT="testing"))
    app.dependency_overrides[get_user_service] = lambda: service
    monkeypatch.setattr(dependencies, "get_db", lambda: db)
    with TestClient(app) as client:
        profile = client.get("/api/user/llm_gpt56_luna")
        games = client.get("/api/user/llm_gpt56_luna/games")
        options = client.get("/api/user/llm_gpt56_luna/games/filter-options")
        ratings = client.get("/api/user/llm_gpt56_luna/rating-history")
    assert profile.status_code == games.status_code == options.status_code == ratings.status_code == 200
    assert profile.json()["username"] == "llm_gpt_luna"
    assert profile.json()["status"] == "inactive"
    assert games.json()["games"][0]["game_code"] == "OLD123"
    assert service.get_game_history.await_args.args[1] == str(bot_id)
    service.get_game_history_filter_options.assert_awaited_once_with(db, str(bot_id))
    service.get_rating_history.assert_awaited_once_with(db, str(bot_id), track="overall", limit=100)


@pytest.mark.parametrize("username,level", [
    ("llm_gpt_luna", "xhigh"), ("llm_gpt_sol", "xhigh"), ("llm_gpt_astra", "xhigh"),
    ("llm_sonnet", "xhigh"), ("llm_opus", "xhigh"), ("llm_fable", "xhigh"), ("llm_grok", "xhigh"),
    ("llm_gptoss120b", "medium"), ("llm_gemini31_lite", "medium"),
    ("llm_gemini_flash", "medium"), ("llm_gemini31_pro_preview", "medium"),
    ("llm_haiku", "enabled"), ("llm_gemma4_31b", "enabled"), ("llm_llama4_maverick", "none"),
])
@pytest.mark.asyncio
async def test_active_catalog_reasoning_rollout_preserves_identity_stats_and_profile_refresh(username, level) -> None:
    now = datetime.now(UTC)
    users = FakeUsersCollection()
    bot_id = ObjectId()
    stats = default_user_stats_payload()
    stats["elo"] = 1432
    provider = BotService.model_availability_required_provider({"username": username})
    users.docs.append({
        "_id": bot_id, "username": username, "role": "bot", "status": "active",
        "stats": stats, "created_at": now,
        "bot_profile": {
            "display_name": username, "llm_reasoning_level": level,
            "model_availability": {"provider": provider, "ready": True, "checked_at": now},
        },
    })
    service = BotService(users, now_factory=lambda: now)
    await service.sync_supported_rule_variants(user_id=str(bot_id), supported_rule_variants=["wild16"])
    assert users.docs[0]["stats"] == stats
    db = FakeDB(users=users, game_archives=FakeUsersCollection())
    profile = await UserService(users).get_public_profile(db, username)
    catalog = await service.list_bots(viewer_llm_bot_tier="tier6")
    assert profile["llm_reasoning_level"] == level
    assert len(catalog.bots) == 1
    assert catalog.bots[0].llm_reasoning_level == level
    assert catalog.bots[0].bot_id == str(bot_id)
    assert catalog.bots[0].elo == 1432
    assert catalog.bots[0].required_tier == bot_required_tier_for_username(username)
