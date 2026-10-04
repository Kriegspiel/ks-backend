from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime

import pytest
from bson import ObjectId

from app.llm_bot_policy import is_llm_bot_document
from app.models.auth import BotRegisterRequest
from app.services.bot_service import BotService
from app.services.game_usage_stats import usage_color_for_game
from app.services.user_service import UserService
from tests.test_game_usage_stats import _report
from tests.test_user_service import FakeDB, FakeUsersCollection


@pytest.mark.parametrize(
    "username,display_name,provider,retired_username",
    [("llm_fable", "Claude Fable", "anthropic", "llm_gpt55"), ("llm_gpt_astra", "GPT Astra", "openai", "llm_gpt55_pro")],
)
@pytest.mark.asyncio
async def test_new_t5_accounts_require_provider_readiness_and_keep_distinct_history(
    username, display_name, provider, retired_username
) -> None:
    users = FakeUsersCollection()
    service = UserService(users)
    old_bot, old_token = await service.create_bot(
        BotRegisterRequest(username=retired_username, display_name="Retired GPT", owner_email="bots@example.com")
    )
    users.docs[0]["status"] = "inactive"
    users.docs[0]["stats"]["games_played"] = 12
    old_document = deepcopy(users.docs[0])
    archives = FakeUsersCollection()
    archives.docs.append({"_id": ObjectId(), "game_code": "OLDT51", "white": {"user_id": old_bot.id}})

    bot, token = await service.create_bot(
        BotRegisterRequest(username=username, display_name=display_name, owner_email="bots@example.com")
    )
    users.docs[1]["bot_profile"]["llm_reasoning_level"] = "max"
    assert bot.id != old_bot.id
    assert token != old_token
    assert users.docs[1]["stats"]["games_played"] == 0
    assert is_llm_bot_document(users.docs[1]) is True
    assert BotService.model_availability_required_provider(users.docs[1]) == provider
    authenticated = await service.authenticate_bot_token(token)
    assert authenticated.id == bot.id
    assert authenticated.bot_profile.llm_reasoning_level == "max"

    now = datetime.now(UTC)
    catalog_service = BotService(users, now_factory=lambda: now)
    assert (await catalog_service.list_bots(viewer_llm_bot_tier="tier5")).bots == []
    wrong_provider = "openai" if provider == "anthropic" else "anthropic"
    await catalog_service.report_model_availability(user_id=bot.id, provider=wrong_provider, ready=True, reason="test")
    assert (await catalog_service.list_bots(viewer_llm_bot_tier="tier5")).bots == []
    await catalog_service.report_model_availability(user_id=bot.id, provider=provider, ready=False, reason="not ready")
    assert (await catalog_service.list_bots(viewer_llm_bot_tier="tier5")).bots == []
    await catalog_service.report_model_availability(user_id=bot.id, provider=provider, ready=True, reason="ready")

    lower_tier = (await catalog_service.list_bots(viewer_llm_bot_tier="tier4")).bots
    assert len(lower_tier) == 1
    assert lower_tier[0].required_tier == "tier5"
    assert lower_tier[0].available_for_viewer is False
    included = (await catalog_service.list_bots(viewer_llm_bot_tier="tier5")).bots
    assert len(included) == 1
    assert included[0].username == username
    assert included[0].available_for_viewer is True
    assert included[0].llm_reasoning_level == "max"
    assert included[0].bot_id == bot.id
    profile = await service.get_public_profile(FakeDB(users=users, game_archives=archives), username)
    assert profile["display_name"] == display_name
    assert profile["llm_reasoning_level"] == "max"
    assert profile["user_metrics"]["completed_games"] == 0
    assert users.docs[0] == old_document
    assert archives.docs[0]["white"]["user_id"] == old_bot.id


@pytest.mark.parametrize(
    "username,provider,model,old_username,old_model",
    [
        ("llm_fable", "anthropic", "claude-fable-5-1", "llm_gpt55", "gpt-5.5"),
        ("llm_gpt_astra", "openai", "gpt-6-astra", "llm_gpt55_pro", "gpt-5.5-pro"),
        ("llm_gpt_astra", "openai", "openai/gpt-6-astra", "llm_gpt55_pro", "openai/gpt-5.5-pro"),
    ],
)
def test_new_model_usage_is_attributed_separately_from_retired_models(
    username, provider, model, old_username, old_model
) -> None:
    game = {"white": {"username": old_username}, "black": {"username": username}}
    report = _report(bot_user_id="", bot_username="openrouterbot", provider=provider, model=model)
    assert usage_color_for_game(game, report) == "black"
    old_report = _report(bot_user_id="", bot_username="openrouterbot", provider="openai", model=old_model)
    assert usage_color_for_game(game, old_report) == "white"
