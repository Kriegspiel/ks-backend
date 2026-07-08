from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.models.auth import BotRegisterRequest, BotRegisterResponse, RegisterRequest
from app.models.bot import BotProfileSyncRequest
from app.models.user import UserModel


def test_register_request_rejects_invalid_email_format() -> None:
    with pytest.raises(ValueError, match="Invalid email format"):
        RegisterRequest(username="playerone", email="invalid-email", password="secret")


def test_bot_register_request_validates_supported_rule_variants_and_deduplicates() -> None:
    payload = BotRegisterRequest(
        username="randobot",
        display_name="Random Bot",
        owner_email="owner@example.com",
        description="bot",
        supported_rule_variants=["berkeley", "berkeley", "berkeley_any"],
    )
    assert payload.supported_rule_variants == ["berkeley", "berkeley_any"]

    extended = BotRegisterRequest(
        username="randobot",
        display_name="Random Bot",
        owner_email="owner@example.com",
        description="bot",
        supported_rule_variants=["cincinnati", "wild16", "cincinnati"],
    )
    assert extended.supported_rule_variants == ["cincinnati", "wild16"]

    remaining = BotRegisterRequest(
        username="randobot",
        display_name="Random Bot",
        owner_email="owner@example.com",
        description="bot",
        supported_rule_variants=["rand", "english", "crazykrieg"],
    )
    assert remaining.supported_rule_variants == ["rand", "english", "crazykrieg"]

    with pytest.raises(ValueError, match="Unsupported rule variant"):
        BotRegisterRequest(
            username="randobot",
            display_name="Random Bot",
            owner_email="owner@example.com",
            description="bot",
            supported_rule_variants=["standard"],
        )

    with pytest.raises(ValueError, match="At least one supported rule variant is required"):
        BotRegisterRequest(
            username="randobot",
            display_name="Random Bot",
            owner_email="owner@example.com",
            description="bot",
            supported_rule_variants=[],
        )


def test_bot_register_response_uses_default_message() -> None:
    response = BotRegisterResponse(
        bot_id="507f1f77bcf86cd799439011",
        username="randobot",
        display_name="Random Bot",
        owner_email="owner@example.com",
        api_token="ksbot_token.secret",
    )

    assert "Save this token now" in response.message


def test_bot_register_request_allows_supported_rule_variants_to_be_omitted() -> None:
    payload = BotRegisterRequest(
        username="randobot",
        display_name="Random Bot",
        owner_email="owner@example.com",
        description="bot",
        supported_rule_variants=None,
    )

    assert payload.supported_rule_variants is None


def test_user_model_accepts_disabled_bot_profile_metadata() -> None:
    now = datetime(2026, 7, 8, tzinfo=UTC)
    user = UserModel.from_mongo(
        {
            "_id": "507f1f77bcf86cd799439011",
            "username": "llm_llama31_8b",
            "username_display": "LLM Llama 3.1 8B (bot)",
            "email": "llm_llama31_8b@kriegspiel.org",
            "password_hash": "",
            "role": "bot",
            "status": "active",
            "last_active_at": now,
            "created_at": now,
            "updated_at": now,
            "bot_profile": {
                "display_name": "LLM Llama 3.1 8B (bot)",
                "listed": False,
                "api_token_id": "token-id",
                "api_token_digest": "token-digest",
                "registered_at": now,
                "disabled_at": now,
                "disabled_reason": "disabled by ks-deploy bot-instance-disable",
            },
        }
    )

    assert user.bot_profile is not None
    assert user.bot_profile.disabled_at == now
    assert user.bot_profile.disabled_reason == "disabled by ks-deploy bot-instance-disable"


def test_bot_profile_sync_request_validates_supported_rule_variants() -> None:
    payload = BotProfileSyncRequest(
        username="llm_gptnano",
        display_name="LLM GPT-4.5 Nano (bot)",
        description="LLM GPT-4.5 Nano (bot) Kriegspiel model bot.",
        supported_rule_variants=["wild16", "berkeley_any", "wild16"],
    )
    assert payload.username == "llm_gptnano"
    assert payload.display_name == "LLM GPT-4.5 Nano (bot)"
    assert payload.description == "LLM GPT-4.5 Nano (bot) Kriegspiel model bot."
    assert payload.supported_rule_variants == ["wild16", "berkeley_any"]

    with pytest.raises(ValueError, match="At least one supported rule variant is required"):
        BotProfileSyncRequest.validate_supported_rule_variants(None)

    with pytest.raises(ValueError, match="Unsupported rule variant"):
        BotProfileSyncRequest(supported_rule_variants=["standard"])

    with pytest.raises(ValueError, match="At least one supported rule variant is required"):
        BotProfileSyncRequest(supported_rule_variants=[])
