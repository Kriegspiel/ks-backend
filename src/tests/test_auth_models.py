from __future__ import annotations

import pytest

from app.models.auth import BotRegisterRequest, BotRegisterResponse, RegisterRequest


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
