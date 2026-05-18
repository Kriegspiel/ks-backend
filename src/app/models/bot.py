from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

SupportedRuleVariant = str
ALL_SUPPORTED_RULE_VARIANTS = ["berkeley", "berkeley_any", "cincinnati", "wild16", "rand", "english", "crazykrieg"]
SUPPORTED_RULE_VARIANT_VALUES = frozenset(ALL_SUPPORTED_RULE_VARIANTS)
DEFAULT_SUPPORTED_RULE_VARIANTS = ["berkeley", "berkeley_any"]
BOT_SPECIFIC_DEFAULT_RULE_VARIANTS: dict[str, list[SupportedRuleVariant]] = {
    "randobot": ALL_SUPPORTED_RULE_VARIANTS,
    "randobotany": ["berkeley_any"],
}


def normalize_supported_rule_variants(value: list[str] | None) -> list[SupportedRuleVariant] | None:
    if value is None:
        return None

    normalized: list[SupportedRuleVariant] = []
    for item in value:
        rule = item.strip()
        if rule not in SUPPORTED_RULE_VARIANT_VALUES:
            raise ValueError("Unsupported rule variant")
        if rule not in normalized:
            normalized.append(rule)
    if not normalized:
        raise ValueError("At least one supported rule variant is required")
    return normalized


def supported_rule_variants_for_bot(username: str, variants: object = None) -> list[SupportedRuleVariant]:
    normalized_username = username.strip().lower()
    if isinstance(variants, list) and variants:
        filtered = [str(item) for item in variants if str(item) in SUPPORTED_RULE_VARIANT_VALUES]
        if filtered:
            return filtered

    return BOT_SPECIFIC_DEFAULT_RULE_VARIANTS.get(normalized_username, DEFAULT_SUPPORTED_RULE_VARIANTS).copy()


class BotModelAvailability(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: Literal["openai", "anthropic"]
    ready: bool = False
    reason: str = ""
    checked_at: datetime


class BotProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    display_name: str
    owner_email: str = "bots@kriegspiel.org"
    description: str = ""
    listed: bool = True
    api_token_id: str | None = None
    api_token_hash: str | None = None
    api_token_digest: str | None = None
    registered_at: datetime | None = None
    last_bot_game_joined_at: datetime | None = None
    supported_rule_variants: list[SupportedRuleVariant] = Field(default_factory=lambda: DEFAULT_SUPPORTED_RULE_VARIANTS.copy())
    model_availability: BotModelAvailability | None = None


class BotListItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bot_id: str
    username: str
    display_name: str
    description: str = ""
    elo: int = 1200
    ratings: dict[str, dict[str, int]] = Field(default_factory=dict)
    supported_rule_variants: list[SupportedRuleVariant] = Field(default_factory=lambda: DEFAULT_SUPPORTED_RULE_VARIANTS.copy())


class BotListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bots: list[BotListItem]


class BotAvailabilityReportRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    provider: Literal["openai", "anthropic"]
    ready: bool
    reason: str = Field(default="", max_length=500)


class BotAvailabilityReportResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool = True


class BotProfileSyncRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    supported_rule_variants: list[SupportedRuleVariant]

    @field_validator("supported_rule_variants")
    @classmethod
    def validate_supported_rule_variants(cls, value: list[str]) -> list[SupportedRuleVariant]:
        normalized = normalize_supported_rule_variants(value)
        if normalized is None:
            raise ValueError("At least one supported rule variant is required")
        return normalized


class BotProfileSyncResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool = True
    supported_rule_variants: list[SupportedRuleVariant]
