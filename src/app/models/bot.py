from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

SupportedRuleVariant = str
DEFAULT_SUPPORTED_RULE_VARIANTS = ["berkeley", "berkeley_any", "cincinnati", "wild16", "rand", "english", "crazykrieg"]


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
