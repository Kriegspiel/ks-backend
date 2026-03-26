from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

GameState = Literal["waiting", "active", "completed"]
RuleVariant = Literal["berkeley", "berkeley_any"]
PlayerColor = Literal["white", "black"]


class PlayerEmbed(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: str
    username: str
    connected: bool = True


class GameDocument(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    id: str | None = Field(default=None, alias="_id")
    game_code: str = Field(min_length=6, max_length=6, pattern=r"^[2-9A-HJ-KM-NP-Z]{6}$")
    rule_variant: RuleVariant = "berkeley_any"
    creator_color: PlayerColor = "white"
    white: PlayerEmbed
    black: PlayerEmbed | None = None
    state: GameState = "waiting"
    turn: PlayerColor | None = None
    move_number: int = Field(default=1, ge=1)
    created_at: datetime
    updated_at: datetime
    expires_at: datetime | None = None
    engine_state: dict[str, Any] | None = None
    moves: list[dict[str, Any]] = Field(default_factory=list)
    result: dict[str, Any] | None = None

    @classmethod
    def from_mongo(cls, doc: dict[str, Any]) -> "GameDocument":
        payload = dict(doc)
        if "_id" in payload:
            payload["_id"] = str(payload["_id"])
        return cls.model_validate(payload)


class CreateGameRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    rule_variant: RuleVariant = "berkeley_any"
    play_as: Literal["white", "black", "random"] = "random"
    time_control: Literal["rapid"] = "rapid"


class CreateGameResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    game_id: str
    game_code: str = Field(min_length=6, max_length=6, pattern=r"^[2-9A-HJ-KM-NP-Z]{6}$")
    play_as: PlayerColor
    rule_variant: RuleVariant
    state: Literal["waiting"] = "waiting"
    join_url: str


class JoinGameResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    game_id: str
    game_code: str = Field(min_length=6, max_length=6, pattern=r"^[2-9A-HJ-KM-NP-Z]{6}$")
    play_as: PlayerColor
    rule_variant: RuleVariant
    state: Literal["active"] = "active"
    game_url: str


class MoveRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    uci: str = Field(min_length=4, max_length=5)


class MoveResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    move_done: bool
    announcement: str
    special_announcement: str | None = None
    capture_square: str | None = None
    turn: PlayerColor
    game_over: bool


class AskAnyResponse(MoveResponse):
    has_any: bool


class OpenGameItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    game_code: str = Field(min_length=6, max_length=6, pattern=r"^[2-9A-HJ-KM-NP-Z]{6}$")
    rule_variant: RuleVariant
    created_by: str
    created_at: datetime
    available_color: PlayerColor


class OpenGamesResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    games: list[OpenGameItem]


class PublicPlayer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    username: str
    connected: bool


class GameMetadataResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    game_id: str
    game_code: str = Field(min_length=6, max_length=6, pattern=r"^[2-9A-HJ-KM-NP-Z]{6}$")
    rule_variant: RuleVariant
    state: GameState
    white: PublicPlayer
    black: PublicPlayer | None = None
    turn: PlayerColor | None = None
    move_number: int = Field(ge=1)
    created_at: datetime
