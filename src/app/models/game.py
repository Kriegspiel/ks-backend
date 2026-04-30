from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

GameState = Literal["waiting", "active", "completed"]
RuleVariant = Literal["berkeley", "berkeley_any", "cincinnati", "wild16", "rand", "english", "crazykrieg"]
PlayerColor = Literal["white", "black"]
OpponentType = Literal["human", "bot"]
PieceAnnouncement = Literal["PAWN", "PIECE", "KNIGHT", "BISHOP", "ROOK", "QUEEN"]


class PlayerEmbed(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: str
    username: str
    connected: bool = True
    role: Literal["user", "guest", "bot"] = "user"


class GameDocument(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    id: str | None = Field(default=None, alias="_id")
    game_code: str = Field(min_length=6, max_length=6, pattern=r"^[2-9A-HJ-KM-NP-Z]{6}$")
    rule_variant: RuleVariant = "berkeley_any"
    creator_color: PlayerColor = "white"
    opponent_type: OpponentType = "human"
    selected_bot_id: str | None = None
    white: PlayerEmbed
    black: PlayerEmbed | None = None
    state: GameState = "waiting"
    turn: PlayerColor | None = None
    move_number: int = Field(default=1, ge=1)
    created_at: datetime
    updated_at: datetime
    expires_at: datetime | None = None
    engine_state: dict[str, Any] | None = None
    white_scoresheet: dict[str, Any] | None = None
    black_scoresheet: dict[str, Any] | None = None
    moves: list[dict[str, Any]] = Field(default_factory=list)
    result: dict[str, Any] | None = None
    time_control: dict[str, Any] | None = None
    rating_snapshot: dict[str, Any] | None = None
    stats_recorded_at: datetime | None = None

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
    opponent_type: OpponentType = "human"
    bot_id: str | None = None

    @model_validator(mode="after")
    def validate_bot_fields(self) -> "CreateGameRequest":
        if self.opponent_type == "bot" and not self.bot_id:
            raise ValueError("bot_id is required when opponent_type is bot")
        if self.opponent_type == "human" and self.bot_id is not None:
            raise ValueError("bot_id is only allowed when opponent_type is bot")
        return self


class CreateGameResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    game_id: str
    game_code: str = Field(min_length=6, max_length=6, pattern=r"^[2-9A-HJ-KM-NP-Z]{6}$")
    play_as: PlayerColor
    rule_variant: RuleVariant
    state: Literal["waiting", "active"]
    join_url: str
    game_url: str | None = None
    opponent_type: OpponentType = "human"
    bot: dict[str, str] | None = None


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


class ClockState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    white_remaining: float = Field(ge=0)
    black_remaining: float = Field(ge=0)
    active_color: PlayerColor | None = None


class MoveResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    move_done: bool
    announcement: str
    special_announcement: str | None = None
    capture_square: str | None = None
    captured_piece_announcement: PieceAnnouncement | None = None
    dropped_piece_announcement: PieceAnnouncement | None = None
    promotion_announced: bool | None = None
    next_turn_pawn_tries: int | None = Field(default=None, ge=0)
    next_turn_has_pawn_capture: bool | None = None
    next_turn_pawn_try_squares: list[str] | None = None
    turn: PlayerColor | None = None
    game_over: bool
    clock: ClockState


class AskAnyResponse(MoveResponse):
    has_any: bool


class ReplayFen(BaseModel):
    model_config = ConfigDict(extra="forbid")

    full: str
    white: str
    black: str


class RefereeLogItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ply: int | None = None
    announcement: str
    special_announcement: str | None = None
    capture_square: str | None = None
    timestamp: datetime | None = None
    replay_fen: ReplayFen | None = None


class AnnouncementEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["move", "ask_any", "illegal_move", "capture", "status"]
    actor: Literal["self", "opponent"] | None = None
    prompt: str | None = None
    message: str
    messages: list[str] = Field(default_factory=list)
    move_uci: str | None = None
    question_type: str | None = None


class RefereeTurnEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    turn: int = Field(ge=1)
    white: list[AnnouncementEntry | str] = Field(default_factory=list)
    black: list[AnnouncementEntry | str] = Field(default_factory=list)


class ScoresheetTurn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    turn: int = Field(ge=1)
    white: list[AnnouncementEntry | str] = Field(default_factory=list)
    black: list[AnnouncementEntry | str] = Field(default_factory=list)


class ViewerScoresheet(BaseModel):
    model_config = ConfigDict(extra="forbid")

    viewer_color: PlayerColor
    last_move_number: int = Field(ge=0)
    turns: list[ScoresheetTurn] = Field(default_factory=list)


class MaterialSideSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pieces_remaining: int = Field(ge=0, le=16)
    pawns_captured: int | None = Field(default=None, ge=0, le=8)


class MaterialSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    white: MaterialSideSummary
    black: MaterialSideSummary


class ReserveSideSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pawns: int = Field(default=0, ge=0)
    knights: int = Field(default=0, ge=0)
    bishops: int = Field(default=0, ge=0)
    rooks: int = Field(default=0, ge=0)
    queens: int = Field(default=0, ge=0)


class ReserveSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    white: ReserveSideSummary
    black: ReserveSideSummary


class GameStateResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    game_id: str
    state: GameState
    turn: PlayerColor | None = None
    move_number: int = Field(ge=1)
    your_color: PlayerColor
    your_fen: str
    allowed_moves: list[str] = Field(default_factory=list)
    material_summary: MaterialSummary
    reserve_summary: ReserveSummary
    scoresheet: ViewerScoresheet
    referee_log: list[RefereeLogItem]
    referee_turns: list[RefereeTurnEntry] = Field(default_factory=list)
    possible_actions: list[Literal["move", "ask_any"]]
    result: dict[str, Any] | None = None
    clock: ClockState


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


class LobbyStatsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    active_games_now: int = 0
    completed_last_hour: int = 0
    completed_last_24_hours: int = 0
    completed_total: int = 0


class TranscriptAnswer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    main: str
    capture_square: str | None = None
    captured_piece_announcement: PieceAnnouncement | None = None
    dropped_piece_announcement: PieceAnnouncement | None = None
    promotion_announced: bool | None = None
    special: str | None = None
    next_turn_pawn_tries: int | None = Field(default=None, ge=0)
    next_turn_has_pawn_capture: bool | None = None
    next_turn_pawn_try_squares: list[str] | None = None


class TranscriptMoveItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ply: int = Field(ge=1)
    color: PlayerColor
    question_type: str
    uci: str | None = None
    answer: TranscriptAnswer
    move_done: bool
    timestamp: datetime | None = None
    replay_fen: ReplayFen | None = None


class GameTranscriptResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    game_id: str
    rule_variant: RuleVariant
    viewer_color: PlayerColor | None = None
    moves: list[TranscriptMoveItem]


class RecentGameItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    game_id: str
    game_code: str = Field(min_length=6, max_length=6, pattern=r"^[2-9A-HJ-KM-NP-Z]{6}$")
    rule_variant: RuleVariant
    white: PublicPlayer
    black: PublicPlayer
    result: dict[str, Any] | None = None
    completed_at: datetime


class RecentGamesResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    games: list[RecentGameItem]


class PublicPlayer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    username: str
    connected: bool
    role: Literal["user", "bot"] = "user"
    elo: int = 1200
    ratings: dict[str, dict[str, int]] = Field(default_factory=dict)


class GameMetadataResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    game_id: str
    game_code: str = Field(min_length=6, max_length=6, pattern=r"^[2-9A-HJ-KM-NP-Z]{6}$")
    rule_variant: RuleVariant
    state: GameState
    opponent_type: OpponentType = "human"
    white: PublicPlayer
    black: PublicPlayer | None = None
    turn: PlayerColor | None = None
    move_number: int = Field(ge=1)
    created_at: datetime
    updated_at: datetime
    result: dict[str, Any] | None = None
    rating_snapshot: dict[str, Any] | None = None
