from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.models.bot import BotProfile


class UserStats(BaseModel):
    games_played: int = 0
    games_won: int = 0
    games_lost: int = 0
    games_drawn: int = 0
    elo: int = 1200
    elo_peak: int = 1200


class UserSettings(BaseModel):
    board_theme: str = "default"
    piece_set: str = "cburnett"
    sound_enabled: bool = True
    auto_ask_any: bool = False


class UserProfile(BaseModel):
    bio: str = ""
    avatar_url: str | None = None
    country: str | None = None


class UserModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(alias="_id")
    username: str
    username_display: str
    email: str
    email_verified: bool = False
    email_verification_sent_at: datetime | None = None
    email_verified_at: datetime | None = None
    password_hash: str
    auth_providers: list[str] = Field(default_factory=lambda: ["local"])
    profile: UserProfile = Field(default_factory=UserProfile)
    bot_profile: BotProfile | None = None
    stats: UserStats = Field(default_factory=UserStats)
    settings: UserSettings = Field(default_factory=UserSettings)
    role: str = "user"
    status: str = "active"
    last_active_at: datetime
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_mongo(cls, doc: dict[str, Any]) -> "UserModel":
        payload = dict(doc)
        payload["_id"] = str(payload["_id"])
        return cls.model_validate(payload)


def utcnow() -> datetime:
    return datetime.now(UTC)
