from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.models.bot import BotProfile


class EloRatingTrack(BaseModel):
    elo: int = 1200
    peak: int = 1200


class UserStats(BaseModel):
    games_played: int = 0
    games_won: int = 0
    games_lost: int = 0
    games_drawn: int = 0
    elo: int = 1200
    elo_peak: int = 1200
    ratings: dict[str, EloRatingTrack] = Field(
        default_factory=lambda: {
            "overall": EloRatingTrack(),
            "vs_humans": EloRatingTrack(),
            "vs_bots": EloRatingTrack(),
        }
    )


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
        payload["stats"] = normalize_user_stats_payload(payload.get("stats"))
        return cls.model_validate(payload)


def utcnow() -> datetime:
    return datetime.now(UTC)


def default_user_stats_payload() -> dict[str, Any]:
    return {
        "games_played": 0,
        "games_won": 0,
        "games_lost": 0,
        "games_drawn": 0,
        "elo": 1200,
        "elo_peak": 1200,
        "ratings": {
            "overall": {"elo": 1200, "peak": 1200},
            "vs_humans": {"elo": 1200, "peak": 1200},
            "vs_bots": {"elo": 1200, "peak": 1200},
        },
    }


def normalize_user_stats_payload(raw_stats: dict[str, Any] | None) -> dict[str, Any]:
    stats = default_user_stats_payload()
    current = dict(raw_stats or {})
    for key in ("games_played", "games_won", "games_lost", "games_drawn"):
        stats[key] = int(current.get(key, stats[key]))

    ratings = current.get("ratings") if isinstance(current.get("ratings"), dict) else {}
    overall_elo = int(current.get("elo", ratings.get("overall", {}).get("elo", 1200)))
    overall_peak = int(current.get("elo_peak", ratings.get("overall", {}).get("peak", overall_elo)))

    stats["ratings"]["overall"]["elo"] = overall_elo
    stats["ratings"]["overall"]["peak"] = max(overall_peak, overall_elo)

    for key in ("vs_humans", "vs_bots"):
        track = ratings.get(key) if isinstance(ratings.get(key), dict) else {}
        elo = int(track.get("elo", 1200))
        peak = int(track.get("peak", elo))
        stats["ratings"][key]["elo"] = elo
        stats["ratings"][key]["peak"] = max(peak, elo)

    stats["elo"] = stats["ratings"]["overall"]["elo"]
    stats["elo_peak"] = stats["ratings"]["overall"]["peak"]
    return stats
