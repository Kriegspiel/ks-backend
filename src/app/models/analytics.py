from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


_ATTRIBUTION_VALUE_PATTERN = r"^[a-zA-Z0-9_.:/@+\-\s]+$"


class AttributionUtm(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    source: str | None = Field(default=None, min_length=1, max_length=80, pattern=_ATTRIBUTION_VALUE_PATTERN)
    medium: str | None = Field(default=None, min_length=1, max_length=80, pattern=_ATTRIBUTION_VALUE_PATTERN)
    campaign: str | None = Field(default=None, min_length=1, max_length=120, pattern=_ATTRIBUTION_VALUE_PATTERN)
    content: str | None = Field(default=None, min_length=1, max_length=120, pattern=_ATTRIBUTION_VALUE_PATTERN)
    term: str | None = Field(default=None, min_length=1, max_length=120, pattern=_ATTRIBUTION_VALUE_PATTERN)

    @field_validator("source", "medium", "campaign", "content", "term", mode="before")
    @classmethod
    def empty_string_to_none(cls, value: object) -> object:
        if isinstance(value, str) and not value.strip():
            return None
        return value


class CampaignVisitRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    landing_path: str = Field(default="/", min_length=1, max_length=512)
    referrer_host: str | None = Field(default=None, min_length=1, max_length=255, pattern=r"^[a-zA-Z0-9_.:-]+$")
    utm: AttributionUtm = Field(default_factory=AttributionUtm)

    @field_validator("landing_path")
    @classmethod
    def landing_path_must_be_relative(cls, value: str) -> str:
        if not value.startswith("/") or value.startswith("//"):
            return "/"
        return value


class CampaignVisitResponse(BaseModel):
    attribution_id: str
    cookie_max_age_seconds: int


class AcquisitionReportRow(BaseModel):
    source: str | None = None
    medium: str | None = None
    campaign: str | None = None
    visits: int = 0
    sessions: int = 0
    acquired_users: int = 0
    games_created: int = 0
    games_completed: int = 0


class AcquisitionReportResponse(BaseModel):
    days: int
    generated_at: datetime
    rows: list[AcquisitionReportRow]


AttributionEventType = Literal["campaign_visit"]
