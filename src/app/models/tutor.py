from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


TutorConfidence = Literal["low", "medium", "high"]
TutorFeedbackRating = Literal["helpful", "not_helpful", "incorrect"]


class TutorKeyMoment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    turn: int = Field(ge=1)
    title: str = Field(min_length=1, max_length=100)
    observation: str = Field(min_length=1, max_length=600)
    why_it_matters: str = Field(min_length=1, max_length=600)
    suggestion: str = Field(min_length=1, max_length=600)
    evidence: list[str] = Field(min_length=1, max_length=4)
    confidence: TutorConfidence


class TutorSkillNote(BaseModel):
    model_config = ConfigDict(extra="forbid")

    skill: str = Field(min_length=1, max_length=80)
    evidence: str = Field(min_length=1, max_length=500)
    action: str = Field(min_length=1, max_length=500)


class TutorDrill(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str = Field(min_length=1, max_length=100)
    instructions: str = Field(min_length=1, max_length=700)
    success_criterion: str = Field(min_length=1, max_length=400)


class TutorFocusArea(BaseModel):
    model_config = ConfigDict(extra="forbid")

    skill: str = Field(min_length=1, max_length=80)
    reason: str = Field(min_length=1, max_length=500)
    next_action: str = Field(min_length=1, max_length=500)


class TutorProfileContent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    summary: str = Field(min_length=1, max_length=900)
    strengths: list[str] = Field(max_length=3)
    focus_areas: list[TutorFocusArea] = Field(max_length=3)


class TutorModelOutput(BaseModel):
    """Strict structured output returned by the Tutor model."""

    model_config = ConfigDict(extra="forbid")

    overview: str = Field(min_length=1, max_length=1200)
    result_context: str = Field(min_length=1, max_length=700)
    key_moments: list[TutorKeyMoment] = Field(max_length=5)
    strengths: list[TutorSkillNote] = Field(max_length=3)
    improvements: list[TutorSkillNote] = Field(max_length=3)
    next_drill: TutorDrill
    profile_update: TutorProfileContent


class TutorUsageResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    month: str
    limit_usd: float = Field(ge=0)
    spent_usd: float = Field(ge=0)
    reserved_usd: float = Field(ge=0)
    remaining_usd: float = Field(ge=0)


class TutorFeedbackRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    rating: TutorFeedbackRating
    comment: str | None = Field(default=None, max_length=500)

    @field_validator("comment")
    @classmethod
    def normalize_comment(cls, value: str | None) -> str | None:
        return value or None


class TutorFeedbackResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rating: TutorFeedbackRating
    comment: str | None = None
    updated_at: datetime


class TutorProfileResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reviewed_games: int = Field(ge=0)
    ready: bool
    games_until_ready: int = Field(ge=0)
    summary: str
    strengths: list[str] = Field(default_factory=list)
    focus_areas: list[TutorFocusArea] = Field(default_factory=list)
    updated_at: datetime | None = None


class TutorAnalysisResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    game_code: str
    generated_at: datetime
    cached: bool
    model: str
    analysis_version: str
    prompt_version: str
    review: TutorModelOutput
    feedback: TutorFeedbackResponse | None = None


class TutorGameResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    game_code: str
    eligible: bool
    eligibility_reason: str | None = None
    analysis: TutorAnalysisResponse | None = None
    profile: TutorProfileResponse
    usage: TutorUsageResponse
