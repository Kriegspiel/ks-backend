"""Application-owned teaching contracts; none accept an identity or arbitrary query."""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.models.game import RuleVariant


class TutorInput(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class SessionCreate(TutorInput):
    game_code: str | None = Field(default=None, pattern=r"^[2-9A-HJ-KM-NP-Z]{6}$")


class MessageCreate(TutorInput):
    request_id: UUID
    text: str = Field(min_length=1, max_length=4000)


class GameSearch(TutorInput):
    offset: int = Field(default=0, ge=0, le=100000)
    limit: int = Field(default=15, ge=1, le=30)
    rule_variant: RuleVariant | None = None


class GameWindow(TutorInput):
    game_code: str = Field(pattern=r"^[2-9A-HJ-KM-NP-Z]{6}$")
    start: int = Field(default=1, ge=1, le=20000)
    limit: int = Field(default=30, ge=1, le=60)


class RuleRequest(TutorInput):
    rule_variant: RuleVariant


class Evidence(TutorInput):
    game_code: str = Field(pattern=r"^[2-9A-HJ-KM-NP-Z]{6}$")
    ply: int = Field(ge=1, le=20000)


class LearningNote(TutorInput):
    key: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,59}$")
    kind: Literal["observation", "hypothesis", "preference"]
    skill: str = Field(min_length=1, max_length=80)
    text: str = Field(min_length=1, max_length=900)
    confidence: Literal["low", "medium", "high"]
    evidence: list[Evidence] = Field(max_length=8)


class LearningGoal(TutorInput):
    key: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,59}$")
    skill: str = Field(min_length=1, max_length=80)
    title: str = Field(min_length=1, max_length=160)
    rationale: str = Field(min_length=1, max_length=700)
    success_criterion: str = Field(min_length=1, max_length=500)
    next_activity: str = Field(min_length=1, max_length=500)
    status: Literal["active", "maintenance", "complete"]
    evidence: list[Evidence] = Field(min_length=1, max_length=8)


class ExerciseCreate(TutorInput):
    game_code: str = Field(pattern=r"^[2-9A-HJ-KM-NP-Z]{6}$")
    ply: int = Field(ge=1, le=20000)
    kind: Literal["play", "predict", "explain"]
    skill: str = Field(min_length=1, max_length=80)
    prompt: str = Field(min_length=1, max_length=1200)
    hints: list[str] = Field(max_length=3)


class ExerciseAction(TutorInput):
    request_id: UUID
    revision: int = Field(ge=0)
    action: Literal["move", "ask_any", "predict", "explain", "hint", "reveal", "reset"]
    value: str = Field(default="", max_length=2000)
    marks: list[str] = Field(default_factory=list, max_length=64)


class MemoryDelete(TutorInput):
    kind: Literal["note", "goal"]
    key: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,59}$")
