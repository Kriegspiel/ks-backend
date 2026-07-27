from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Callable
from uuid import uuid4

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError
from pydantic import ValidationError
import structlog

from app.config import Settings
from app.models.game import GameReviewResponse
from app.models.tutor import (
    TutorAnalysisResponse,
    TutorFeedbackRequest,
    TutorFeedbackResponse,
    TutorGameResponse,
    TutorModelOutput,
    TutorProfileContent,
    TutorProfileResponse,
    TutorUsageResponse,
)
from app.services.tutor_analysis import (
    OpenAITutorProvider,
    TutorEvidence,
    TutorEvidenceError,
    TutorProviderError,
)


logger = structlog.get_logger("app.tutor")
PROFILE_READY_GAMES = 5
GENERATION_CLAIM_TTL = timedelta(minutes=5)


class TutorServiceError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class TutorService:
    def __init__(
        self,
        analyses_collection: Any,
        profiles_collection: Any,
        usage_collection: Any,
        *,
        settings: Settings,
        provider: OpenAITutorProvider | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.analyses = analyses_collection
        self.profiles = profiles_collection
        self.usage = usage_collection
        self.settings = settings
        self.provider = provider or OpenAITutorProvider(settings)
        self.now = now or (lambda: datetime.now(UTC))

    def _analysis_query(self, *, user_id: str, game_id: str) -> dict[str, Any]:
        return {
            "user_id": user_id,
            "game_id": game_id,
            "analysis_version": self.settings.TUTOR_ANALYSIS_VERSION,
            "prompt_version": self.settings.TUTOR_PROMPT_VERSION,
            "model": self.settings.TUTOR_ANALYSIS_MODEL,
        }

    def _month(self) -> str:
        return self.now().astimezone(UTC).strftime("%Y-%m")

    def _budget_limit(self) -> float:
        return round(max(0.0, float(self.settings.TUTOR_MONTHLY_BUDGET_USD)), 6)

    async def _usage_response(self, *, user_id: str) -> TutorUsageResponse:
        month = self._month()
        document = await self.usage.find_one({"user_id": user_id, "month": month}) or {}
        spent = round(max(0.0, float(document.get("spent_usd", 0.0))), 6)
        reserved = round(max(0.0, float(document.get("reserved_usd", 0.0))), 6)
        limit = self._budget_limit()
        return TutorUsageResponse(
            month=month,
            limit_usd=limit,
            spent_usd=spent,
            reserved_usd=reserved,
            remaining_usd=round(max(0.0, limit - spent - reserved), 6),
        )

    @staticmethod
    def _profile_content(document: dict[str, Any]) -> TutorProfileContent:
        content = document.get("content")
        if isinstance(content, dict):
            try:
                return TutorProfileContent.model_validate(content)
            except ValidationError:
                pass
        return TutorProfileContent(
            summary="Tutor is learning from your reviewed games.",
            strengths=[],
            focus_areas=[],
        )

    async def get_profile(self, *, user_id: str) -> TutorProfileResponse:
        document = await self.profiles.find_one({"user_id": user_id}) or {}
        reviewed_games = max(0, int(document.get("reviewed_games", 0)))
        content = self._profile_content(document)
        updated_at = document.get("updated_at") if isinstance(document.get("updated_at"), datetime) else None
        return TutorProfileResponse(
            reviewed_games=reviewed_games,
            ready=reviewed_games >= PROFILE_READY_GAMES,
            games_until_ready=max(0, PROFILE_READY_GAMES - reviewed_games),
            summary=content.summary,
            strengths=content.strengths,
            focus_areas=content.focus_areas,
            updated_at=updated_at,
        )

    @staticmethod
    def _feedback(document: dict[str, Any]) -> TutorFeedbackResponse | None:
        feedback = document.get("feedback")
        if not isinstance(feedback, dict):
            return None
        return TutorFeedbackResponse.model_validate(feedback)

    def _analysis_response(self, document: dict[str, Any], *, cached: bool) -> TutorAnalysisResponse:
        return TutorAnalysisResponse(
            game_code=str(document["game_code"]),
            generated_at=document["generated_at"],
            cached=cached,
            model=str(document["model"]),
            analysis_version=str(document["analysis_version"]),
            prompt_version=str(document["prompt_version"]),
            review=TutorModelOutput.model_validate(document["review"]),
            feedback=self._feedback(document),
        )

    def _build_evidence(self, review: GameReviewResponse) -> TutorEvidence:
        try:
            return self.provider_evidence(review)
        except TutorEvidenceError as exc:
            raise TutorServiceError("TUTOR_GAME_NOT_ELIGIBLE", str(exc)) from exc

    @staticmethod
    def provider_evidence(review: GameReviewResponse) -> TutorEvidence:
        from app.services.tutor_analysis import build_tutor_evidence

        return build_tutor_evidence(review)

    def _eligibility_reason(self, evidence: TutorEvidence) -> str | None:
        minimum = max(1, int(self.settings.TUTOR_MIN_COMPLETED_TURNS))
        if evidence.completed_turns < minimum:
            return f"Tutor needs at least {minimum} completed turns for a useful review."
        return None

    async def get_game(self, *, review: GameReviewResponse, user_id: str) -> TutorGameResponse:
        evidence = self._build_evidence(review)
        query = self._analysis_query(user_id=user_id, game_id=review.transcript.game_id)
        document = await self.analyses.find_one({**query, "status": "completed"})
        profile = await self.get_profile(user_id=user_id)
        usage = await self._usage_response(user_id=user_id)
        reason = self._eligibility_reason(evidence)
        return TutorGameResponse(
            game_code=evidence.game_code,
            eligible=reason is None,
            eligibility_reason=reason,
            analysis=self._analysis_response(document, cached=True) if document else None,
            profile=profile,
            usage=usage,
        )

    async def _claim_generation(self, *, query: dict[str, Any], game_code: str) -> str:
        now = self.now()
        claim_token = uuid4().hex
        document = {
            **query,
            "game_code": game_code,
            "status": "generating",
            "claim_token": claim_token,
            "started_at": now,
            "updated_at": now,
        }
        try:
            await self.analyses.insert_one(document)
            return claim_token
        except DuplicateKeyError:
            stale_before = now - GENERATION_CLAIM_TTL
            result = await self.analyses.update_one(
                {
                    **query,
                    "status": "generating",
                    "started_at": {"$lte": stale_before},
                },
                {
                    "$set": {
                        "claim_token": claim_token,
                        "started_at": now,
                        "updated_at": now,
                    }
                },
            )
            if result.modified_count == 1:
                return claim_token
            raise TutorServiceError("TUTOR_GENERATION_IN_PROGRESS", "Tutor is already analyzing this game.")

    async def _release_claim(self, *, query: dict[str, Any], claim_token: str) -> None:
        await self.analyses.delete_one({**query, "claim_token": claim_token, "status": "generating"})

    async def _reserve_budget(self, *, user_id: str, amount: float) -> None:
        month = self._month()
        now = self.now()
        limit = self._budget_limit()
        key = {"user_id": user_id, "month": month}
        await self.usage.update_one(
            key,
            {
                "$setOnInsert": {
                    **key,
                    "spent_usd": 0.0,
                    "reserved_usd": 0.0,
                    "request_count": 0,
                    "failed_request_count": 0,
                    "created_at": now,
                },
                "$set": {"updated_at": now, "limit_usd": limit},
            },
            upsert=True,
        )
        reserved = await self.usage.find_one_and_update(
            {
                **key,
                "$expr": {
                    "$lte": [
                        {
                            "$add": [
                                {"$ifNull": ["$spent_usd", 0.0]},
                                {"$ifNull": ["$reserved_usd", 0.0]},
                                amount,
                            ]
                        },
                        limit,
                    ]
                },
            },
            {"$inc": {"reserved_usd": amount}, "$set": {"updated_at": now}},
            return_document=ReturnDocument.AFTER,
        )
        if reserved is None:
            limit_label = f"{limit:g}"
            raise TutorServiceError(
                "TUTOR_BUDGET_EXCEEDED",
                f"Tutor's ${limit_label} monthly private-beta budget has been reached.",
            )

    async def _settle_budget(
        self,
        *,
        user_id: str,
        reservation: float,
        actual: float,
        failed: bool,
    ) -> None:
        increments = {
            "reserved_usd": -reservation,
            "spent_usd": actual,
            "request_count": 1,
        }
        if failed:
            increments["failed_request_count"] = 1
        await self.usage.update_one(
            {"user_id": user_id, "month": self._month()},
            {"$inc": increments, "$set": {"updated_at": self.now()}},
        )

    def _actual_cost(
        self,
        *,
        input_tokens: int | None,
        cached_input_tokens: int | None,
        output_tokens: int | None,
        reservation: float,
    ) -> float:
        if input_tokens is None or output_tokens is None:
            return reservation
        if cached_input_tokens is not None and not 0 <= cached_input_tokens <= input_tokens:
            return reservation
        cached_tokens = cached_input_tokens or 0
        uncached_tokens = input_tokens - cached_tokens
        input_cost = (
            uncached_tokens * max(0.0, self.settings.TUTOR_INPUT_COST_PER_MILLION_USD)
            + cached_tokens * max(0.0, self.settings.TUTOR_CACHED_INPUT_COST_PER_MILLION_USD)
        ) / 1_000_000
        output_cost = output_tokens * max(0.0, self.settings.TUTOR_OUTPUT_COST_PER_MILLION_USD) / 1_000_000
        return round(max(0.0, input_cost + output_cost), 6)

    async def _store_completed_analysis(
        self,
        *,
        query: dict[str, Any],
        claim_token: str,
        game_code: str,
        provider_result: Any,
        actual_cost: float,
    ) -> dict[str, Any]:
        now = self.now()
        completed = {
            **query,
            "game_code": game_code,
            "status": "completed",
            "generated_at": now,
            "updated_at": now,
            "review": provider_result.review.model_dump(mode="json"),
            "provider": {
                "response_id": provider_result.response_id,
                "input_tokens": provider_result.input_tokens,
                "cached_input_tokens": provider_result.cached_input_tokens,
                "output_tokens": provider_result.output_tokens,
                "cost_usd": actual_cost,
            },
        }
        result = await self.analyses.find_one_and_update(
            {**query, "claim_token": claim_token, "status": "generating"},
            {"$set": completed, "$unset": {"claim_token": "", "started_at": ""}},
            return_document=ReturnDocument.AFTER,
        )
        if result is None:
            raise TutorServiceError("TUTOR_STORAGE_FAILED", "Tutor analysis could not be saved.")
        return result

    async def _update_profile(
        self,
        *,
        user_id: str,
        game_id: str,
        content: TutorProfileContent,
    ) -> None:
        now = self.now()
        await self.profiles.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "content": content.model_dump(mode="json"),
                    "analysis_version": self.settings.TUTOR_ANALYSIS_VERSION,
                    "prompt_version": self.settings.TUTOR_PROMPT_VERSION,
                    "model": self.settings.TUTOR_ANALYSIS_MODEL,
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
                "$inc": {"reviewed_games": 1},
                "$addToSet": {"game_ids": game_id},
            },
            upsert=True,
        )

    async def generate(self, *, review: GameReviewResponse, user_id: str) -> TutorGameResponse:
        evidence = self._build_evidence(review)
        reason = self._eligibility_reason(evidence)
        if reason is not None:
            raise TutorServiceError("TUTOR_GAME_TOO_SHORT", reason)

        query = self._analysis_query(user_id=user_id, game_id=review.transcript.game_id)
        cached = await self.analyses.find_one({**query, "status": "completed"})
        if cached is not None:
            return TutorGameResponse(
                game_code=evidence.game_code,
                eligible=True,
                analysis=self._analysis_response(cached, cached=True),
                profile=await self.get_profile(user_id=user_id),
                usage=await self._usage_response(user_id=user_id),
            )

        try:
            self.provider.ensure_available()
        except TutorProviderError as exc:
            raise TutorServiceError(exc.code, str(exc)) from exc
        claim_token = await self._claim_generation(query=query, game_code=evidence.game_code)
        try:
            profile = await self.get_profile(user_id=user_id)
            prepared = self.provider.prepare(evidence=evidence, profile=profile, user_id=user_id)
        except Exception:
            await self._release_claim(query=query, claim_token=claim_token)
            raise
        try:
            await self._reserve_budget(user_id=user_id, amount=prepared.reservation_usd)
        except TutorServiceError:
            await self._release_claim(query=query, claim_token=claim_token)
            raise

        try:
            provider_result = await self.provider.generate(prepared)
        except TutorProviderError as exc:
            await self._settle_budget(
                user_id=user_id,
                reservation=prepared.reservation_usd,
                actual=prepared.reservation_usd,
                failed=True,
            )
            await self._release_claim(query=query, claim_token=claim_token)
            logger.warning("tutor_generation_failed", user_id=user_id, game_code=evidence.game_code, code=exc.code)
            raise TutorServiceError(exc.code, str(exc)) from exc
        except Exception as exc:
            await self._settle_budget(
                user_id=user_id,
                reservation=prepared.reservation_usd,
                actual=prepared.reservation_usd,
                failed=True,
            )
            await self._release_claim(query=query, claim_token=claim_token)
            logger.error(
                "tutor_generation_unexpected_error",
                user_id=user_id,
                game_code=evidence.game_code,
                error_type=type(exc).__name__,
            )
            raise TutorServiceError("TUTOR_PROVIDER_FAILED", "Tutor generation failed.") from exc

        actual_cost = self._actual_cost(
            input_tokens=provider_result.input_tokens,
            cached_input_tokens=provider_result.cached_input_tokens,
            output_tokens=provider_result.output_tokens,
            reservation=prepared.reservation_usd,
        )
        await self._settle_budget(
            user_id=user_id,
            reservation=prepared.reservation_usd,
            actual=actual_cost,
            failed=False,
        )
        document = await self._store_completed_analysis(
            query=query,
            claim_token=claim_token,
            game_code=evidence.game_code,
            provider_result=provider_result,
            actual_cost=actual_cost,
        )
        try:
            await self._update_profile(
                user_id=user_id,
                game_id=review.transcript.game_id,
                content=provider_result.review.profile_update,
            )
        except Exception as exc:
            logger.error(
                "tutor_profile_update_failed",
                user_id=user_id,
                game_code=evidence.game_code,
                error_type=type(exc).__name__,
            )
        logger.info(
            "tutor_generation_complete",
            user_id=user_id,
            game_code=evidence.game_code,
            model=self.settings.TUTOR_ANALYSIS_MODEL,
            cost_usd=actual_cost,
        )
        return TutorGameResponse(
            game_code=evidence.game_code,
            eligible=True,
            analysis=self._analysis_response(document, cached=False),
            profile=await self.get_profile(user_id=user_id),
            usage=await self._usage_response(user_id=user_id),
        )

    async def submit_feedback(
        self,
        *,
        game_id: str,
        game_code: str,
        user_id: str,
        feedback: TutorFeedbackRequest,
    ) -> TutorFeedbackResponse:
        now = self.now()
        feedback_document = {
            "rating": feedback.rating,
            "comment": feedback.comment,
            "updated_at": now,
        }
        query = self._analysis_query(user_id=user_id, game_id=game_id)
        document = await self.analyses.find_one_and_update(
            {**query, "game_code": game_code, "status": "completed"},
            {"$set": {"feedback": feedback_document, "updated_at": now}},
            return_document=ReturnDocument.AFTER,
        )
        if document is None:
            raise TutorServiceError("TUTOR_ANALYSIS_NOT_FOUND", "Generate this Tutor review before sending feedback.")
        return TutorFeedbackResponse.model_validate(document["feedback"])
