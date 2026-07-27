from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import JSONResponse

from app.dependencies import require_db, require_tutor_access
from app.models.game import GameReviewResponse
from app.models.tutor import TutorFeedbackRequest, TutorFeedbackResponse, TutorGameResponse, TutorProfileResponse
from app.models.user import UserModel
from app.routers.game import get_game_service
from app.services.game_service import GameService, GameServiceError
from app.services.tutor_service import TutorService, TutorServiceError


router = APIRouter(prefix="/tutor", tags=["tutor"])


def get_tutor_service(request: Request) -> TutorService:
    service = getattr(request.app.state, "tutor_service", None)
    if service is not None:
        return service
    db = require_db()
    return TutorService(
        db.tutor_analyses,
        db.tutor_profiles,
        db.tutor_usage,
        settings=request.app.state.settings,
    )


def _error_response(*, status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"error": {"code": code, "message": message}})


def _map_tutor_error(exc: TutorServiceError) -> JSONResponse:
    if exc.code in {"TUTOR_GAME_NOT_ELIGIBLE", "TUTOR_ANALYSIS_NOT_FOUND"}:
        http_status = status.HTTP_404_NOT_FOUND
    elif exc.code in {"TUTOR_GAME_TOO_SHORT", "TUTOR_GENERATION_IN_PROGRESS"}:
        http_status = status.HTTP_409_CONFLICT
    elif exc.code == "TUTOR_BUDGET_EXCEEDED":
        http_status = status.HTTP_429_TOO_MANY_REQUESTS
    elif exc.code == "TUTOR_PROVIDER_RATE_LIMITED":
        http_status = status.HTTP_429_TOO_MANY_REQUESTS
    elif exc.code in {"TUTOR_UNAVAILABLE", "TUTOR_STORAGE_FAILED"}:
        http_status = status.HTTP_503_SERVICE_UNAVAILABLE
    elif exc.code.startswith("TUTOR_PROVIDER_") or exc.code == "TUTOR_REFUSED":
        http_status = status.HTTP_503_SERVICE_UNAVAILABLE
    else:
        http_status = status.HTTP_400_BAD_REQUEST
    return _error_response(status_code=http_status, code=exc.code, message=str(exc))


async def _participant_review(
    *,
    game_code: str,
    user: UserModel,
    game_service: GameService,
) -> GameReviewResponse | JSONResponse:
    try:
        return await game_service.get_game_review(game_id=game_code, user_id=user.id)
    except GameServiceError:
        return _error_response(
            status_code=status.HTTP_404_NOT_FOUND,
            code="TUTOR_GAME_NOT_ELIGIBLE",
            message="Tutor analysis is available only for your own completed games.",
        )


@router.get("/profile", response_model=TutorProfileResponse)
async def get_tutor_profile(
    user: UserModel = Depends(require_tutor_access),
    tutor_service: TutorService = Depends(get_tutor_service),
) -> TutorProfileResponse:
    return await tutor_service.get_profile(user_id=user.id)


@router.get("/games/{game_code}", response_model=TutorGameResponse)
async def get_tutor_game(
    game_code: str,
    user: UserModel = Depends(require_tutor_access),
    game_service: GameService = Depends(get_game_service),
    tutor_service: TutorService = Depends(get_tutor_service),
) -> Any:
    review = await _participant_review(game_code=game_code, user=user, game_service=game_service)
    if isinstance(review, JSONResponse):
        return review
    try:
        return await tutor_service.get_game(review=review, user_id=user.id)
    except TutorServiceError as exc:
        return _map_tutor_error(exc)

@router.post("/games/{game_code}/analysis", response_model=TutorGameResponse)
async def generate_tutor_game_analysis(
    game_code: str,
    user: UserModel = Depends(require_tutor_access),
    game_service: GameService = Depends(get_game_service),
    tutor_service: TutorService = Depends(get_tutor_service),
) -> Any:
    review = await _participant_review(game_code=game_code, user=user, game_service=game_service)
    if isinstance(review, JSONResponse):
        return review
    try:
        return await tutor_service.generate(review=review, user_id=user.id)
    except TutorServiceError as exc:
        return _map_tutor_error(exc)


@router.post("/games/{game_code}/feedback", response_model=TutorFeedbackResponse)
async def submit_tutor_feedback(
    game_code: str,
    payload: TutorFeedbackRequest,
    user: UserModel = Depends(require_tutor_access),
    game_service: GameService = Depends(get_game_service),
    tutor_service: TutorService = Depends(get_tutor_service),
) -> Any:
    review = await _participant_review(game_code=game_code, user=user, game_service=game_service)
    if isinstance(review, JSONResponse):
        return review
    try:
        return await tutor_service.submit_feedback(
            game_id=review.transcript.game_id,
            game_code=review.game.game_code,
            user_id=user.id,
            feedback=payload,
        )
    except TutorServiceError as exc:
        return _map_tutor_error(exc)
