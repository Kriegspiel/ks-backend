from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from app.config import Settings
from app.dependencies import get_session_service, get_tutor_candidate_user
from app.main import create_app
from app.models.tutor import TutorFeedbackRequest
from app.routers import tutor as tutor_router
from app.services.game_service import GameNotFoundError
from app.services.tutor_service import TutorService, TutorServiceError
from tests.tutor_helpers import GAME_CODE, GAME_ID, USER_ID, empty_profile, make_review


def _game_response() -> dict:
    return {
        "game_code": GAME_CODE,
        "eligible": True,
        "eligibility_reason": None,
        "analysis": None,
        "profile": empty_profile(),
        "usage": {
            "month": "2026-07",
            "limit_usd": 5,
            "spent_usd": 0,
            "reserved_usd": 0,
            "remaining_usd": 5,
        },
    }


def test_tutor_http_route_is_invisible_to_outsiders_and_available_to_exact_fil_id() -> None:
    app = create_app(Settings(ENVIRONMENT="testing", TUTOR_ENABLED=True, TUTOR_BETA_USER_IDS=USER_ID))
    game_service = SimpleNamespace(get_game_review=AsyncMock(return_value=make_review()))
    tutor_service = SimpleNamespace(get_game=AsyncMock(return_value=_game_response()))
    app.dependency_overrides[tutor_router.get_game_service] = lambda: game_service
    app.dependency_overrides[tutor_router.get_tutor_service] = lambda: tutor_service
    app.dependency_overrides[get_session_service] = lambda: object()

    with TestClient(app) as client:
        anonymous = client.get(f"/api/tutor/games/{GAME_CODE}", headers={"host": "app.kriegspiel.org"})
        assert anonymous.status_code == 404
        assert tutor_service.get_game.await_count == 0

        app.dependency_overrides[get_tutor_candidate_user] = lambda: SimpleNamespace(
            id="outsider", role="user", status="active"
        )
        hidden = client.get(f"/api/tutor/games/{GAME_CODE}", headers={"host": "app.kriegspiel.org"})
        assert hidden.status_code == 404
        assert tutor_service.get_game.await_count == 0

        app.dependency_overrides[get_tutor_candidate_user] = lambda: SimpleNamespace(
            id=USER_ID, role="user", status="active"
        )
        visible = client.get(f"/api/tutor/games/{GAME_CODE}", headers={"host": "app.kriegspiel.org"})

    assert visible.status_code == 200
    assert visible.json()["usage"]["limit_usd"] == 5
    tutor_service.get_game.assert_awaited_once()


def test_get_tutor_service_prefers_app_state_and_builds_database_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    existing = object()
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(tutor_service=existing)))
    assert tutor_router.get_tutor_service(request) is existing

    db = SimpleNamespace(tutor_analyses=object(), tutor_profiles=object(), tutor_usage=object())
    monkeypatch.setattr(tutor_router, "require_db", lambda: db)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(settings=Settings())))
    service = tutor_router.get_tutor_service(request)

    assert isinstance(service, TutorService)
    assert service.analyses is db.tutor_analyses
    assert service.profiles is db.tutor_profiles
    assert service.usage is db.tutor_usage


@pytest.mark.parametrize(
    ("code", "expected_status"),
    [
        ("TUTOR_GAME_NOT_ELIGIBLE", 404),
        ("TUTOR_ANALYSIS_NOT_FOUND", 404),
        ("TUTOR_GAME_TOO_SHORT", 409),
        ("TUTOR_GENERATION_IN_PROGRESS", 409),
        ("TUTOR_BUDGET_EXCEEDED", 429),
        ("TUTOR_PROVIDER_RATE_LIMITED", 429),
        ("TUTOR_UNAVAILABLE", 503),
        ("TUTOR_STORAGE_FAILED", 503),
        ("TUTOR_PROVIDER_QUOTA", 503),
        ("TUTOR_PROVIDER_FAILED", 503),
        ("TUTOR_REFUSED", 503),
        ("UNKNOWN", 400),
    ],
)
def test_tutor_errors_have_stable_http_contract(code: str, expected_status: int) -> None:
    response = tutor_router._map_tutor_error(TutorServiceError(code, "message"))  # noqa: SLF001

    assert response.status_code == expected_status
    assert response.body == b'{"error":{"code":"' + code.encode() + b'","message":"message"}}'


@pytest.mark.asyncio
async def test_participant_review_returns_review_and_hides_game_errors() -> None:
    user = SimpleNamespace(id=USER_ID)
    game_service = SimpleNamespace(get_game_review=AsyncMock(return_value=make_review()))

    review = await tutor_router._participant_review(  # noqa: SLF001
        game_code=GAME_CODE,
        user=user,
        game_service=game_service,
    )
    assert review == make_review()
    game_service.get_game_review.assert_awaited_once_with(game_id=GAME_CODE, user_id=USER_ID)

    game_service.get_game_review.side_effect = GameNotFoundError("missing")
    response = await tutor_router._participant_review(  # noqa: SLF001
        game_code=GAME_CODE,
        user=user,
        game_service=game_service,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    assert b"your own completed games" in response.body


@pytest.mark.asyncio
async def test_profile_endpoint_returns_longitudinal_profile() -> None:
    service = SimpleNamespace(get_profile=AsyncMock(return_value=empty_profile(reviewed_games=3)))

    response = await tutor_router.get_tutor_profile(
        user=SimpleNamespace(id=USER_ID),
        tutor_service=service,
    )

    assert response.reviewed_games == 3
    service.get_profile.assert_awaited_once_with(user_id=USER_ID)


@pytest.mark.asyncio
async def test_game_endpoint_returns_service_result_and_maps_service_error() -> None:
    review = make_review()
    game_service = SimpleNamespace(get_game_review=AsyncMock(return_value=review))
    tutor_service = SimpleNamespace(get_game=AsyncMock(return_value=_game_response()))
    user = SimpleNamespace(id=USER_ID)

    response = await tutor_router.get_tutor_game(
        GAME_CODE,
        user=user,
        game_service=game_service,
        tutor_service=tutor_service,
    )
    assert response["game_code"] == GAME_CODE
    tutor_service.get_game.assert_awaited_once_with(review=review, user_id=USER_ID)

    tutor_service.get_game.side_effect = TutorServiceError("TUTOR_GAME_TOO_SHORT", "short")
    response = await tutor_router.get_tutor_game(
        GAME_CODE,
        user=user,
        game_service=game_service,
        tutor_service=tutor_service,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 409

    game_service.get_game_review.side_effect = GameNotFoundError("missing")
    response = await tutor_router.get_tutor_game(
        GAME_CODE,
        user=user,
        game_service=game_service,
        tutor_service=tutor_service,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_generate_endpoint_returns_early_private_error_service_result_and_mapped_error() -> None:
    review = make_review()
    user = SimpleNamespace(id=USER_ID)
    game_service = SimpleNamespace(get_game_review=AsyncMock(side_effect=GameNotFoundError("missing")))
    tutor_service = SimpleNamespace(generate=AsyncMock(return_value=_game_response()))

    response = await tutor_router.generate_tutor_game_analysis(
        GAME_CODE,
        user=user,
        game_service=game_service,
        tutor_service=tutor_service,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    tutor_service.generate.assert_not_awaited()

    game_service.get_game_review.side_effect = None
    game_service.get_game_review.return_value = review
    response = await tutor_router.generate_tutor_game_analysis(
        GAME_CODE,
        user=user,
        game_service=game_service,
        tutor_service=tutor_service,
    )
    assert response["eligible"] is True

    tutor_service.generate.side_effect = TutorServiceError("TUTOR_BUDGET_EXCEEDED", "limit")
    response = await tutor_router.generate_tutor_game_analysis(
        GAME_CODE,
        user=user,
        game_service=game_service,
        tutor_service=tutor_service,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 429


@pytest.mark.asyncio
async def test_feedback_endpoint_returns_early_private_error_saved_feedback_and_mapped_error() -> None:
    review = make_review()
    user = SimpleNamespace(id=USER_ID)
    feedback = TutorFeedbackRequest(rating="helpful", comment="Useful")
    game_service = SimpleNamespace(get_game_review=AsyncMock(side_effect=GameNotFoundError("missing")))
    tutor_service = SimpleNamespace(
        submit_feedback=AsyncMock(return_value={"rating": "helpful", "comment": "Useful", "updated_at": review.game.updated_at})
    )

    response = await tutor_router.submit_tutor_feedback(
        GAME_CODE,
        feedback,
        user=user,
        game_service=game_service,
        tutor_service=tutor_service,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    tutor_service.submit_feedback.assert_not_awaited()

    game_service.get_game_review.side_effect = None
    game_service.get_game_review.return_value = review
    response = await tutor_router.submit_tutor_feedback(
        GAME_CODE,
        feedback,
        user=user,
        game_service=game_service,
        tutor_service=tutor_service,
    )
    assert response["rating"] == "helpful"
    tutor_service.submit_feedback.assert_awaited_once_with(
        game_id=GAME_ID,
        game_code=GAME_CODE,
        user_id=USER_ID,
        feedback=feedback,
    )

    tutor_service.submit_feedback.side_effect = TutorServiceError("TUTOR_ANALYSIS_NOT_FOUND", "missing")
    response = await tutor_router.submit_tutor_feedback(
        GAME_CODE,
        feedback,
        user=user,
        game_service=game_service,
        tutor_service=tutor_service,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
