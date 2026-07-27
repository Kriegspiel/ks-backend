from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from pymongo.errors import DuplicateKeyError

from app.config import Settings
from app.models.tutor import TutorFeedbackRequest
from app.services.tutor_analysis import PreparedTutorRequest, TutorProviderError, TutorProviderResult
from app.services.tutor_service import GENERATION_CLAIM_TTL, TutorService, TutorServiceError
from tests.tutor_helpers import GAME_CODE, GAME_ID, NOW, USER_ID, make_review, sample_model_output


def _collections() -> tuple[SimpleNamespace, SimpleNamespace, SimpleNamespace]:
    analyses = SimpleNamespace(
        find_one=AsyncMock(return_value=None),
        insert_one=AsyncMock(),
        update_one=AsyncMock(),
        delete_one=AsyncMock(),
        find_one_and_update=AsyncMock(),
    )
    profiles = SimpleNamespace(find_one=AsyncMock(return_value=None), update_one=AsyncMock())
    usage = SimpleNamespace(
        find_one=AsyncMock(return_value=None),
        update_one=AsyncMock(),
        find_one_and_update=AsyncMock(return_value={"reserved_usd": 0.1}),
    )
    return analyses, profiles, usage


def _provider() -> SimpleNamespace:
    return SimpleNamespace(
        ensure_available=Mock(),
        prepare=Mock(
            return_value=PreparedTutorRequest(
                payload={"request": True},
                reservation_usd=0.1,
                evidence_refs=frozenset(),
                evidence_turns=frozenset(),
            )
        ),
        generate=AsyncMock(
            return_value=TutorProviderResult(
                review=sample_model_output(),
                response_id="resp_123",
                input_tokens=1000,
                cached_input_tokens=400,
                output_tokens=500,
            )
        ),
    )


def _service(
    *,
    settings: Settings | None = None,
    provider: SimpleNamespace | None = None,
) -> tuple[TutorService, SimpleNamespace, SimpleNamespace, SimpleNamespace, SimpleNamespace]:
    analyses, profiles, usage = _collections()
    resolved_provider = provider or _provider()
    service = TutorService(
        analyses,
        profiles,
        usage,
        settings=settings or Settings(TUTOR_MONTHLY_BUDGET_USD=5),
        provider=resolved_provider,
        now=lambda: NOW,
    )
    return service, analyses, profiles, usage, resolved_provider


def _analysis_document(*, feedback: bool = False) -> dict:
    document = {
        "user_id": USER_ID,
        "game_id": GAME_ID,
        "game_code": GAME_CODE,
        "status": "completed",
        "model": "gpt-5.6-terra",
        "analysis_version": "tutor-evidence-v1",
        "prompt_version": "tutor-private-beta-v1",
        "generated_at": NOW,
        "review": sample_model_output().model_dump(mode="json"),
    }
    if feedback:
        document["feedback"] = {"rating": "helpful", "comment": "Useful", "updated_at": NOW}
    return document


def test_service_defaults_provider_and_builds_versioned_query() -> None:
    analyses, profiles, usage = _collections()
    service = TutorService(analyses, profiles, usage, settings=Settings(), now=lambda: NOW)

    assert service.provider.__class__.__name__ == "OpenAITutorProvider"
    assert service._analysis_query(user_id=USER_ID, game_id=GAME_ID) == {  # noqa: SLF001
        "user_id": USER_ID,
        "game_id": GAME_ID,
        "analysis_version": "tutor-evidence-v1",
        "prompt_version": "tutor-private-beta-v1",
        "model": "gpt-5.6-terra",
    }
    assert service._month() == "2026-07"  # noqa: SLF001


@pytest.mark.asyncio
async def test_usage_response_enforces_five_dollar_limit_and_clamps_bad_storage_values() -> None:
    service, _analyses, _profiles, usage, _provider_obj = _service(
        settings=Settings(TUTOR_MONTHLY_BUDGET_USD=-5)
    )
    usage.find_one.return_value = {"spent_usd": -1, "reserved_usd": -2}

    response = await service._usage_response(user_id=USER_ID)  # noqa: SLF001

    assert response.model_dump() == {
        "month": "2026-07",
        "limit_usd": 0.0,
        "spent_usd": 0.0,
        "reserved_usd": 0.0,
        "remaining_usd": 0.0,
    }

    service.settings.TUTOR_MONTHLY_BUDGET_USD = 5
    usage.find_one.return_value = {"spent_usd": 1.25, "reserved_usd": 0.5}
    response = await service._usage_response(user_id=USER_ID)  # noqa: SLF001
    assert response.limit_usd == 5
    assert response.remaining_usd == 3.25


@pytest.mark.asyncio
async def test_profiles_progress_from_learning_to_ready_and_recover_from_corrupt_content() -> None:
    service, _analyses, profiles, _usage, _provider_obj = _service()
    learning = await service.get_profile(user_id=USER_ID)
    assert learning.reviewed_games == 0
    assert learning.ready is False
    assert learning.games_until_ready == 5
    assert learning.summary == "Tutor is learning from your reviewed games."

    profiles.find_one.return_value = {
        "reviewed_games": 5,
        "updated_at": NOW,
        "content": sample_model_output().profile_update.model_dump(mode="json"),
    }
    ready = await service.get_profile(user_id=USER_ID)
    assert ready.ready is True
    assert ready.games_until_ready == 0
    assert ready.updated_at == NOW
    assert ready.strengths == ["Probe discipline"]

    profiles.find_one.return_value = {
        "reviewed_games": -10,
        "updated_at": "not-a-date",
        "content": {"summary": "missing required fields"},
    }
    recovered = await service.get_profile(user_id=USER_ID)
    assert recovered.reviewed_games == 0
    assert recovered.updated_at is None
    assert recovered.focus_areas == []

    assert service._profile_content({"content": "not-a-mapping"}).strengths == []  # noqa: SLF001


def test_analysis_response_parses_optional_feedback() -> None:
    service, *_rest = _service()
    without_feedback = service._analysis_response(_analysis_document(), cached=True)  # noqa: SLF001
    with_feedback = service._analysis_response(_analysis_document(feedback=True), cached=False)  # noqa: SLF001

    assert without_feedback.cached is True
    assert without_feedback.feedback is None
    assert with_feedback.cached is False
    assert with_feedback.feedback is not None
    assert with_feedback.feedback.rating == "helpful"


def test_build_evidence_maps_private_eligibility_errors_and_minimum_turns() -> None:
    service, *_rest = _service(settings=Settings(TUTOR_MIN_COMPLETED_TURNS=0))
    evidence = service._build_evidence(make_review())  # noqa: SLF001
    assert service._eligibility_reason(evidence) is None  # noqa: SLF001

    service.settings.TUTOR_MIN_COMPLETED_TURNS = 5
    assert "at least 5" in str(service._eligibility_reason(evidence))  # noqa: SLF001

    with pytest.raises(TutorServiceError) as exc_info:
        service._build_evidence(make_review(participant=False))  # noqa: SLF001
    assert exc_info.value.code == "TUTOR_GAME_NOT_ELIGIBLE"


@pytest.mark.asyncio
async def test_get_game_returns_eligibility_cache_profile_and_budget() -> None:
    service, analyses, profiles, usage, _provider_obj = _service(settings=Settings(TUTOR_MIN_COMPLETED_TURNS=5))
    analyses.find_one.return_value = _analysis_document(feedback=True)
    profiles.find_one.return_value = {"reviewed_games": 1}
    usage.find_one.return_value = {"spent_usd": 0.02, "reserved_usd": 0}

    response = await service.get_game(review=make_review(), user_id=USER_ID)

    assert response.game_code == GAME_CODE
    assert response.eligible is False
    assert response.eligibility_reason is not None
    assert response.analysis is not None
    assert response.analysis.cached is True
    assert response.profile.reviewed_games == 1
    assert response.usage.limit_usd == 5

    analyses.find_one.return_value = None
    service.settings.TUTOR_MIN_COMPLETED_TURNS = 4
    response = await service.get_game(review=make_review(), user_id=USER_ID)
    assert response.eligible is True
    assert response.eligibility_reason is None
    assert response.analysis is None


@pytest.mark.asyncio
async def test_claim_generation_inserts_reclaims_stale_and_rejects_live_claim() -> None:
    service, analyses, _profiles, _usage, _provider_obj = _service()
    query = service._analysis_query(user_id=USER_ID, game_id=GAME_ID)  # noqa: SLF001

    token = await service._claim_generation(query=query, game_code=GAME_CODE)  # noqa: SLF001
    assert token
    inserted = analyses.insert_one.await_args.args[0]
    assert inserted["claim_token"] == token
    assert inserted["status"] == "generating"

    analyses.insert_one.side_effect = DuplicateKeyError("duplicate")
    analyses.update_one.return_value = SimpleNamespace(modified_count=1)
    reclaimed = await service._claim_generation(query=query, game_code=GAME_CODE)  # noqa: SLF001
    assert reclaimed != token
    stale_query = analyses.update_one.await_args.args[0]
    assert stale_query["started_at"] == {"$lte": NOW - GENERATION_CLAIM_TTL}

    analyses.update_one.return_value = SimpleNamespace(modified_count=0)
    with pytest.raises(TutorServiceError) as exc_info:
        await service._claim_generation(query=query, game_code=GAME_CODE)  # noqa: SLF001
    assert exc_info.value.code == "TUTOR_GENERATION_IN_PROGRESS"

    await service._release_claim(query=query, claim_token=reclaimed)  # noqa: SLF001
    analyses.delete_one.assert_awaited_once_with({**query, "claim_token": reclaimed, "status": "generating"})


@pytest.mark.asyncio
async def test_budget_reservation_is_atomic_and_rejects_requests_over_five_dollars() -> None:
    service, _analyses, _profiles, usage, _provider_obj = _service()

    await service._reserve_budget(user_id=USER_ID, amount=0.1)  # noqa: SLF001

    key = {"user_id": USER_ID, "month": "2026-07"}
    usage.update_one.assert_awaited_once()
    assert usage.update_one.await_args.args[0] == key
    reserve_query = usage.find_one_and_update.await_args.args[0]
    assert reserve_query["$expr"]["$lte"][1] == 5.0
    assert reserve_query["$expr"]["$lte"][0]["$add"][-1] == 0.1

    usage.find_one_and_update.return_value = None
    with pytest.raises(TutorServiceError) as exc_info:
        await service._reserve_budget(user_id=USER_ID, amount=5.01)  # noqa: SLF001
    assert exc_info.value.code == "TUTOR_BUDGET_EXCEEDED"
    assert "$5" in str(exc_info.value)


@pytest.mark.asyncio
async def test_budget_settlement_and_actual_cost_are_fail_closed() -> None:
    service, _analyses, _profiles, usage, _provider_obj = _service(
        settings=Settings(
            TUTOR_INPUT_COST_PER_MILLION_USD=2.5,
            TUTOR_CACHED_INPUT_COST_PER_MILLION_USD=0.25,
            TUTOR_OUTPUT_COST_PER_MILLION_USD=15,
        )
    )

    assert (
        service._actual_cost(  # noqa: SLF001
            input_tokens=None,
            cached_input_tokens=None,
            output_tokens=10,
            reservation=0.1,
        )
        == 0.1
    )
    assert (
        service._actual_cost(  # noqa: SLF001
            input_tokens=10,
            cached_input_tokens=None,
            output_tokens=None,
            reservation=0.2,
        )
        == 0.2
    )
    assert (
        service._actual_cost(  # noqa: SLF001
            input_tokens=100,
            cached_input_tokens=101,
            output_tokens=10,
            reservation=0.3,
        )
        == 0.3
    )
    assert (
        service._actual_cost(  # noqa: SLF001
            input_tokens=100,
            cached_input_tokens=-1,
            output_tokens=10,
            reservation=0.4,
        )
        == 0.4
    )
    assert (
        service._actual_cost(  # noqa: SLF001
            input_tokens=1000,
            cached_input_tokens=400,
            output_tokens=500,
            reservation=0.1,
        )
        == 0.0091
    )

    await service._settle_budget(user_id=USER_ID, reservation=0.1, actual=0.01, failed=False)  # noqa: SLF001
    assert "failed_request_count" not in usage.update_one.await_args.args[1]["$inc"]
    await service._settle_budget(user_id=USER_ID, reservation=0.1, actual=0.1, failed=True)  # noqa: SLF001
    assert usage.update_one.await_args.args[1]["$inc"]["failed_request_count"] == 1

    service.settings.TUTOR_INPUT_COST_PER_MILLION_USD = -1
    service.settings.TUTOR_CACHED_INPUT_COST_PER_MILLION_USD = -1
    service.settings.TUTOR_OUTPUT_COST_PER_MILLION_USD = -1
    assert (
        service._actual_cost(  # noqa: SLF001
            input_tokens=100,
            cached_input_tokens=50,
            output_tokens=100,
            reservation=1,
        )
        == 0
    )


@pytest.mark.asyncio
async def test_store_completed_analysis_and_profile_update() -> None:
    service, analyses, profiles, _usage, provider_obj = _service()
    query = service._analysis_query(user_id=USER_ID, game_id=GAME_ID)  # noqa: SLF001
    provider_result = await provider_obj.generate(
        PreparedTutorRequest(
            payload={},
            reservation_usd=0.1,
            evidence_refs=frozenset(),
            evidence_turns=frozenset(),
        )
    )
    analyses.find_one_and_update.return_value = _analysis_document()

    document = await service._store_completed_analysis(  # noqa: SLF001
        query=query,
        claim_token="claim",
        game_code=GAME_CODE,
        provider_result=provider_result,
        actual_cost=0.01,
    )

    assert document["game_code"] == GAME_CODE
    update = analyses.find_one_and_update.await_args.args[1]
    assert update["$set"]["provider"]["response_id"] == "resp_123"
    assert update["$set"]["provider"]["cached_input_tokens"] == 400
    assert update["$set"]["provider"]["cost_usd"] == 0.01
    assert update["$unset"] == {"claim_token": "", "started_at": ""}

    await service._update_profile(  # noqa: SLF001
        user_id=USER_ID,
        game_id=GAME_ID,
        content=provider_result.review.profile_update,
    )
    profile_update = profiles.update_one.await_args.args[1]
    assert profile_update["$inc"] == {"reviewed_games": 1}
    assert profile_update["$addToSet"] == {"game_ids": GAME_ID}

    analyses.find_one_and_update.return_value = None
    with pytest.raises(TutorServiceError) as exc_info:
        await service._store_completed_analysis(  # noqa: SLF001
            query=query,
            claim_token="claim",
            game_code=GAME_CODE,
            provider_result=provider_result,
            actual_cost=0.01,
        )
    assert exc_info.value.code == "TUTOR_STORAGE_FAILED"


@pytest.mark.asyncio
async def test_generate_rejects_short_games_and_returns_cached_analysis_without_provider_cost() -> None:
    service, analyses, _profiles, _usage, provider_obj = _service(settings=Settings(TUTOR_MIN_COMPLETED_TURNS=5))
    with pytest.raises(TutorServiceError) as exc_info:
        await service.generate(review=make_review(), user_id=USER_ID)
    assert exc_info.value.code == "TUTOR_GAME_TOO_SHORT"
    provider_obj.ensure_available.assert_not_called()

    service.settings.TUTOR_MIN_COMPLETED_TURNS = 4
    analyses.find_one.return_value = _analysis_document()
    response = await service.generate(review=make_review(), user_id=USER_ID)
    assert response.analysis is not None
    assert response.analysis.cached is True
    provider_obj.ensure_available.assert_not_called()
    provider_obj.generate.assert_not_awaited()


@pytest.mark.asyncio
async def test_generate_maps_provider_unavailability_before_claiming_or_spending() -> None:
    service, analyses, _profiles, usage, provider_obj = _service()
    provider_obj.ensure_available.side_effect = TutorProviderError("TUTOR_UNAVAILABLE", "disabled")

    with pytest.raises(TutorServiceError) as exc_info:
        await service.generate(review=make_review(), user_id=USER_ID)

    assert exc_info.value.code == "TUTOR_UNAVAILABLE"
    analyses.insert_one.assert_not_awaited()
    usage.update_one.assert_not_awaited()


@pytest.mark.asyncio
async def test_generate_successfully_reserves_calls_provider_caches_and_updates_profile() -> None:
    service, analyses, profiles, usage, provider_obj = _service()
    analyses.find_one.side_effect = [None]
    completed = _analysis_document()
    analyses.find_one_and_update.return_value = completed
    profiles.find_one.side_effect = [None, {"reviewed_games": 1, "content": sample_model_output().profile_update.model_dump()}]
    usage.find_one.return_value = {"spent_usd": 0.01, "reserved_usd": 0}

    response = await service.generate(review=make_review(), user_id=USER_ID)

    assert response.analysis is not None
    assert response.analysis.cached is False
    assert response.profile.reviewed_games == 1
    provider_obj.ensure_available.assert_called_once_with()
    provider_obj.prepare.assert_called_once()
    provider_obj.generate.assert_awaited_once()
    analyses.insert_one.assert_awaited_once()
    profiles.update_one.assert_awaited_once()
    assert usage.update_one.await_count == 2


@pytest.mark.asyncio
async def test_generate_releases_claim_when_prepare_or_budget_fails() -> None:
    service, analyses, _profiles, _usage, provider_obj = _service()
    provider_obj.prepare.side_effect = RuntimeError("bad settings")

    with pytest.raises(RuntimeError, match="bad settings"):
        await service.generate(review=make_review(), user_id=USER_ID)
    analyses.delete_one.assert_awaited_once()

    service, analyses, _profiles, _usage, _provider_obj = _service()
    service._reserve_budget = AsyncMock(  # type: ignore[method-assign]
        side_effect=TutorServiceError("TUTOR_BUDGET_EXCEEDED", "limit")
    )
    with pytest.raises(TutorServiceError) as exc_info:
        await service.generate(review=make_review(), user_id=USER_ID)
    assert exc_info.value.code == "TUTOR_BUDGET_EXCEEDED"
    analyses.delete_one.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("provider_error", [True, False])
async def test_generate_accounts_for_failures_and_releases_claim(provider_error: bool) -> None:
    service, analyses, _profiles, usage, provider_obj = _service()
    if provider_error:
        provider_obj.generate.side_effect = TutorProviderError("TUTOR_REFUSED", "refused")
        expected_code = "TUTOR_REFUSED"
    else:
        provider_obj.generate.side_effect = RuntimeError("unexpected")
        expected_code = "TUTOR_PROVIDER_FAILED"

    with pytest.raises(TutorServiceError) as exc_info:
        await service.generate(review=make_review(), user_id=USER_ID)

    assert exc_info.value.code == expected_code
    settlement = usage.update_one.await_args_list[-1].args[1]["$inc"]
    assert settlement["spent_usd"] == 0.1
    assert settlement["failed_request_count"] == 1
    analyses.delete_one.assert_awaited_once()


@pytest.mark.asyncio
async def test_generate_returns_analysis_even_if_profile_update_fails() -> None:
    service, analyses, profiles, _usage, _provider_obj = _service()
    analyses.find_one_and_update.return_value = _analysis_document()
    profiles.update_one.side_effect = RuntimeError("profile offline")

    response = await service.generate(review=make_review(), user_id=USER_ID)

    assert response.analysis is not None
    assert response.analysis.cached is False


@pytest.mark.asyncio
async def test_feedback_is_saved_or_rejected_when_analysis_is_missing() -> None:
    service, analyses, _profiles, _usage, _provider_obj = _service()
    analyses.find_one_and_update.return_value = _analysis_document(feedback=True)

    response = await service.submit_feedback(
        game_id=GAME_ID,
        game_code=GAME_CODE,
        user_id=USER_ID,
        feedback=TutorFeedbackRequest(rating="helpful", comment="Useful"),
    )

    assert response.rating == "helpful"
    query = analyses.find_one_and_update.await_args.args[0]
    assert query["game_code"] == GAME_CODE
    assert query["status"] == "completed"

    analyses.find_one_and_update.return_value = None
    with pytest.raises(TutorServiceError) as exc_info:
        await service.submit_feedback(
            game_id=GAME_ID,
            game_code=GAME_CODE,
            user_id=USER_ID,
            feedback=TutorFeedbackRequest(rating="incorrect"),
        )
    assert exc_info.value.code == "TUTOR_ANALYSIS_NOT_FOUND"
