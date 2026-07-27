from __future__ import annotations

import json
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from app.config import Settings
from app.models.game import GameReviewResponse
from app.services.tutor_analysis import (
    MAX_ATTEMPTS_PER_TURN,
    MAX_INCLUDED_PLAYER_TURNS,
    OpenAITutorProvider,
    PreparedTutorRequest,
    TutorEvidenceError,
    TutorProviderError,
    _reasoning_effort,
    _stable_safety_identifier,
    build_tutor_evidence,
)
from tests.tutor_helpers import USER_ID, empty_profile, make_review, sample_model_output


def _mutate_review(review: GameReviewResponse, mutate) -> GameReviewResponse:  # noqa: ANN001
    payload = review.model_dump(mode="python")
    mutate(payload)
    return GameReviewResponse.model_validate(payload)


def test_build_tutor_evidence_rejects_active_and_nonparticipant_reviews() -> None:
    with pytest.raises(TutorEvidenceError, match="only after"):
        build_tutor_evidence(make_review(completed=False))

    with pytest.raises(TutorEvidenceError, match="your own"):
        build_tutor_evidence(make_review(participant=False))


def test_build_tutor_evidence_is_player_safe_and_computes_metrics() -> None:
    review = _mutate_review(
        make_review(extra_attempts=2),
        lambda payload: payload["transcript"]["moves"][2]["answer"].update(
            {"captured_piece_announcement": "PAWN", "special": "special public answer"}
        ),
    )

    evidence = build_tutor_evidence(review)
    serialized = json.dumps(evidence.payload)
    metrics = evidence.payload["metrics"]

    assert evidence.game_code == "ABC234"
    assert evidence.completed_turns == 4
    assert evidence.payload["player_color"] == "white"
    assert evidence.payload["opponent_type"] == "bot"
    assert evidence.payload["result"] == "win"
    assert evidence.payload["result_reason"] == "checkmate"
    assert metrics == {
        "completed_game_turns": 4,
        "player_completed_moves": 4,
        "move_attempts": 7,
        "unsuccessful_move_attempts": 3,
        "first_attempt_success_rate": 0.75,
        "average_attempts_per_completed_move": 1.75,
        "longest_attempt_sequence": 5,
        "ask_any_questions": 1,
        "ask_any_positive_answers": 1,
        "captures_announced": 1,
        "checks_announced": 1,
    }
    assert "hidden-full" not in serialized
    assert "replay_fen" not in serialized
    assert "e7e5" not in serialized
    assert "e2e4" in serialized
    assert evidence.payload["player_turns"][0]["attempts"][0]["kind"] == "ask_any"
    assert "special public answer" in serialized


@pytest.mark.parametrize(
    ("winner", "expected"),
    [(None, "draw"), ("black", "loss")],
)
def test_build_tutor_evidence_describes_draws_and_losses(winner: str | None, expected: str) -> None:
    evidence = build_tutor_evidence(make_review(winner=winner, opponent_role="user"))

    assert evidence.payload["result"] == expected
    assert evidence.payload["opponent_type"] == "user"


def test_build_tutor_evidence_handles_black_player_unknown_opponent_and_no_completed_moves() -> None:
    def mutate(payload: dict) -> None:
        payload["game"]["white"] = None
        payload["game"]["result"] = None
        payload["transcript"]["viewer_color"] = "black"
        payload["transcript"]["moves"] = [
            {
                "ply": 1,
                "color": "black",
                "question_type": "ASK_ANY",
                "uci": None,
                "answer": {"main": "No"},
                "move_done": False,
            }
        ]

    evidence = build_tutor_evidence(_mutate_review(make_review(), mutate))

    assert evidence.completed_turns == 0
    assert evidence.payload["opponent_type"] == "unknown"
    assert evidence.payload["result"] == "draw"
    assert evidence.payload["result_reason"] == ""
    assert evidence.payload["metrics"]["first_attempt_success_rate"] == 0.0
    assert evidence.payload["metrics"]["average_attempts_per_completed_move"] == 0.0
    assert evidence.payload["metrics"]["longest_attempt_sequence"] == 1
    assert evidence.payload["metrics"]["ask_any_positive_answers"] == 0
    assert evidence.payload["player_turns"][0]["completed"] is False


def test_build_tutor_evidence_bounds_attempts_and_long_games() -> None:
    evidence = build_tutor_evidence(make_review(turns=60, extra_attempts=12))
    turns = evidence.payload["player_turns"]

    assert evidence.payload["turns_sampled"] is True
    assert len(turns) == MAX_INCLUDED_PLAYER_TURNS
    first = next(turn for turn in turns if turn["turn"] == 1)
    assert len(first["attempts"]) == MAX_ATTEMPTS_PER_TURN
    assert first["omitted_attempts"] == 7
    assert first["attempt_count"] == 15
    assert turns[-1]["turn"] == 60


def test_reasoning_and_safety_helpers_are_stable() -> None:
    assert _reasoning_effort(" HIGH ") == "high"
    assert _reasoning_effort("unexpected") == "medium"
    identifier = _stable_safety_identifier(user_id=USER_ID, secret_key="secret")

    assert identifier == _stable_safety_identifier(user_id=USER_ID, secret_key="secret")
    assert USER_ID not in identifier
    assert identifier.startswith("ks_tutor_")


def test_provider_requires_key_and_prepares_strict_bounded_request() -> None:
    unavailable = OpenAITutorProvider(Settings(OPENAI_API_KEY=None))
    with pytest.raises(TutorProviderError) as exc_info:
        unavailable.ensure_available()
    assert exc_info.value.code == "TUTOR_UNAVAILABLE"

    invalid_input_price = OpenAITutorProvider(
        Settings(OPENAI_API_KEY="test-key", TUTOR_INPUT_COST_PER_MILLION_USD=0)
    )
    with pytest.raises(TutorProviderError) as exc_info:
        invalid_input_price.ensure_available()
    assert exc_info.value.code == "TUTOR_UNAVAILABLE"

    invalid_output_price = OpenAITutorProvider(
        Settings(OPENAI_API_KEY="test-key", TUTOR_OUTPUT_COST_PER_MILLION_USD=0)
    )
    with pytest.raises(TutorProviderError) as exc_info:
        invalid_output_price.ensure_available()
    assert exc_info.value.code == "TUTOR_UNAVAILABLE"

    invalid_cached_input_price = OpenAITutorProvider(
        Settings(OPENAI_API_KEY="test-key", TUTOR_CACHED_INPUT_COST_PER_MILLION_USD=0)
    )
    with pytest.raises(TutorProviderError) as exc_info:
        invalid_cached_input_price.ensure_available()
    assert exc_info.value.code == "TUTOR_UNAVAILABLE"

    settings = Settings(
        OPENAI_API_KEY="test-key",
        TUTOR_REASONING_EFFORT="invalid",
        TUTOR_MAX_OUTPUT_TOKENS=10,
    )
    provider = OpenAITutorProvider(settings)
    provider.ensure_available()
    request = provider.prepare(
        evidence=build_tutor_evidence(make_review()),
        profile=empty_profile(),
        user_id=USER_ID,
    )

    assert request.payload["model"] == "gpt-5.6-terra"
    assert request.payload["reasoning"] == {"effort": "medium"}
    assert request.payload["max_output_tokens"] == 256
    assert request.payload["store"] is False
    assert request.payload["text"]["format"]["strict"] is True
    assert request.payload["text"]["format"]["schema"]["additionalProperties"] is False
    assert request.payload["metadata"]["prompt_version"] == "tutor-private-beta-v1"
    assert request.payload["safety_identifier"].startswith("ks_tutor_")
    assert request.reservation_usd > 0
    assert request.evidence_turns == frozenset({1, 2, 3, 4})
    assert {"T1A1", "T1A2"}.issubset(request.evidence_refs)

    defensive_request = OpenAITutorProvider(
        Settings(
            OPENAI_API_KEY="test-key",
            TUTOR_INPUT_COST_PER_MILLION_USD=-1,
            TUTOR_OUTPUT_COST_PER_MILLION_USD=-1,
        )
    ).prepare(
        evidence=build_tutor_evidence(make_review()),
        profile=empty_profile(),
        user_id=USER_ID,
    )
    assert defensive_request.reservation_usd == 0.000001

    normal_request = OpenAITutorProvider(Settings(OPENAI_API_KEY="test-key")).prepare(
        evidence=build_tutor_evidence(make_review()),
        profile=empty_profile(reviewed_games=5),
        user_id=USER_ID,
    )
    assert normal_request.payload["max_output_tokens"] == 2400
    assert normal_request.reservation_usd > 0.03
    assert json.loads(normal_request.payload["input"])["prior_profile"]["ready"] is True


@pytest.mark.asyncio
async def test_provider_post_uses_configured_endpoint_headers_and_minimum_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = Mock()

    class FakeAsyncClient:
        def __init__(self) -> None:
            self.post = AsyncMock(return_value=response)

        async def __aenter__(self):  # noqa: ANN201
            return self

        async def __aexit__(self, *_args):  # noqa: ANN002, ANN201
            return None

    client = FakeAsyncClient()
    factory = Mock(return_value=client)
    monkeypatch.setattr(httpx, "AsyncClient", factory)
    provider = OpenAITutorProvider(
        Settings(
            OPENAI_API_KEY="test-key",
            OPENAI_BASE_URL="https://openai.example/v1/",
            TUTOR_OPENAI_TIMEOUT_SECONDS=0,
        )
    )

    assert await provider._post({"hello": "world"}) is response  # noqa: SLF001
    factory.assert_called_once_with(timeout=1.0)
    client.post.assert_awaited_once_with(
        "https://openai.example/v1/responses",
        headers={"Authorization": "Bearer test-key", "Content-Type": "application/json"},
        json={"hello": "world"},
    )


def test_provider_response_text_supports_direct_nested_and_refusal_payloads() -> None:
    review_json = sample_model_output().model_dump_json()

    assert OpenAITutorProvider._response_text({"output_text": review_json}) == review_json  # noqa: SLF001
    assert (
        OpenAITutorProvider._response_text(  # noqa: SLF001
            {
                "output_text": " ",
                "output": [
                    "skip",
                    {"type": "tool_call"},
                    {"type": "message", "content": "skip"},
                    {
                        "type": "message",
                        "content": ["skip", {"type": "other"}, {"type": "output_text", "text": review_json}],
                    },
                ],
            }
        )
        == review_json
    )

    with pytest.raises(TutorProviderError) as exc_info:
        OpenAITutorProvider._response_text(  # noqa: SLF001
            {"output": [{"type": "message", "content": [{"type": "refusal", "refusal": "policy"}]}]}
        )
    assert exc_info.value.code == "TUTOR_REFUSED"

    with pytest.raises(TutorProviderError) as exc_info:
        OpenAITutorProvider._response_text({"output": None})  # noqa: SLF001
    assert exc_info.value.code == "TUTOR_PROVIDER_INVALID_RESPONSE"


def _provider_response(payload: object, *, json_error: Exception | None = None) -> Mock:
    response = Mock()
    response.raise_for_status = Mock()
    response.json = Mock(side_effect=json_error) if json_error else Mock(return_value=payload)
    return response


def _prepared_request(
    *,
    payload: dict | None = None,
    evidence_refs: frozenset[str] = frozenset({"T1A1", "T1A2"}),
    evidence_turns: frozenset[int] = frozenset({1}),
) -> PreparedTutorRequest:
    return PreparedTutorRequest(
        payload=payload or {},
        reservation_usd=0.1,
        evidence_refs=evidence_refs,
        evidence_turns=evidence_turns,
    )


@pytest.mark.asyncio
async def test_provider_generate_validates_output_and_usage() -> None:
    provider = OpenAITutorProvider(Settings(OPENAI_API_KEY="test-key"))
    payload = {
        "id": "resp_123",
        "status": "completed",
        "output_text": sample_model_output().model_dump_json(),
        "usage": {
            "input_tokens": 321,
            "input_tokens_details": {"cached_tokens": 120},
            "output_tokens": 654,
        },
    }
    provider._post = AsyncMock(return_value=_provider_response(payload))  # type: ignore[method-assign]

    result = await provider.generate(_prepared_request(payload={"request": True}))

    assert result.review == sample_model_output()
    assert result.response_id == "resp_123"
    assert result.input_tokens == 321
    assert result.cached_input_tokens == 120
    assert result.output_tokens == 654


@pytest.mark.asyncio
async def test_provider_generate_accepts_missing_optional_provider_metadata() -> None:
    provider = OpenAITutorProvider(Settings(OPENAI_API_KEY="test-key"))
    provider._post = AsyncMock(  # type: ignore[method-assign]
        return_value=_provider_response({"output_text": sample_model_output().model_dump_json(), "usage": "unknown"})
    )

    result = await provider.generate(_prepared_request())

    assert result.response_id is None
    assert result.input_tokens is None
    assert result.cached_input_tokens is None
    assert result.output_tokens is None

    assert provider._usage_tokens(True) is None  # noqa: SLF001
    assert provider._usage_tokens(-1) is None  # noqa: SLF001
    assert provider._usage_tokens(0) == 0  # noqa: SLF001


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("turn", "evidence", "evidence_refs", "evidence_turns"),
    [
        (99, ["T1A1"], frozenset({"T1A1"}), frozenset({1})),
        (1, ["T9A1"], frozenset({"T1A1"}), frozenset({1})),
        (2, ["T1A1"], frozenset({"T1A1", "T2A1"}), frozenset({1, 2})),
    ],
)
async def test_provider_rejects_key_moments_not_grounded_in_supplied_evidence(
    turn: int,
    evidence: list[str],
    evidence_refs: frozenset[str],
    evidence_turns: frozenset[int],
) -> None:
    provider = OpenAITutorProvider(Settings(OPENAI_API_KEY="test-key"))
    output = sample_model_output().model_dump(mode="json")
    output["key_moments"][0]["turn"] = turn
    output["key_moments"][0]["evidence"] = evidence
    provider._post = AsyncMock(  # type: ignore[method-assign]
        return_value=_provider_response({"output_text": json.dumps(output)})
    )

    with pytest.raises(TutorProviderError) as exc_info:
        await provider.generate(
            _prepared_request(evidence_refs=evidence_refs, evidence_turns=evidence_turns)
        )

    assert exc_info.value.code == "TUTOR_PROVIDER_INVALID_RESPONSE"
    assert "not grounded" in str(exc_info.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "expected_code"),
    [
        (["not", "a", "mapping"], "TUTOR_PROVIDER_INVALID_RESPONSE"),
        ({"status": "incomplete"}, "TUTOR_PROVIDER_INCOMPLETE"),
        ({"output_text": "not-json"}, "TUTOR_PROVIDER_INVALID_RESPONSE"),
        ({"output": []}, "TUTOR_PROVIDER_INVALID_RESPONSE"),
    ],
)
async def test_provider_generate_rejects_invalid_responses(payload: object, expected_code: str) -> None:
    provider = OpenAITutorProvider(Settings(OPENAI_API_KEY="test-key"))
    provider._post = AsyncMock(return_value=_provider_response(payload))  # type: ignore[method-assign]

    with pytest.raises(TutorProviderError) as exc_info:
        await provider.generate(_prepared_request())

    assert exc_info.value.code == expected_code


@pytest.mark.asyncio
async def test_provider_generate_maps_http_and_json_errors() -> None:
    provider = OpenAITutorProvider(Settings(OPENAI_API_KEY="test-key"))
    http_response = _provider_response({})
    http_response.raise_for_status.side_effect = httpx.HTTPError("offline")
    provider._post = AsyncMock(return_value=http_response)  # type: ignore[method-assign]

    with pytest.raises(TutorProviderError) as exc_info:
        await provider.generate(_prepared_request())
    assert exc_info.value.code == "TUTOR_PROVIDER_FAILED"

    provider._post = AsyncMock(return_value=_provider_response({}, json_error=ValueError("bad json")))  # type: ignore[method-assign]
    with pytest.raises(TutorProviderError) as exc_info:
        await provider.generate(_prepared_request())
    assert exc_info.value.code == "TUTOR_PROVIDER_INVALID_RESPONSE"
