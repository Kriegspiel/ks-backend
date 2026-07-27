from __future__ import annotations

import asyncio
from dataclasses import dataclass
import hashlib
import hmac
import json
import random
from typing import Any

import httpx
from pydantic import ValidationError

from app.config import Settings
from app.models.game import GameReviewResponse, TranscriptMoveItem
from app.models.tutor import TutorModelOutput, TutorProfileResponse


TUTOR_OUTPUT_SCHEMA_NAME = "kriegspiel_tutor_review"
MAX_INCLUDED_PLAYER_TURNS = 48
MAX_ATTEMPTS_PER_TURN = 8
MAX_RATE_LIMIT_RETRIES = 2
QUOTA_ERROR_LABELS = frozenset({"insufficient_quota", "billing_hard_limit_reached", "usage_limit_reached"})


class TutorEvidenceError(ValueError):
    pass


class TutorProviderError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class TutorEvidence:
    game_code: str
    completed_turns: int
    payload: dict[str, Any]


@dataclass(frozen=True)
class PreparedTutorRequest:
    payload: dict[str, Any]
    reservation_usd: float
    evidence_refs: frozenset[str]
    evidence_turns: frozenset[int]


@dataclass(frozen=True)
class TutorProviderResult:
    review: TutorModelOutput
    response_id: str | None
    input_tokens: int | None
    cached_input_tokens: int | None
    output_tokens: int | None


def _short_text(value: object, *, limit: int = 180) -> str:
    return " ".join(str(value or "").split())[:limit]


def _answer_label(move: TranscriptMoveItem) -> str:
    messages = [move.answer.main, move.answer.special, *move.answer.checks]
    return _short_text(" | ".join(message for message in messages if message))


def _attempt_payload(move: TranscriptMoveItem, *, turn: int, attempt: int) -> dict[str, Any]:
    question_type = move.question_type.upper()
    return {
        "ref": f"T{turn}A{attempt}",
        "kind": "ask_any" if question_type == "ASK_ANY" else "move_attempt",
        "move": move.uci,
        "completed_move": move.move_done,
        "capture_announced": bool(move.answer.capture_square or move.answer.captured_piece_announcement),
        "check_announced": bool(move.answer.checks),
        "outcome": _answer_label(move),
    }


def _bounded_attempts(attempts: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    if len(attempts) <= MAX_ATTEMPTS_PER_TURN:
        return attempts, 0
    head = attempts[:3]
    tail = attempts[-(MAX_ATTEMPTS_PER_TURN - len(head)) :]
    return [*head, *tail], len(attempts) - len(head) - len(tail)


def _player_turns(moves: list[TranscriptMoveItem], *, color: str) -> list[dict[str, Any]]:
    turns: list[dict[str, Any]] = []
    current: list[dict[str, Any]] = []
    turn_number = 1
    for move in moves:
        if move.color != color:
            continue
        current.append(_attempt_payload(move, turn=turn_number, attempt=len(current) + 1))
        if move.move_done:
            bounded, omitted = _bounded_attempts(current)
            turns.append(
                {
                    "turn": turn_number,
                    "attempts": bounded,
                    "omitted_attempts": omitted,
                    "attempt_count": len(current),
                    "completed": True,
                }
            )
            current = []
            turn_number += 1
    if current:
        bounded, omitted = _bounded_attempts(current)
        turns.append(
            {
                "turn": turn_number,
                "attempts": bounded,
                "omitted_attempts": omitted,
                "attempt_count": len(current),
                "completed": False,
            }
        )
    return turns


def _sample_player_turns(turns: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], bool]:
    if len(turns) <= MAX_INCLUDED_PLAYER_TURNS:
        return turns, False
    edge_turns = [*turns[:12], *turns[-12:]]
    edge_numbers = {int(turn["turn"]) for turn in edge_turns}
    important = sorted(
        (turn for turn in turns if int(turn["turn"]) not in edge_numbers),
        key=lambda turn: (
            -int(turn["attempt_count"]),
            -int(any(item["capture_announced"] or item["check_announced"] for item in turn["attempts"])),
            int(turn["turn"]),
        ),
    )[:24]
    selected_turns = {int(turn["turn"]) for turn in [*edge_turns, *important]}
    sampled = [turn for turn in turns if int(turn["turn"]) in selected_turns]
    return sampled[:MAX_INCLUDED_PLAYER_TURNS], True


def _result_for_player(result: dict[str, Any] | None, *, color: str) -> str:
    winner = (result or {}).get("winner")
    if winner is None:
        return "draw"
    return "win" if winner == color else "loss"


def _opponent_type(review: GameReviewResponse, *, color: str) -> str:
    opponent = review.game.black if color == "white" else review.game.white
    return opponent.role if opponent is not None else "unknown"


def build_tutor_evidence(review: GameReviewResponse) -> TutorEvidence:
    if review.game.state != "completed":
        raise TutorEvidenceError("Tutor analysis is available only after a game is completed.")
    color = review.transcript.viewer_color
    if color is None:
        raise TutorEvidenceError("Tutor analysis is available only for your own completed games.")

    successful_moves = [move for move in review.transcript.moves if move.move_done]
    completed_turns = len(successful_moves) // 2
    player_turns = _player_turns(review.transcript.moves, color=color)
    sampled_turns, turns_sampled = _sample_player_turns(player_turns)
    own_moves = [move for move in review.transcript.moves if move.color == color]
    move_attempts = [move for move in own_moves if move.question_type.upper() != "ASK_ANY"]
    completed_player_moves = [move for move in move_attempts if move.move_done]
    completed_turn_rows = [turn for turn in player_turns if turn["completed"]]
    first_try_successes = sum(
        1
        for turn in completed_turn_rows
        if next((item for item in turn["attempts"] if item["kind"] == "move_attempt"), {}).get("completed_move")
    )
    ask_any = [move for move in own_moves if move.question_type.upper() == "ASK_ANY"]
    ask_any_yes = sum(1 for move in ask_any if "yes" in move.answer.main.lower())
    captures = sum(
        1
        for move in completed_player_moves
        if move.answer.capture_square or move.answer.captured_piece_announcement
    )
    checks = sum(1 for move in completed_player_moves if move.answer.checks)
    completed_count = len(completed_player_moves)
    game_code = review.game.game_code

    metrics = {
        "completed_game_turns": completed_turns,
        "player_completed_moves": completed_count,
        "move_attempts": len(move_attempts),
        "unsuccessful_move_attempts": sum(1 for move in move_attempts if not move.move_done),
        "first_attempt_success_rate": round(first_try_successes / completed_count, 3) if completed_count else 0.0,
        "average_attempts_per_completed_move": round(len(move_attempts) / completed_count, 3) if completed_count else 0.0,
        "longest_attempt_sequence": max((int(turn["attempt_count"]) for turn in player_turns), default=0),
        "ask_any_questions": len(ask_any),
        "ask_any_positive_answers": ask_any_yes,
        "captures_announced": captures,
        "checks_announced": checks,
    }
    payload = {
        "ruleset": review.game.rule_variant,
        "player_color": color,
        "opponent_type": _opponent_type(review, color=color),
        "result": _result_for_player(review.game.result, color=color),
        "result_reason": _short_text((review.game.result or {}).get("reason")),
        "metrics": metrics,
        "player_turns": sampled_turns,
        "turns_sampled": turns_sampled,
        "evidence_policy": (
            "Player move attempts and public referee outcomes only. Opponent move squares and all replay FENs are omitted."
        ),
    }
    return TutorEvidence(game_code=game_code, completed_turns=completed_turns, payload=payload)


def _stable_safety_identifier(*, user_id: str, secret_key: str) -> str:
    digest = hmac.new(secret_key.encode("utf-8"), user_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"ks_tutor_{digest[:32]}"


def _reasoning_effort(value: str) -> str:
    normalized = value.strip().lower()
    return normalized if normalized in {"none", "low", "medium", "high", "xhigh", "max"} else "medium"


class OpenAITutorProvider:
    INSTRUCTIONS = (
        "You are Kriegspiel Tutor, a precise training coach. Use only the supplied player-perspective metrics, "
        "move attempts, public referee outcomes, and prior Tutor profile. Never infer or name hidden opponent "
        "piece squares. Never claim an objectively best move, missed tactic, or certain hidden position unless the "
        "evidence explicitly proves it. Treat failed move attempts as information-gathering probes, not ordinary "
        "chess blunders. Distinguish evidence from inference, state uncertainty, and prefer concrete habits the "
        "player can test in the next game. Compare against prior progress only when the profile contains reviewed "
        "games. Keep the profile cumulative but revise it when the current evidence conflicts with earlier patterns. "
        "Every key_moment.evidence value must exactly match a supplied attempt ref, and each key moment must cite at "
        "least one ref from its stated turn."
    )

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def ensure_available(self) -> None:
        if not self.settings.OPENAI_API_KEY:
            raise TutorProviderError("TUTOR_UNAVAILABLE", "Tutor generation is temporarily unavailable.")
        if self.settings.TUTOR_INPUT_COST_PER_MILLION_USD <= 0:
            raise TutorProviderError("TUTOR_UNAVAILABLE", "Tutor generation is temporarily unavailable.")
        if self.settings.TUTOR_CACHED_INPUT_COST_PER_MILLION_USD <= 0:
            raise TutorProviderError("TUTOR_UNAVAILABLE", "Tutor generation is temporarily unavailable.")
        if self.settings.TUTOR_OUTPUT_COST_PER_MILLION_USD <= 0:
            raise TutorProviderError("TUTOR_UNAVAILABLE", "Tutor generation is temporarily unavailable.")

    def prepare(
        self,
        *,
        evidence: TutorEvidence,
        profile: TutorProfileResponse,
        user_id: str,
    ) -> PreparedTutorRequest:
        model_input = json.dumps(
            {
                "game_evidence": evidence.payload,
                "prior_profile": profile.model_dump(mode="json"),
            },
            separators=(",", ":"),
            ensure_ascii=True,
        )
        payload = {
            "model": self.settings.TUTOR_ANALYSIS_MODEL,
            "instructions": self.INSTRUCTIONS,
            "input": model_input,
            "reasoning": {"effort": _reasoning_effort(self.settings.TUTOR_REASONING_EFFORT)},
            "text": {
                "verbosity": "medium",
                "format": {
                    "type": "json_schema",
                    "name": TUTOR_OUTPUT_SCHEMA_NAME,
                    "strict": True,
                    "schema": TutorModelOutput.model_json_schema(),
                },
            },
            "max_output_tokens": max(256, int(self.settings.TUTOR_MAX_OUTPUT_TOKENS)),
            "store": False,
            "safety_identifier": _stable_safety_identifier(user_id=user_id, secret_key=self.settings.SECRET_KEY),
            "metadata": {
                "feature": "kriegspiel_tutor",
                "analysis_version": self.settings.TUTOR_ANALYSIS_VERSION,
                "prompt_version": self.settings.TUTOR_PROMPT_VERSION,
            },
        }
        request_bytes = len(json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8"))
        max_input_cost = request_bytes * max(0.0, self.settings.TUTOR_INPUT_COST_PER_MILLION_USD) / 1_000_000
        max_output_cost = (
            payload["max_output_tokens"]
            * max(0.0, self.settings.TUTOR_OUTPUT_COST_PER_MILLION_USD)
            / 1_000_000
        )
        reservation = round((max_input_cost + max_output_cost) * 1.05, 6)
        evidence_refs = frozenset(
            str(attempt["ref"])
            for turn in evidence.payload["player_turns"]
            for attempt in turn["attempts"]
        )
        evidence_turns = frozenset(int(turn["turn"]) for turn in evidence.payload["player_turns"])
        return PreparedTutorRequest(
            payload=payload,
            reservation_usd=max(reservation, 0.000001),
            evidence_refs=evidence_refs,
            evidence_turns=evidence_turns,
        )

    async def _post(self, payload: dict[str, Any]) -> httpx.Response:
        base_url = self.settings.OPENAI_BASE_URL.rstrip("/")
        timeout = max(1.0, float(self.settings.TUTOR_OPENAI_TIMEOUT_SECONDS))
        async with httpx.AsyncClient(timeout=timeout) as client:
            return await client.post(
                f"{base_url}/responses",
                headers={
                    "Authorization": f"Bearer {self.settings.OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )

    @staticmethod
    def _provider_error_labels(response: httpx.Response) -> frozenset[str]:
        try:
            payload = response.json()
        except (TypeError, ValueError):
            return frozenset()
        if not isinstance(payload, dict) or not isinstance(payload.get("error"), dict):
            return frozenset()
        error = payload["error"]
        return frozenset(
            value.strip().lower()
            for key in ("code", "type")
            if isinstance((value := error.get(key)), str) and value.strip()
        )

    @classmethod
    def _http_status_error(cls, response: httpx.Response) -> TutorProviderError:
        if response.status_code == 429:
            if cls._provider_error_labels(response) & QUOTA_ERROR_LABELS:
                return TutorProviderError(
                    "TUTOR_PROVIDER_QUOTA",
                    "Tutor's model quota is unavailable. No review was generated.",
                )
            return TutorProviderError(
                "TUTOR_PROVIDER_RATE_LIMITED",
                "Tutor is temporarily rate-limited. Please wait one minute before trying again.",
            )
        if response.status_code in {401, 403}:
            return TutorProviderError(
                "TUTOR_PROVIDER_AUTH",
                "Tutor's model access is unavailable. No review was generated.",
            )
        return TutorProviderError("TUTOR_PROVIDER_FAILED", "Tutor generation failed.")

    async def _post_with_rate_limit_backoff(self, payload: dict[str, Any]) -> httpx.Response:
        retry = 0
        while True:
            response = await self._post(payload)
            if response.status_code != 429:
                return response
            error = self._http_status_error(response)
            if error.code == "TUTOR_PROVIDER_QUOTA" or retry == MAX_RATE_LIMIT_RETRIES:
                raise error
            await asyncio.sleep((2**retry) + random.uniform(0.0, 0.25))
            retry += 1

    @staticmethod
    def _response_text(payload: dict[str, Any]) -> str:
        direct = payload.get("output_text")
        if isinstance(direct, str) and direct.strip():
            return direct
        for output in payload.get("output") if isinstance(payload.get("output"), list) else []:
            if not isinstance(output, dict) or output.get("type") != "message":
                continue
            for item in output.get("content") if isinstance(output.get("content"), list) else []:
                if not isinstance(item, dict):
                    continue
                refusal = item.get("refusal")
                if isinstance(refusal, str) and refusal.strip():
                    raise TutorProviderError("TUTOR_REFUSED", "Tutor could not analyze this game.")
                text = item.get("text")
                if item.get("type") == "output_text" and isinstance(text, str) and text.strip():
                    return text
        raise TutorProviderError("TUTOR_PROVIDER_INVALID_RESPONSE", "Tutor returned an incomplete response.")

    @staticmethod
    def _usage_tokens(value: object) -> int | None:
        return value if type(value) is int and value >= 0 else None

    @staticmethod
    def _validate_evidence_refs(review: TutorModelOutput, request: PreparedTutorRequest) -> None:
        for moment in review.key_moments:
            if moment.turn not in request.evidence_turns:
                raise TutorProviderError(
                    "TUTOR_PROVIDER_INVALID_RESPONSE",
                    "Tutor returned coaching that was not grounded in the supplied evidence.",
                )
            if any(reference not in request.evidence_refs for reference in moment.evidence):
                raise TutorProviderError(
                    "TUTOR_PROVIDER_INVALID_RESPONSE",
                    "Tutor returned coaching that was not grounded in the supplied evidence.",
                )
            turn_prefix = f"T{moment.turn}A"
            if not any(reference.startswith(turn_prefix) for reference in moment.evidence):
                raise TutorProviderError(
                    "TUTOR_PROVIDER_INVALID_RESPONSE",
                    "Tutor returned coaching that was not grounded in the supplied evidence.",
                )

    async def generate(self, request: PreparedTutorRequest) -> TutorProviderResult:
        self.ensure_available()
        try:
            response = await self._post_with_rate_limit_backoff(request.payload)
            response.raise_for_status()
        except TutorProviderError:
            raise
        except httpx.HTTPStatusError as exc:
            raise self._http_status_error(exc.response) from exc
        except httpx.HTTPError as exc:
            raise TutorProviderError("TUTOR_PROVIDER_FAILED", "Tutor generation failed.") from exc

        try:
            payload = response.json()
        except (TypeError, ValueError) as exc:
            raise TutorProviderError("TUTOR_PROVIDER_INVALID_RESPONSE", "Tutor returned invalid coaching data.") from exc
        if not isinstance(payload, dict):
            raise TutorProviderError("TUTOR_PROVIDER_INVALID_RESPONSE", "Tutor returned invalid coaching data.")
        if payload.get("status") not in {None, "completed"}:
            raise TutorProviderError("TUTOR_PROVIDER_INCOMPLETE", "Tutor generation did not complete.")
        try:
            review = TutorModelOutput.model_validate_json(self._response_text(payload))
        except (ValidationError, ValueError, TypeError) as exc:
            raise TutorProviderError("TUTOR_PROVIDER_INVALID_RESPONSE", "Tutor returned invalid coaching data.") from exc
        self._validate_evidence_refs(review, request)
        usage = payload.get("usage") if isinstance(payload.get("usage"), dict) else {}
        input_tokens = self._usage_tokens(usage.get("input_tokens"))
        input_details = usage.get("input_tokens_details") if isinstance(usage.get("input_tokens_details"), dict) else {}
        cached_input_tokens = self._usage_tokens(input_details.get("cached_tokens"))
        output_tokens = self._usage_tokens(usage.get("output_tokens"))
        response_id = payload.get("id") if isinstance(payload.get("id"), str) else None
        return TutorProviderResult(
            review=review,
            response_id=response_id,
            input_tokens=input_tokens,
            cached_input_tokens=cached_input_tokens,
            output_tokens=output_tokens,
        )
