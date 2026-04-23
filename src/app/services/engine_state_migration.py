from __future__ import annotations

from typing import Any

import chess
from kriegspiel.move import KriegspielAnswer, KriegspielMove, KriegspielScoresheet, MainAnnouncement, QuestionAnnouncement
from kriegspiel.serialization import MalformedDataError

from app.services.engine_adapter import (
    create_new_game,
    deserialize_game_state,
    deserialize_scoresheet,
    is_current_canonical_engine_state,
    serialize_game_state,
)
from app.services.state_projection import reconstruct_scoresheets_from_moves


def classify_engine_state(payload: Any) -> str:
    if payload is None:
        return "none"
    if isinstance(payload, dict) and isinstance(payload.get("game_state"), dict):
        return f"canonical:{payload.get('schema_version', 'unknown')}"
    if isinstance(payload, dict):
        return f"legacy:{payload.get('schema_version', 'unknown')}"
    return type(payload).__name__


def canonicalize_game_document(game: dict[str, Any]) -> dict[str, Any] | None:
    current = game.get("engine_state")
    if is_current_canonical_engine_state(current):
        return None

    engine = _load_engine_for_migration(game)
    _hydrate_scoresheets_from_moves_if_needed(game=game, engine=engine, original_engine_state=current)
    canonical = _validated_canonical_payload(engine=engine, game=game)
    return canonical


def _load_engine_for_migration(game: dict[str, Any]) -> Any:
    state = game.get("engine_state")
    if isinstance(state, dict):
        return deserialize_game_state(state)
    return create_new_game(any_rule=game.get("rule_variant", "berkeley_any") == "berkeley_any")


def _hydrate_scoresheets_from_moves_if_needed(*, game: dict[str, Any], engine: Any, original_engine_state: Any) -> None:
    if _legacy_engine_state_has_scoresheets(original_engine_state):
        return

    moves = game.get("moves")
    if not isinstance(moves, list) or not moves:
        return

    reconstructed = reconstruct_scoresheets_from_moves(moves)
    engine._whites_scoresheet = deserialize_scoresheet(reconstructed["white"], fallback_color="white")  # noqa: SLF001
    engine._blacks_scoresheet = deserialize_scoresheet(reconstructed["black"], fallback_color="black")  # noqa: SLF001


def _legacy_engine_state_has_scoresheets(payload: Any) -> bool:
    return (
        isinstance(payload, dict)
        and isinstance(payload.get("white_scoresheet"), dict)
        and isinstance(payload.get("black_scoresheet"), dict)
    )


def _validated_canonical_payload(*, engine: Any, game: dict[str, Any]) -> dict[str, Any]:
    canonical = serialize_game_state(engine)
    try:
        deserialize_game_state(canonical)
        return canonical
    except MalformedDataError as exc:
        if "Scoresheet-derived moves do not match move_stack" not in str(exc):
            raise

    # Some older documents have transcripts that drifted from their scoresheet snapshots.
    # Fall back to a minimal move_stack-derived scoresheet so canonical engine_state remains loadable.
    _synthesize_scoresheets_from_move_stack(engine)
    canonical = serialize_game_state(engine)
    deserialize_game_state(canonical)
    return canonical


def _synthesize_scoresheets_from_move_stack(engine: Any) -> None:
    white = KriegspielScoresheet(chess.WHITE)
    black = KriegspielScoresheet(chess.BLACK)

    for ply, chess_move in enumerate(getattr(engine._board, "move_stack", []), start=1):  # noqa: SLF001
        move = KriegspielMove(QuestionAnnouncement.COMMON, chess_move)
        answer = KriegspielAnswer(MainAnnouncement.REGULAR_MOVE)
        if ply % 2 == 1:
            white.record_move_own(move, answer)
            black.record_move_opponent(QuestionAnnouncement.COMMON, answer)
        else:
            black.record_move_own(move, answer)
            white.record_move_opponent(QuestionAnnouncement.COMMON, answer)

    engine._whites_scoresheet = white  # noqa: SLF001
    engine._blacks_scoresheet = black  # noqa: SLF001
