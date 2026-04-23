from __future__ import annotations

import copy
from typing import Any

import chess
from kriegspiel import __version__ as KRIEGSPIEL_LIBRARY_VERSION
from kriegspiel.move import KriegspielAnswer, KriegspielMove, KriegspielScoresheet, MainAnnouncement, QuestionAnnouncement
from kriegspiel.serialization import MalformedDataError

from app.services.engine_adapter import (
    CANONICAL_ENGINE_STATE_SCHEMA_VERSION,
    PREVIOUS_CANONICAL_ENGINE_STATE_SCHEMA_VERSION,
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

    upgraded = _upgrade_previous_canonical_engine_state(current)
    if upgraded is not None:
        return upgraded

    engine = _load_engine_for_migration(game)
    _hydrate_scoresheets_from_moves_if_needed(game=game, engine=engine, original_engine_state=current)
    canonical = _validated_canonical_payload(engine=engine, game=game)
    return canonical


def build_engine_state_migration_update(game: dict[str, Any]) -> dict[str, Any] | None:
    current = game.get("engine_state")
    if is_current_canonical_engine_state(current):
        return None

    patch = _previous_canonical_engine_state_patch(current)
    if patch is not None:
        return patch

    canonical = canonicalize_game_document(game)
    if canonical is None:
        return None
    return {"engine_state": canonical}


def _previous_canonical_engine_state_patch(payload: Any) -> dict[str, Any] | None:
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != PREVIOUS_CANONICAL_ENGINE_STATE_SCHEMA_VERSION
        or not isinstance(payload.get("game_state"), dict)
    ):
        return None

    any_rule = bool(payload["game_state"].get("any_rule", True))
    return {
        "engine_state.schema_version": CANONICAL_ENGINE_STATE_SCHEMA_VERSION,
        "engine_state.library_version": KRIEGSPIEL_LIBRARY_VERSION,
        "engine_state.game_state.ruleset_id": "berkeley_any" if any_rule else "berkeley",
    }


def _upgrade_previous_canonical_engine_state(payload: Any) -> dict[str, Any] | None:
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != PREVIOUS_CANONICAL_ENGINE_STATE_SCHEMA_VERSION
        or not isinstance(payload.get("game_state"), dict)
    ):
        return None

    _validate_previous_canonical_engine_state(payload)
    upgraded = copy.deepcopy(payload)
    upgraded["schema_version"] = CANONICAL_ENGINE_STATE_SCHEMA_VERSION
    upgraded["library_version"] = KRIEGSPIEL_LIBRARY_VERSION

    game_state = upgraded["game_state"]
    any_rule = bool(game_state.get("any_rule", True))
    game_state.setdefault("ruleset_id", "berkeley_any" if any_rule else "berkeley")
    return upgraded


def _validate_previous_canonical_engine_state(payload: dict[str, Any]) -> None:
    game_state = payload["game_state"]
    move_stack = _raw_move_stack(game_state)
    _validate_board_fen_matches_move_stack(game_state=game_state, move_stack=move_stack)
    scoresheet_move_stack = _raw_move_stack_from_scoresheets(game_state)
    if scoresheet_move_stack != move_stack:
        raise MalformedDataError("Scoresheet-derived moves do not match move_stack")


def _raw_move_stack(game_state: dict[str, Any]) -> tuple[str, ...]:
    move_stack = game_state.get("move_stack")
    if not isinstance(move_stack, list) or not all(isinstance(move_uci, str) for move_uci in move_stack):
        raise MalformedDataError("Invalid move_stack in canonical engine_state")
    return tuple(move_stack)


def _validate_board_fen_matches_move_stack(*, game_state: dict[str, Any], move_stack: tuple[str, ...]) -> None:
    board_fen = game_state.get("board_fen")
    if not isinstance(board_fen, str):
        raise MalformedDataError("Invalid board_fen in canonical engine_state")

    board = chess.Board()
    try:
        for move_uci in move_stack:
            board.push_uci(move_uci)
    except ValueError as exc:
        raise MalformedDataError(f"Invalid move_stack entry: {move_uci}") from exc

    if board.fen() != board_fen:
        raise MalformedDataError("Serialized move_stack does not match board_fen")


def _raw_move_stack_from_scoresheets(game_state: dict[str, Any]) -> tuple[str, ...]:
    white_moves = _raw_scoresheet_own_moves(game_state.get("white_scoresheet"))
    black_moves = _raw_scoresheet_own_moves(game_state.get("black_scoresheet"))
    extracted: list[str] = []
    max_turns = max(len(white_moves), len(black_moves))

    for turn_index in range(max_turns):
        if turn_index < len(white_moves):
            extracted.extend(_raw_completed_moves_from_turn(white_moves[turn_index]))
        if turn_index < len(black_moves):
            extracted.extend(_raw_completed_moves_from_turn(black_moves[turn_index]))

    return tuple(extracted)


def _raw_scoresheet_own_moves(scoresheet: Any) -> list[Any]:
    if not isinstance(scoresheet, dict) or not isinstance(scoresheet.get("moves_own"), list):
        raise MalformedDataError("Invalid scoresheet in canonical engine_state")
    return scoresheet["moves_own"]


def _raw_completed_moves_from_turn(turn: Any) -> tuple[str, ...]:
    if not isinstance(turn, list):
        raise MalformedDataError("Invalid scoresheet turn in canonical engine_state")

    completed: list[str] = []
    for pair in turn:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            raise MalformedDataError("Invalid scoresheet move pair in canonical engine_state")
        question, answer = pair
        if not isinstance(question, dict) or not isinstance(answer, dict):
            raise MalformedDataError("Invalid scoresheet move pair in canonical engine_state")
        if question.get("question_type") != "COMMON":
            continue
        if answer.get("main_announcement") not in {"REGULAR_MOVE", "CAPTURE_DONE"}:
            continue
        move_uci = question.get("chess_move")
        if not isinstance(move_uci, str):
            raise MalformedDataError("Scoresheet move is missing chess_move")
        completed.append(move_uci)

    if len(completed) > 1:
        raise MalformedDataError("Scoresheet turn contains multiple completed moves")
    return tuple(completed)


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
