from __future__ import annotations

from typing import Any

from kriegspiel.serialization import SERIALIZATION_SCHEMA_VERSION as CANONICAL_ENGINE_STATE_SCHEMA_VERSION

from app.services.engine_adapter import create_new_game, deserialize_game_state, deserialize_scoresheet, serialize_game_state
from app.services.state_projection import reconstruct_scoresheets_from_moves


def classify_engine_state(payload: Any) -> str:
    if payload is None:
        return "none"
    if isinstance(payload, dict) and isinstance(payload.get("game_state"), dict):
        return f"canonical:{payload.get('schema_version', 'unknown')}"
    if isinstance(payload, dict):
        return f"legacy:{payload.get('schema_version', 'unknown')}"
    return type(payload).__name__


def is_current_canonical_engine_state(payload: Any) -> bool:
    return (
        isinstance(payload, dict)
        and isinstance(payload.get("game_state"), dict)
        and payload.get("schema_version") == CANONICAL_ENGINE_STATE_SCHEMA_VERSION
    )


def canonicalize_game_document(game: dict[str, Any]) -> dict[str, Any] | None:
    current = game.get("engine_state")
    if is_current_canonical_engine_state(current):
        return None

    engine = _load_engine_for_migration(game)
    _hydrate_scoresheets_from_moves_if_needed(game=game, engine=engine, original_engine_state=current)
    canonical = serialize_game_state(engine)

    # Validate the exact payload that will be written.
    deserialize_game_state(canonical)
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
