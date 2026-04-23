from __future__ import annotations

import pytest
from kriegspiel.serialization import SERIALIZATION_SCHEMA_VERSION as CANONICAL_ENGINE_STATE_SCHEMA_VERSION
from kriegspiel.serialization import MalformedDataError

from app.services.engine_adapter import _serialize_legacy_game_state, attempt_move, create_new_game, serialize_game_state
from app.services.engine_adapter import PREVIOUS_CANONICAL_ENGINE_STATE_SCHEMA_VERSION
from app.services.engine_state_migration import (
    build_engine_state_migration_update,
    canonicalize_game_document,
    classify_engine_state,
)


def _previous_canonical_payload(game):
    payload = serialize_game_state(game)
    payload["schema_version"] = PREVIOUS_CANONICAL_ENGINE_STATE_SCHEMA_VERSION
    payload["library_version"] = "1.2.3"
    payload["game_state"].pop("ruleset_id", None)
    return payload


def test_canonicalize_game_document_migrates_legacy_v2_payload() -> None:
    game = create_new_game(any_rule=True)
    attempt_move(game, "e2e4")

    legacy = _serialize_legacy_game_state(game, schema_version=2, include_scoresheets=True)
    canonical = canonicalize_game_document({"engine_state": legacy, "moves": [], "rule_variant": "berkeley_any"})

    assert canonical is not None
    assert canonical["schema_version"] == CANONICAL_ENGINE_STATE_SCHEMA_VERSION
    assert canonical["game_state"]["move_stack"] == ["e2e4"]
    assert canonical["game_state"]["possible_to_ask"]


def test_canonicalize_game_document_rehydrates_scoresheets_from_moves_for_legacy_v1() -> None:
    game = create_new_game(any_rule=True)
    attempt_move(game, "e2e4")
    legacy = _serialize_legacy_game_state(game, schema_version=1, include_scoresheets=False)

    canonical = canonicalize_game_document(
        {
            "engine_state": legacy,
            "rule_variant": "berkeley_any",
            "moves": [
                {
                    "ply": 1,
                    "color": "white",
                    "question_type": "COMMON",
                    "uci": "e2e4",
                    "announcement": "REGULAR_MOVE",
                    "special_announcement": None,
                    "capture_square": None,
                    "move_done": True,
                }
            ],
        }
    )

    assert canonical is not None
    assert canonical["game_state"]["white_scoresheet"]["moves_own"]
    assert canonical["game_state"]["black_scoresheet"]["moves_opponent"]


def test_canonicalize_game_document_bootstraps_missing_engine_state() -> None:
    canonical = canonicalize_game_document({"engine_state": None, "rule_variant": "berkeley", "moves": []})

    assert canonical is not None
    assert canonical["schema_version"] == CANONICAL_ENGINE_STATE_SCHEMA_VERSION
    assert canonical["game_state"]["any_rule"] is False


def test_canonicalize_game_document_migrates_previous_canonical_schema() -> None:
    game = create_new_game(any_rule=True)
    attempt_move(game, "e2e4")

    previous = _previous_canonical_payload(game)
    canonical = canonicalize_game_document({"engine_state": previous, "moves": [], "rule_variant": "berkeley_any"})

    assert classify_engine_state(previous) == "canonical:3"
    assert canonical is not None
    assert canonical["schema_version"] == CANONICAL_ENGINE_STATE_SCHEMA_VERSION
    assert canonical["library_version"] == "1.2.6"
    assert canonical["game_state"]["ruleset_id"] == "berkeley_any"
    assert canonical["game_state"]["move_stack"] == ["e2e4"]


def test_canonicalize_game_document_preserves_berkeley_without_any_rule_from_previous_schema() -> None:
    game = create_new_game(any_rule=False)

    previous = _previous_canonical_payload(game)
    canonical = canonicalize_game_document({"engine_state": previous, "moves": [], "rule_variant": "berkeley"})

    assert canonical is not None
    assert canonical["schema_version"] == CANONICAL_ENGINE_STATE_SCHEMA_VERSION
    assert canonical["game_state"]["ruleset_id"] == "berkeley"
    assert canonical["game_state"]["any_rule"] is False


def test_build_engine_state_migration_update_patches_previous_canonical_schema() -> None:
    game = create_new_game(any_rule=False)
    previous = _previous_canonical_payload(game)

    update = build_engine_state_migration_update({"engine_state": previous, "moves": [], "rule_variant": "berkeley"})

    assert update == {
        "engine_state.schema_version": CANONICAL_ENGINE_STATE_SCHEMA_VERSION,
        "engine_state.library_version": "1.2.6",
        "engine_state.game_state.ruleset_id": "berkeley",
    }


def test_canonicalize_game_document_rejects_previous_canonical_board_mismatch() -> None:
    game = create_new_game(any_rule=True)
    attempt_move(game, "e2e4")
    previous = _previous_canonical_payload(game)
    previous["game_state"]["board_fen"] = create_new_game(any_rule=True)._board.fen()  # noqa: SLF001

    with pytest.raises(MalformedDataError, match="move_stack"):
        canonicalize_game_document({"engine_state": previous, "moves": [], "rule_variant": "berkeley_any"})


def test_canonicalize_game_document_rejects_previous_canonical_scoresheet_mismatch() -> None:
    game = create_new_game(any_rule=True)
    attempt_move(game, "e2e4")
    previous = _previous_canonical_payload(game)
    previous["game_state"]["white_scoresheet"]["moves_own"][0][0][0]["chess_move"] = "d2d4"

    with pytest.raises(MalformedDataError, match="Scoresheet-derived moves"):
        canonicalize_game_document({"engine_state": previous, "moves": [], "rule_variant": "berkeley_any"})


def test_canonicalize_game_document_skips_current_canonical() -> None:
    game = create_new_game(any_rule=True)
    current = serialize_game_state(game)

    assert canonicalize_game_document({"engine_state": current, "moves": [], "rule_variant": "berkeley_any"}) is None


def test_canonicalize_game_document_falls_back_to_move_stack_when_moves_conflict() -> None:
    game = create_new_game(any_rule=True)
    attempt_move(game, "e2e4")
    legacy = _serialize_legacy_game_state(game, schema_version=1, include_scoresheets=False)

    canonical = canonicalize_game_document(
        {
            "engine_state": legacy,
            "rule_variant": "berkeley_any",
            "moves": [
                {
                    "ply": 1,
                    "color": "white",
                    "question_type": "COMMON",
                    "uci": "d2d4",
                    "announcement": "REGULAR_MOVE",
                    "special_announcement": None,
                    "capture_square": None,
                    "move_done": True,
                }
            ],
        }
    )

    assert canonical is not None
    assert canonical["game_state"]["white_scoresheet"]["moves_own"][0][0][0]["chess_move"] == "e2e4"
