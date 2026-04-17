from __future__ import annotations

from kriegspiel.serialization import SERIALIZATION_SCHEMA_VERSION as CANONICAL_ENGINE_STATE_SCHEMA_VERSION

from app.services.engine_adapter import _serialize_legacy_game_state, attempt_move, create_new_game, serialize_game_state
from app.services.engine_state_migration import canonicalize_game_document


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


def test_canonicalize_game_document_skips_current_canonical() -> None:
    game = create_new_game(any_rule=True)
    current = serialize_game_state(game)

    assert canonicalize_game_document({"engine_state": current, "moves": [], "rule_variant": "berkeley_any"}) is None
