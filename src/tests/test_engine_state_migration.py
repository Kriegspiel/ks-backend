from __future__ import annotations

import pytest
from kriegspiel import __version__ as KRIEGSPIEL_LIBRARY_VERSION
from kriegspiel.serialization import SERIALIZATION_SCHEMA_VERSION as CANONICAL_ENGINE_STATE_SCHEMA_VERSION
from kriegspiel.serialization import MalformedDataError

from app.services.engine_adapter import _serialize_legacy_game_state, attempt_move, create_new_game, serialize_game_state
from app.services.engine_adapter import (
    INTERMEDIATE_CANONICAL_ENGINE_STATE_SCHEMA_VERSION,
    PREVIOUS_CANONICAL_ENGINE_STATE_SCHEMA_VERSION,
)
from app.services.engine_state_migration import (
    _raw_completed_moves_from_turn,
    _raw_move_stack,
    _raw_move_stack_from_scoresheets,
    _raw_scoresheet_own_moves,
    _synthesize_scoresheets_from_move_stack,
    _upgrade_previous_canonical_engine_state,
    _validated_canonical_payload,
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


def test_classify_engine_state_handles_empty_legacy_and_unexpected_payloads() -> None:
    assert classify_engine_state(None) == "none"
    assert classify_engine_state({"schema_version": 2}) == "legacy:2"
    assert classify_engine_state(["bad"]) == "list"


def test_build_engine_state_migration_update_skips_current_canonical_payload() -> None:
    current = serialize_game_state(create_new_game(any_rule=True))

    assert build_engine_state_migration_update({"engine_state": current, "moves": []}) is None


def test_build_engine_state_migration_update_returns_none_when_canonicalize_has_no_patch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "app.services.engine_state_migration.canonicalize_game_document",
        lambda _game: None,
    )

    assert build_engine_state_migration_update({"engine_state": {"schema_version": "legacy"}, "moves": []}) is None


def test_build_engine_state_migration_update_wraps_new_canonical_payload() -> None:
    update = build_engine_state_migration_update({"engine_state": None, "moves": [], "rule_variant": "berkeley_any"})

    assert update is not None
    assert update["engine_state"]["schema_version"] == CANONICAL_ENGINE_STATE_SCHEMA_VERSION


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
    assert canonical["library_version"] == KRIEGSPIEL_LIBRARY_VERSION
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


def test_canonicalize_game_document_migrates_intermediate_canonical_schema() -> None:
    game = create_new_game(any_rule=True)
    attempt_move(game, "e2e4")

    intermediate = serialize_game_state(game)
    intermediate["schema_version"] = INTERMEDIATE_CANONICAL_ENGINE_STATE_SCHEMA_VERSION
    intermediate["library_version"] = "1.2.6"

    canonical = canonicalize_game_document({"engine_state": intermediate, "moves": [], "rule_variant": "berkeley_any"})

    assert canonical is not None
    assert canonical["schema_version"] == CANONICAL_ENGINE_STATE_SCHEMA_VERSION
    assert canonical["library_version"] == KRIEGSPIEL_LIBRARY_VERSION
    assert canonical["game_state"]["ruleset_id"] == "berkeley_any"
    assert canonical["game_state"]["move_stack"] == ["e2e4"]


def test_build_engine_state_migration_update_patches_previous_canonical_schema() -> None:
    game = create_new_game(any_rule=False)
    previous = _previous_canonical_payload(game)

    update = build_engine_state_migration_update({"engine_state": previous, "moves": [], "rule_variant": "berkeley"})

    assert update == {
        "engine_state.schema_version": CANONICAL_ENGINE_STATE_SCHEMA_VERSION,
        "engine_state.library_version": KRIEGSPIEL_LIBRARY_VERSION,
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


def test_previous_canonical_validation_rejects_malformed_move_stack_and_scoresheets() -> None:
    game = create_new_game(any_rule=True)
    previous = _previous_canonical_payload(game)

    with pytest.raises(MalformedDataError, match="Invalid move_stack"):
        _raw_move_stack({"move_stack": [object()]})

    with pytest.raises(MalformedDataError, match="Invalid scoresheet"):
        _raw_scoresheet_own_moves({"moves_own": "bad"})

    previous["game_state"]["white_scoresheet"] = "bad"
    with pytest.raises(MalformedDataError, match="Invalid scoresheet"):
        canonicalize_game_document({"engine_state": previous, "moves": [], "rule_variant": "berkeley_any"})

    assert _upgrade_previous_canonical_engine_state({"schema_version": PREVIOUS_CANONICAL_ENGINE_STATE_SCHEMA_VERSION}) is None


def test_previous_canonical_validation_rejects_bad_board_and_invalid_move_stack_entry() -> None:
    with pytest.raises(MalformedDataError, match="Invalid board_fen"):
        canonicalize_game_document(
            {
                "engine_state": {
                    "schema_version": PREVIOUS_CANONICAL_ENGINE_STATE_SCHEMA_VERSION,
                    "game_state": {
                        "move_stack": [],
                        "white_scoresheet": {"moves_own": []},
                        "black_scoresheet": {"moves_own": []},
                    },
                },
                "moves": [],
                "rule_variant": "berkeley_any",
            }
        )

    game = create_new_game(any_rule=True)
    previous = _previous_canonical_payload(game)
    previous["game_state"]["move_stack"] = ["not-uci"]
    with pytest.raises(MalformedDataError, match="Invalid move_stack entry"):
        canonicalize_game_document({"engine_state": previous, "moves": [], "rule_variant": "berkeley_any"})


def test_raw_completed_moves_from_turn_rejects_malformed_turn_shapes() -> None:
    with pytest.raises(MalformedDataError, match="Invalid scoresheet turn"):
        _raw_completed_moves_from_turn("bad")

    with pytest.raises(MalformedDataError, match="Invalid scoresheet move pair"):
        _raw_completed_moves_from_turn([["only-one"]])

    with pytest.raises(MalformedDataError, match="Invalid scoresheet move pair"):
        _raw_completed_moves_from_turn([[{}, "bad"]])

    with pytest.raises(MalformedDataError, match="missing chess_move"):
        _raw_completed_moves_from_turn(
            [[{"question_type": "COMMON"}, {"main_announcement": "REGULAR_MOVE"}]]
        )

    with pytest.raises(MalformedDataError, match="multiple completed moves"):
        _raw_completed_moves_from_turn(
            [
                [{"question_type": "COMMON", "chess_move": "e2e4"}, {"main_announcement": "REGULAR_MOVE"}],
                [{"question_type": "COMMON", "chess_move": "d2d4"}, {"main_announcement": "CAPTURE_DONE"}],
            ]
        )


def test_raw_move_stack_from_scoresheets_ignores_non_completed_and_non_common_entries() -> None:
    game_state = {
        "white_scoresheet": {
            "moves_own": [
                [
                    [{"question_type": "ASK_ANY"}, {"main_announcement": "HAS_ANY"}],
                    [{"question_type": "COMMON", "chess_move": "e2e4"}, {"main_announcement": "ILLEGAL_MOVE"}],
                ]
            ]
        },
        "black_scoresheet": {"moves_own": []},
    }

    assert _raw_move_stack_from_scoresheets(game_state) == ()

    black_only = {
        "white_scoresheet": {"moves_own": []},
        "black_scoresheet": {
            "moves_own": [
                [[{"question_type": "COMMON", "chess_move": "e7e5"}, {"main_announcement": "REGULAR_MOVE"}]]
            ]
        },
    }
    assert _raw_move_stack_from_scoresheets(black_only) == ("e7e5",)


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


def test_validated_canonical_payload_reraises_unexpected_malformed_data(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = create_new_game(any_rule=True)

    def broken_deserialize(_payload):
        raise MalformedDataError("different failure")

    monkeypatch.setattr("app.services.engine_state_migration.deserialize_game_state", broken_deserialize)

    with pytest.raises(MalformedDataError, match="different failure"):
        _validated_canonical_payload(engine=engine, game={"moves": []})


def test_synthesized_scoresheets_include_black_moves_from_even_plies() -> None:
    engine = create_new_game(any_rule=True)
    attempt_move(engine, "e2e4")
    attempt_move(engine, "e7e5")

    _synthesize_scoresheets_from_move_stack(engine)
    payload = serialize_game_state(engine)

    assert payload["game_state"]["white_scoresheet"]["moves_own"][0][0][0]["chess_move"] == "e2e4"
    assert payload["game_state"]["black_scoresheet"]["moves_own"][0][0][0]["chess_move"] == "e7e5"
