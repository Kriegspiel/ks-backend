from __future__ import annotations

import chess
import pytest
from kriegspiel.move import (
    CapturedPieceAnnouncement,
    KriegspielAnswer,
    KriegspielMove,
    MainAnnouncement,
    QuestionAnnouncement,
    SpecialCaseAnnouncement,
)

from app.services.engine_adapter import (
    CANONICAL_ENGINE_STATE_SCHEMA_VERSION,
    INTERMEDIATE_CANONICAL_ENGINE_STATE_SCHEMA_VERSION,
    PREVIOUS_CANONICAL_ENGINE_STATE_SCHEMA_VERSION,
    _deserialize_answer,
    _deserialize_color,
    _deserialize_question,
    _deserialize_scoresheet_turn,
    _repair_possible_to_ask,
    _serialize_legacy_game_state,
    _serialize_answer,
    _serialize_color,
    _serialize_scoresheet_turn,
    ask_any,
    attempt_move,
    create_new_game,
    deserialize_game_state,
    extract_stored_scoresheets,
    is_current_canonical_engine_state,
    is_supported_canonical_engine_state,
    project_visible_board,
    public_material_summary,
    public_reserve_summary,
    serialize_scoresheet,
    serialize_game_state,
)


def _previous_canonical_payload(game):
    payload = serialize_game_state(game)
    payload["schema_version"] = PREVIOUS_CANONICAL_ENGINE_STATE_SCHEMA_VERSION
    payload["library_version"] = "1.2.3"
    payload["game_state"].pop("ruleset_id", None)
    return payload


def test_create_new_game_and_legal_move_succeeds() -> None:
    game = create_new_game(any_rule=True)

    result = attempt_move(game, "e2e4")

    assert result["move_done"] is True
    assert result["announcement"] in {"REGULAR_MOVE", "CAPTURE_DONE"}


def test_illegal_move_returns_classified_announcement() -> None:
    game = create_new_game(any_rule=True)

    result = attempt_move(game, "e2e5")

    assert result["move_done"] is False
    assert result["announcement"] in {"ILLEGAL_MOVE", "IMPOSSIBLE_TO_ASK"}


def test_visible_projection_hides_opponent_pieces() -> None:
    game = create_new_game(any_rule=True)

    white_view = project_visible_board(game, "white")

    assert white_view.piece_at(0).symbol().isupper()
    assert white_view.piece_at(56) is None


def test_ask_any_has_stable_contract() -> None:
    game = create_new_game(any_rule=True)

    result = ask_any(game)

    assert result["announcement"] in {"HAS_ANY", "NO_ANY", "IMPOSSIBLE_TO_ASK"}
    assert isinstance(result["has_any"], bool)


def test_serialize_deserialize_round_trip_preserves_state() -> None:
    game = create_new_game(any_rule=True)
    attempt_move(game, "e2e4")
    attempt_move(game, "e7e5")
    ask_any(game)

    payload = serialize_game_state(game)
    restored = deserialize_game_state(payload)

    assert restored._board.fen() == game._board.fen()  # noqa: SLF001
    assert restored.turn == game.turn
    assert restored.must_use_pawns == game.must_use_pawns
    assert [m.uci() for m in restored._board.move_stack] == [m.uci() for m in game._board.move_stack]  # noqa: SLF001


def test_deserialize_game_state_accepts_previous_canonical_schema() -> None:
    game = create_new_game(any_rule=False)
    attempt_move(game, "e2e4")

    payload = _previous_canonical_payload(game)
    restored = deserialize_game_state(payload)

    assert payload["schema_version"] == 3
    assert is_supported_canonical_engine_state(payload) is True
    assert is_current_canonical_engine_state(payload) is False
    assert restored._board.fen() == game._board.fen()  # noqa: SLF001
    assert restored.ruleset_id == "berkeley"


def test_deserialize_game_state_accepts_intermediate_canonical_schema() -> None:
    game = create_new_game(any_rule=True)
    attempt_move(game, "e2e4")

    payload = serialize_game_state(game)
    payload["schema_version"] = INTERMEDIATE_CANONICAL_ENGINE_STATE_SCHEMA_VERSION
    payload["library_version"] = "1.2.6"

    restored = deserialize_game_state(payload)

    assert is_supported_canonical_engine_state(payload) is True
    assert is_current_canonical_engine_state(payload) is False
    assert restored._board.fen() == game._board.fen()  # noqa: SLF001
    assert restored.ruleset_id == "berkeley_any"


def test_deserialize_repairs_empty_possible_to_ask_when_pawn_captures_required() -> None:
    game = create_new_game(any_rule=True)
    attempt_move(game, "d2c3")
    attempt_move(game, "b2b3")
    attempt_move(game, "e7e5")
    attempt_move(game, "g2f3")
    attempt_move(game, "d2d4")
    result = ask_any(game)

    assert result["announcement"] == "HAS_ANY"

    payload = _serialize_legacy_game_state(game)
    payload["possible_to_ask"] = []

    restored = deserialize_game_state(payload)
    restored_moves = sorted(
        move.chess_move.uci()
        for move in restored.possible_to_ask
        if move.question_type.name == "COMMON" and move.chess_move is not None
    )

    assert restored.must_use_pawns is True
    assert restored_moves == [
        "a7b6",
        "b7a6",
        "b7c6",
        "c7b6",
        "c7d6",
        "d7c6",
        "d7e6",
        "e5d4",
        "e5f4",
        "f7e6",
        "f7g6",
        "g7f6",
        "g7h6",
        "h7g6",
    ]


def test_invalid_uci_and_default_scoresheet_serialization_are_stable() -> None:
    game = create_new_game(any_rule=True)

    result = attempt_move(game, "bad-uci")

    assert result == {
        "move_done": False,
        "announcement": "INVALID_UCI",
        "special_announcement": None,
        "capture_square": None,
        "captured_piece_announcement": None,
        "dropped_piece_announcement": None,
        "promotion_announced": None,
        "next_turn_pawn_tries": None,
        "next_turn_has_pawn_capture": None,
        "next_turn_pawn_try_squares": None,
    }
    assert serialize_scoresheet(None) == {"color": None, "last_move_number": 0, "moves_own": [], "moves_opponent": []}


def test_deserialize_game_state_validates_board_and_repairs_missing_move_lists() -> None:
    payload = _serialize_legacy_game_state(create_new_game(any_rule=True))
    payload["board_fen"] = "invalid-fen"

    with pytest.raises(ValueError, match="move_stack"):
        deserialize_game_state(payload)

    class RepairStub:
        def __init__(self, *, must_use_pawns: bool) -> None:
            self._possible_to_ask = []
            self._game_over = False
            self._must_use_pawns = must_use_pawns
            self.generated = False

        def _generate_possible_to_ask_list(self) -> None:
            self.generated = True

    repair_stub = RepairStub(must_use_pawns=False)
    _repair_possible_to_ask(repair_stub)
    assert repair_stub.generated is True

    with pytest.raises(AttributeError, match="pawn-capture generator"):
        _repair_possible_to_ask(RepairStub(must_use_pawns=True))


def test_extract_stored_scoresheets_supports_canonical_and_legacy_payloads() -> None:
    game = create_new_game(any_rule=True)
    attempt_move(game, "e2e4")

    canonical = serialize_game_state(game)
    legacy = _serialize_legacy_game_state(game)
    previous_canonical = _previous_canonical_payload(game)

    canonical_scoresheets = extract_stored_scoresheets(canonical)
    legacy_scoresheets = extract_stored_scoresheets(legacy)
    previous_canonical_scoresheets = extract_stored_scoresheets(previous_canonical)

    assert canonical["schema_version"] == CANONICAL_ENGINE_STATE_SCHEMA_VERSION
    assert canonical_scoresheets is not None
    assert canonical_scoresheets["white"]["moves_own"]
    assert canonical_scoresheets["black"]["moves_opponent"]
    assert legacy_scoresheets is not None
    assert legacy_scoresheets["white"]["moves_own"]
    assert previous_canonical_scoresheets is not None
    assert previous_canonical_scoresheets["white"]["moves_own"]


def test_engine_adapter_private_serializers_cover_fallback_shapes() -> None:
    assert _serialize_color("white") == "white"
    assert _serialize_color("black") == "black"
    assert _serialize_color("unknown") is None
    assert _deserialize_color(None, fallback="black") == "black"

    assert _serialize_scoresheet_turn([("bad",)], own=True) == []
    assert _deserialize_scoresheet_turn([None], own=True) == []
    question = _deserialize_question({}, own=True)
    assert isinstance(question, KriegspielMove)
    assert question.question_type == QuestionAnnouncement.NONE
    assert _serialize_answer(object())["main_announcement"] == "ILLEGAL_MOVE"


def test_deserialize_answer_handles_capture_and_special_cases() -> None:
    capture_answer = _deserialize_answer(
        {
            "main_announcement": "CAPTURE_DONE",
            "capture_square": "d4",
        }
    )
    assert isinstance(capture_answer, KriegspielAnswer)
    assert capture_answer.main_announcement == MainAnnouncement.CAPTURE_DONE
    assert capture_answer.capture_at_square is not None

    special_answer = _deserialize_answer(
        {
            "main_announcement": "REGULAR_MOVE",
            "special_announcement": "CHECK_DOUBLE",
            "checks": ["CHECK_FILE", "CHECK_RANK"],
        }
    )
    assert special_answer.special_announcement == SpecialCaseAnnouncement.CHECK_DOUBLE


def test_answer_serializers_cover_named_checks_and_non_double_specials() -> None:
    double_check = _deserialize_answer(
        {
            "main_announcement": "REGULAR_MOVE",
            "special_announcement": "CHECK_DOUBLE",
            "checks": ["CHECK_FILE", "CHECK_RANK"],
        }
    )
    serialized = _serialize_answer(double_check)
    restored = _deserialize_answer(
        {
            "main_announcement": "REGULAR_MOVE",
            "special_announcement": "CHECK_FILE",
            "checks": ["CHECK_FILE"],
        }
    )

    assert serialized["checks"] == ["CHECK_FILE", "CHECK_RANK"]
    assert serialized["special_announcement"] == "CHECK_DOUBLE"
    assert restored.special_announcement == SpecialCaseAnnouncement.CHECK_FILE


@pytest.mark.parametrize(
    ("rule_variant", "expected_ruleset_id"),
    [
        ("cincinnati", "cincinnati"),
        ("wild16", "wild16"),
        ("rand", "rand"),
        ("english", "english"),
        ("crazykrieg", "crazykrieg"),
    ],
)
def test_create_new_game_accepts_new_rulesets(rule_variant: str, expected_ruleset_id: str) -> None:
    game = create_new_game(rule_variant=rule_variant)

    assert game.ruleset_id == expected_ruleset_id


def test_attempt_move_surfaces_cincinnati_next_turn_binary_pawn_capture() -> None:
    game = create_new_game(rule_variant="cincinnati")

    attempt_move(game, "e2e4")
    result = attempt_move(game, "d7d5")

    assert result["announcement"] == "REGULAR_MOVE"
    assert result["next_turn_has_pawn_capture"] is True
    assert result["next_turn_pawn_tries"] is None


def test_attempt_move_surfaces_wild16_pawn_try_count_and_typed_capture() -> None:
    game = create_new_game(rule_variant="wild16")
    attempt_move(game, "e2e4")
    result = attempt_move(game, "d7d5")

    assert result["announcement"] == "REGULAR_MOVE"
    assert result["next_turn_pawn_tries"] == 1
    assert result["next_turn_has_pawn_capture"] is None

    capture = _serialize_answer(
        KriegspielAnswer(
            MainAnnouncement.CAPTURE_DONE,
            capture_at_square=chess.D4,
            captured_piece_announcement=CapturedPieceAnnouncement.PAWN,
            next_turn_pawn_tries=0,
        )
    )

    assert capture["captured_piece_announcement"] == "PAWN"
    assert capture["next_turn_pawn_tries"] == 0


def test_attempt_move_surfaces_rand_and_crazykrieg_metadata() -> None:
    rand = create_new_game(rule_variant="rand")
    rand_result = attempt_move(rand, "e2e4")
    assert rand_result["next_turn_pawn_try_squares"] == []

    crazy = create_new_game(rule_variant="crazykrieg")
    attempt_move(crazy, "e2e4")
    attempt_move(crazy, "d7d5")
    capture = attempt_move(crazy, "e4d5")
    assert capture["captured_piece_announcement"] == "PAWN"
    assert public_reserve_summary(crazy)["white"]["pawns"] == 1

    attempt_move(crazy, "e7e6")
    drop = attempt_move(crazy, "P@e4")
    assert drop["dropped_piece_announcement"] == "PAWN"
    assert public_reserve_summary(crazy)["white"]["pawns"] == 0


@pytest.mark.parametrize("rule_variant", ["berkeley", "berkeley_any"])
def test_public_material_summary_hides_pawn_counts_for_berkeley_family(rule_variant: str) -> None:
    game = create_new_game(rule_variant=rule_variant)
    attempt_move(game, "e2e4")
    attempt_move(game, "d7d5")
    attempt_move(game, "e4d5")

    assert public_material_summary(game) == {
        "white": {"pieces_remaining": 16, "pawns_captured": None},
        "black": {"pieces_remaining": 15, "pawns_captured": None},
    }


@pytest.mark.parametrize("rule_variant", ["cincinnati", "wild16", "rand", "crazykrieg"])
def test_public_material_summary_includes_public_pawn_counts_for_typed_rulesets(rule_variant: str) -> None:
    game = create_new_game(rule_variant=rule_variant)
    attempt_move(game, "e2e4")
    attempt_move(game, "d7d5")
    attempt_move(game, "e4d5")

    assert public_material_summary(game) == {
        "white": {"pieces_remaining": 16, "pawns_captured": 0},
        "black": {"pieces_remaining": 15, "pawns_captured": 1},
    }


def test_public_reserve_summary_defaults_to_zero_for_non_drop_rulesets() -> None:
    assert public_reserve_summary(create_new_game(rule_variant="english")) == {
        "white": {"pawns": 0, "knights": 0, "bishops": 0, "rooks": 0, "queens": 0},
        "black": {"pawns": 0, "knights": 0, "bishops": 0, "rooks": 0, "queens": 0},
    }
