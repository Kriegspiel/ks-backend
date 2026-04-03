from __future__ import annotations

from app.services.engine_adapter import (
    ask_any,
    attempt_move,
    create_new_game,
    deserialize_game_state,
    project_visible_board,
    serialize_game_state,
)


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


def test_deserialize_repairs_empty_possible_to_ask_when_pawn_captures_required() -> None:
    game = create_new_game(any_rule=True)
    attempt_move(game, "d2c3")
    attempt_move(game, "b2b3")
    attempt_move(game, "e7e5")
    attempt_move(game, "g2f3")
    attempt_move(game, "d2d4")
    result = ask_any(game)

    assert result["announcement"] == "HAS_ANY"

    payload = serialize_game_state(game)
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
