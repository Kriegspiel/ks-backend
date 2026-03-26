from __future__ import annotations

from typing import Any, Literal

import chess
from kriegspiel.berkeley import BerkeleyGame
from kriegspiel.move import KriegspielMove, QuestionAnnouncement

PlayerColor = Literal["white", "black"]


def create_new_game(*, any_rule: bool = True) -> BerkeleyGame:
    return BerkeleyGame(any_rule=any_rule)


def project_visible_board(game: BerkeleyGame, color: PlayerColor) -> chess.Board:
    board = game._board.copy(stack=False)  # noqa: SLF001
    player_color = chess.WHITE if color == "white" else chess.BLACK
    for square in chess.SQUARES:
        piece = board.piece_at(square)
        if piece is not None and piece.color != player_color:
            board.remove_piece_at(square)
    return board


def visible_fen(game: BerkeleyGame, color: PlayerColor) -> str:
    board = project_visible_board(game, color)
    turn = "w" if game.turn == chess.WHITE else "b"
    return f"{board.board_fen()} {turn} - - 0 1"


def full_fen(game: BerkeleyGame) -> str:
    return game._board.fen()  # noqa: SLF001


def attempt_move(game: BerkeleyGame, move_uci: str) -> dict[str, Any]:
    try:
        chess_move = chess.Move.from_uci(move_uci)
    except ValueError:
        return {
            "move_done": False,
            "announcement": "INVALID_UCI",
            "special_announcement": None,
            "capture_square": None,
        }

    answer = game.ask_for(KriegspielMove(QuestionAnnouncement.COMMON, chess_move))
    return _answer_payload(game, answer)


def ask_any(game: BerkeleyGame) -> dict[str, Any]:
    answer = game.ask_for(KriegspielMove(QuestionAnnouncement.ASK_ANY))
    payload = _answer_payload(game, answer)
    payload["has_any"] = payload["announcement"] == "HAS_ANY"
    return payload


def serialize_game_state(game: BerkeleyGame) -> dict[str, Any]:
    serialized_moves = [
        {
            "question_type": move.question_type.name,
            "move_uci": move.chess_move.uci() if move.chess_move is not None else None,
        }
        for move in game.possible_to_ask
    ]
    serialized_moves.sort(key=lambda item: (item["question_type"], item["move_uci"] or ""))

    return {
        "schema_version": 1,
        "any_rule": game._any_rule,  # noqa: SLF001
        "must_use_pawns": game.must_use_pawns,
        "game_over": game.game_over,
        "board_fen": game._board.fen(),  # noqa: SLF001
        "move_stack": [move.uci() for move in game._board.move_stack],  # noqa: SLF001
        "possible_to_ask": serialized_moves,
    }


def deserialize_game_state(payload: dict[str, Any]) -> BerkeleyGame:
    any_rule = bool(payload.get("any_rule", True))
    game = BerkeleyGame(any_rule=any_rule)

    board = chess.Board()
    for move_uci in payload.get("move_stack", []):
        board.push(chess.Move.from_uci(move_uci))

    expected_fen = payload["board_fen"]
    if board.fen() != expected_fen:
        raise ValueError("Serialized move_stack does not match board_fen")

    game._board = board  # noqa: SLF001
    game._must_use_pawns = bool(payload.get("must_use_pawns", False))  # noqa: SLF001
    game._game_over = bool(payload.get("game_over", False))  # noqa: SLF001
    game._possible_to_ask = [_deserialize_ks_move(item) for item in payload.get("possible_to_ask", [])]  # noqa: SLF001
    return game


def _deserialize_ks_move(item: dict[str, Any]) -> KriegspielMove:
    question = QuestionAnnouncement[item["question_type"]]
    move_uci = item.get("move_uci")
    if move_uci is None:
        return KriegspielMove(question)
    return KriegspielMove(question, chess.Move.from_uci(move_uci))


def _answer_payload(game: BerkeleyGame, answer: Any) -> dict[str, Any]:
    capture_square = chess.square_name(answer.capture_at_square) if answer.capture_at_square is not None else None
    special = answer.special_announcement

    return {
        "move_done": bool(answer.move_done),
        "announcement": answer.main_announcement.name,
        "special_announcement": None if special is None else special.name,
        "capture_square": capture_square,
        "full_fen": game._board.fen(),  # noqa: SLF001
        "white_fen": visible_fen(game, "white"),
        "black_fen": visible_fen(game, "black"),
        "turn": "white" if game.turn == chess.WHITE else "black",
        "game_over": bool(game.game_over),
    }
