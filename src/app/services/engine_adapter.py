from __future__ import annotations

from typing import Any, Literal

import chess
from kriegspiel import KriegspielGame
from kriegspiel.berkeley import BerkeleyGame
from kriegspiel.cincinnati import CincinnatiGame
from kriegspiel.move import (
    CapturedPieceAnnouncement,
    KriegspielAnswer,
    KriegspielMove,
    KriegspielScoresheet,
    MainAnnouncement,
    QuestionAnnouncement,
    SpecialCaseAnnouncement,
)
from kriegspiel.serialization import (
    SERIALIZATION_SCHEMA_VERSION as CANONICAL_ENGINE_STATE_SCHEMA_VERSION,
    deserialize_berkeley_game,
    deserialize_kriegspiel_scoresheet,
    serialize_berkeley_game,
)
from kriegspiel.wild16 import Wild16Game

PlayerColor = Literal["white", "black"]
RuleVariant = Literal["berkeley", "berkeley_any", "cincinnati", "wild16"]
SUPPORTED_RULE_VARIANTS = frozenset({"berkeley", "berkeley_any", "cincinnati", "wild16"})

PREVIOUS_CANONICAL_ENGINE_STATE_SCHEMA_VERSION = 3
INTERMEDIATE_CANONICAL_ENGINE_STATE_SCHEMA_VERSION = 4
SUPPORTED_CANONICAL_ENGINE_STATE_SCHEMA_VERSIONS = frozenset(
    {
        PREVIOUS_CANONICAL_ENGINE_STATE_SCHEMA_VERSION,
        INTERMEDIATE_CANONICAL_ENGINE_STATE_SCHEMA_VERSION,
        CANONICAL_ENGINE_STATE_SCHEMA_VERSION,
    }
)


def create_new_game(*, any_rule: bool | None = None, rule_variant: RuleVariant | None = None) -> KriegspielGame:
    if rule_variant is None:
        resolved_any_rule = True if any_rule is None else any_rule
        rule_variant = "berkeley_any" if resolved_any_rule else "berkeley"

    if rule_variant == "berkeley":
        return BerkeleyGame(ruleset="berkeley")
    if rule_variant == "berkeley_any":
        return BerkeleyGame(ruleset="berkeley_any")
    if rule_variant == "cincinnati":
        return CincinnatiGame()
    if rule_variant == "wild16":
        return Wild16Game()
    raise ValueError(f"Unsupported rule variant: {rule_variant}")


def project_visible_board(game: KriegspielGame, color: PlayerColor) -> chess.Board:
    board = game._board.copy(stack=False)  # noqa: SLF001
    player_color = chess.WHITE if color == "white" else chess.BLACK
    for square in chess.SQUARES:
        piece = board.piece_at(square)
        if piece is not None and piece.color != player_color:
            board.remove_piece_at(square)
    return board


def visible_fen(game: KriegspielGame, color: PlayerColor) -> str:
    board = project_visible_board(game, color)
    turn = "w" if game.turn == chess.WHITE else "b"
    return f"{board.board_fen()} {turn} - - 0 1"


def full_fen(game: KriegspielGame) -> str:
    return game._board.fen()  # noqa: SLF001


def public_material_summary(game: KriegspielGame) -> dict[str, dict[str, int | None]]:
    summary = game.public_material_summary

    return {
        "white": _material_side_payload(summary.white),
        "black": _material_side_payload(summary.black),
    }


def _material_side_payload(side: Any) -> dict[str, int | None]:
    return {
        "pieces_remaining": int(getattr(side, "pieces_remaining", 16)),
        "pawns_captured": getattr(side, "pawns_captured", None),
    }


def attempt_move(game: KriegspielGame, move_uci: str) -> dict[str, Any]:
    try:
        chess_move = chess.Move.from_uci(move_uci)
    except ValueError:
        return {
            "move_done": False,
            "announcement": "INVALID_UCI",
            "special_announcement": None,
            "capture_square": None,
            "captured_piece_announcement": None,
            "next_turn_pawn_tries": None,
            "next_turn_has_pawn_capture": None,
        }

    answer = game.ask_for(KriegspielMove(QuestionAnnouncement.COMMON, chess_move))
    return _answer_payload(game, answer)


def ask_any(game: KriegspielGame) -> dict[str, Any]:
    answer = game.ask_for(KriegspielMove(QuestionAnnouncement.ASK_ANY))
    payload = _answer_payload(game, answer)
    payload["has_any"] = payload["announcement"] == "HAS_ANY"
    return payload


def serialize_scoresheet(scoresheet: Any) -> dict[str, Any]:
    if scoresheet is None:
        return {"color": None, "last_move_number": 0, "moves_own": [], "moves_opponent": []}

    return {
        "color": _serialize_color(getattr(scoresheet, "color", None)),
        "last_move_number": int(getattr(scoresheet, "_KriegspielScoresheet__last_move_number", 0)),
        "moves_own": [_serialize_scoresheet_turn(turn, own=True) for turn in getattr(scoresheet, "moves_own", [])],
        "moves_opponent": [_serialize_scoresheet_turn(turn, own=False) for turn in getattr(scoresheet, "moves_opponent", [])],
    }


def deserialize_scoresheet(payload: dict[str, Any] | None, *, fallback_color: PlayerColor) -> KriegspielScoresheet:
    color = _deserialize_color((payload or {}).get("color"), fallback=fallback_color)
    scoresheet = KriegspielScoresheet(chess.WHITE if color == "white" else chess.BLACK)
    scoresheet._KriegspielScoresheet__moves_own = [  # noqa: SLF001
        _deserialize_scoresheet_turn(turn, own=True) for turn in (payload or {}).get("moves_own", [])
    ]
    scoresheet._KriegspielScoresheet__moves_opponent = [  # noqa: SLF001
        _deserialize_scoresheet_turn(turn, own=False) for turn in (payload or {}).get("moves_opponent", [])
    ]
    scoresheet._KriegspielScoresheet__last_move_number = int((payload or {}).get("last_move_number", 0))  # noqa: SLF001
    return scoresheet


def serialize_game_state(game: KriegspielGame) -> dict[str, Any]:
    return serialize_berkeley_game(game)


def deserialize_game_state(payload: dict[str, Any]) -> KriegspielGame:
    if is_supported_canonical_engine_state(payload):
        return deserialize_berkeley_game(payload)
    return _deserialize_legacy_game_state(payload)


def extract_stored_scoresheets(payload: dict[str, Any] | None) -> dict[str, dict[str, Any]] | None:
    if not isinstance(payload, dict):
        return None

    if is_supported_canonical_engine_state(payload):
        game_state = payload.get("game_state")
        if not isinstance(game_state, dict):
            return None
        white = game_state.get("white_scoresheet")
        black = game_state.get("black_scoresheet")
        if not isinstance(white, dict) or not isinstance(black, dict):
            return None
        return {
            "white": serialize_scoresheet(deserialize_kriegspiel_scoresheet(white)),
            "black": serialize_scoresheet(deserialize_kriegspiel_scoresheet(black)),
        }

    white = payload.get("white_scoresheet")
    black = payload.get("black_scoresheet")
    if isinstance(white, dict) and isinstance(black, dict):
        return {"white": white, "black": black}
    return None


def is_current_canonical_engine_state(payload: Any) -> bool:
    return _has_canonical_engine_state_shape(payload) and payload.get("schema_version") == CANONICAL_ENGINE_STATE_SCHEMA_VERSION


def is_supported_canonical_engine_state(payload: Any) -> bool:
    return (
        _has_canonical_engine_state_shape(payload)
        and payload.get("schema_version") in SUPPORTED_CANONICAL_ENGINE_STATE_SCHEMA_VERSIONS
    )


def _has_canonical_engine_state_shape(payload: Any) -> bool:
    return isinstance(payload, dict) and isinstance(payload.get("game_state"), dict)


def _is_canonical_engine_state(payload: dict[str, Any]) -> bool:
    return is_supported_canonical_engine_state(payload)


def _serialize_legacy_game_state(
    game: KriegspielGame,
    *,
    schema_version: int = 2,
    include_scoresheets: bool = True,
) -> dict[str, Any]:
    serialized_moves = [
        {
            "question_type": move.question_type.name,
            "move_uci": move.chess_move.uci() if move.chess_move is not None else None,
        }
        for move in game.possible_to_ask
    ]
    serialized_moves.sort(key=lambda item: (item["question_type"], item["move_uci"] or ""))

    payload: dict[str, Any] = {
        "schema_version": schema_version,
        "ruleset_id": game.ruleset_id,
        "any_rule": game._any_rule,  # noqa: SLF001
        "must_use_pawns": game.must_use_pawns,
        "game_over": game.game_over,
        "board_fen": game._board.fen(),  # noqa: SLF001
        "move_stack": [move.uci() for move in game._board.move_stack],  # noqa: SLF001
        "possible_to_ask": serialized_moves,
    }
    if include_scoresheets:
        payload["white_scoresheet"] = serialize_scoresheet(getattr(game, "_whites_scoresheet", None))  # noqa: SLF001
        payload["black_scoresheet"] = serialize_scoresheet(getattr(game, "_blacks_scoresheet", None))  # noqa: SLF001
    return payload


def _deserialize_legacy_game_state(payload: dict[str, Any]) -> KriegspielGame:
    ruleset_id = payload.get("ruleset_id")
    if not isinstance(ruleset_id, str):
        any_rule = bool(payload.get("any_rule", True))
        ruleset_id = "berkeley_any" if any_rule else "berkeley"
    game = create_new_game(rule_variant=ruleset_id)

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
    game._whites_scoresheet = deserialize_scoresheet(payload.get("white_scoresheet"), fallback_color="white")  # noqa: SLF001
    game._blacks_scoresheet = deserialize_scoresheet(payload.get("black_scoresheet"), fallback_color="black")  # noqa: SLF001
    _repair_possible_to_ask(game)
    return game


def _repair_possible_to_ask(game: KriegspielGame) -> None:
    current = list(getattr(game, "_possible_to_ask", []))  # noqa: SLF001
    if current or getattr(game, "_game_over", False):  # noqa: SLF001
        return

    game._generate_possible_to_ask_list()  # noqa: SLF001
    if not getattr(game, "_must_use_pawns", False):  # noqa: SLF001
        return

    pawn_capture_factory = getattr(game, "_generate_possible_pawn_captures", None) or getattr(
        game, "_generate_posible_pawn_captures", None
    )
    if pawn_capture_factory is None:
        raise AttributeError("BerkeleyGame pawn-capture generator is unavailable")
    game._possible_to_ask = list(pawn_capture_factory())  # noqa: SLF001


def _deserialize_ks_move(item: dict[str, Any]) -> KriegspielMove:
    question = QuestionAnnouncement[item["question_type"]]
    move_uci = item.get("move_uci")
    if move_uci is None:
        return KriegspielMove(question)
    return KriegspielMove(question, chess.Move.from_uci(move_uci))


def _answer_payload(game: KriegspielGame, answer: Any) -> dict[str, Any]:
    capture_square = chess.square_name(answer.capture_at_square) if answer.capture_at_square is not None else None
    captured_piece = answer.captured_piece_announcement
    special = answer.special_announcement

    return {
        "move_done": bool(answer.move_done),
        "announcement": answer.main_announcement.name,
        "special_announcement": None if special is None or special == SpecialCaseAnnouncement.NONE else special.name,
        "capture_square": capture_square,
        "captured_piece_announcement": (
            captured_piece.name if isinstance(captured_piece, CapturedPieceAnnouncement) else None
        ),
        "next_turn_pawn_tries": answer.next_turn_pawn_tries,
        "next_turn_has_pawn_capture": answer.next_turn_has_pawn_capture,
        "full_fen": game._board.fen(),  # noqa: SLF001
        "white_fen": visible_fen(game, "white"),
        "black_fen": visible_fen(game, "black"),
        "turn": "white" if game.turn == chess.WHITE else "black",
        "game_over": bool(game.game_over),
    }


def _serialize_color(value: Any) -> str | None:
    if value is chess.WHITE:
        return "white"
    if value is chess.BLACK:
        return "black"
    if value in {"white", "black"}:
        return value
    return None


def _deserialize_color(value: Any, *, fallback: PlayerColor) -> PlayerColor:
    if isinstance(value, str) and value.lower() in {"white", "black"}:
        return value.lower()
    return fallback


def _serialize_scoresheet_turn(turn: Any, *, own: bool) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for pair in turn or []:
        if not isinstance(pair, tuple) or len(pair) != 2:
            continue
        question, answer = pair
        serialized.append(
            {
                "question": _serialize_question(question, own=own),
                "answer": _serialize_answer(answer),
            }
        )
    return serialized


def _deserialize_scoresheet_turn(turn: Any, *, own: bool) -> list[tuple[Any, KriegspielAnswer]]:
    deserialized: list[tuple[Any, KriegspielAnswer]] = []
    for pair in turn or []:
        if not isinstance(pair, dict):
            continue
        question = _deserialize_question(pair.get("question"), own=own)
        answer = _deserialize_answer(pair.get("answer"))
        deserialized.append((question, answer))
    return deserialized


def _serialize_question(question: Any, *, own: bool) -> dict[str, Any]:
    if own:
        move = question if isinstance(question, KriegspielMove) else None
        return {
            "question_type": move.question_type.name if move is not None else "NONE",
            "move_uci": move.chess_move.uci() if move is not None and move.chess_move is not None else None,
        }

    announcement = question if isinstance(question, QuestionAnnouncement) else None
    return {"question_type": announcement.name if announcement is not None else "NONE"}


def _deserialize_question(payload: Any, *, own: bool) -> Any:
    data = payload if isinstance(payload, dict) else {}
    question = QuestionAnnouncement[data.get("question_type", "NONE")]
    if not own:
        return question

    move_uci = data.get("move_uci")
    if move_uci is None:
        return KriegspielMove(question)
    return KriegspielMove(question, chess.Move.from_uci(move_uci))


def _serialize_answer(answer: Any) -> dict[str, Any]:
    if not isinstance(answer, KriegspielAnswer):
        return {
            "main_announcement": "ILLEGAL_MOVE",
            "special_announcement": None,
            "capture_square": None,
            "captured_piece_announcement": None,
            "next_turn_pawn_tries": None,
            "next_turn_has_pawn_capture": None,
            "checks": [],
            "move_done": False,
        }

    special = answer.special_announcement
    checks = []
    for check in (answer.check_1, answer.check_2):
        if isinstance(check, SpecialCaseAnnouncement):
            checks.append(check.name)

    return {
        "main_announcement": answer.main_announcement.name,
        "special_announcement": None if special in {None, SpecialCaseAnnouncement.NONE} else special.name,
        "capture_square": chess.square_name(answer.capture_at_square) if answer.capture_at_square is not None else None,
        "captured_piece_announcement": (
            answer.captured_piece_announcement.name if answer.captured_piece_announcement is not None else None
        ),
        "next_turn_pawn_tries": answer.next_turn_pawn_tries,
        "next_turn_has_pawn_capture": answer.next_turn_has_pawn_capture,
        "checks": checks,
        "move_done": bool(answer.move_done),
    }


def _deserialize_answer(payload: Any) -> KriegspielAnswer:
    data = payload if isinstance(payload, dict) else {}
    kwargs: dict[str, Any] = {}
    main = MainAnnouncement[data.get("main_announcement", "ILLEGAL_MOVE")]
    capture_square = data.get("capture_square")
    if main == MainAnnouncement.CAPTURE_DONE and isinstance(capture_square, str):
        kwargs["capture_at_square"] = chess.parse_square(capture_square)
    captured_piece = data.get("captured_piece_announcement")
    if isinstance(captured_piece, str):
        kwargs["captured_piece_announcement"] = CapturedPieceAnnouncement[captured_piece]
    next_turn_pawn_tries = data.get("next_turn_pawn_tries")
    if isinstance(next_turn_pawn_tries, int):
        kwargs["next_turn_pawn_tries"] = next_turn_pawn_tries
    next_turn_has_pawn_capture = data.get("next_turn_has_pawn_capture")
    if isinstance(next_turn_has_pawn_capture, bool):
        kwargs["next_turn_has_pawn_capture"] = next_turn_has_pawn_capture

    special_name = data.get("special_announcement")
    if isinstance(special_name, str):
        special = SpecialCaseAnnouncement[special_name]
        checks = [SpecialCaseAnnouncement[name] for name in data.get("checks", []) if isinstance(name, str)]
        if special == SpecialCaseAnnouncement.CHECK_DOUBLE and len(checks) >= 2:
            kwargs["special_announcement"] = (SpecialCaseAnnouncement.CHECK_DOUBLE, tuple(checks[:2]))
        else:
            kwargs["special_announcement"] = special

    return KriegspielAnswer(main, **kwargs)
