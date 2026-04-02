from __future__ import annotations

from typing import Any, Literal

from app.services.engine_adapter import full_fen, visible_fen

PlayerColor = Literal["white", "black"]
_ALLOWED_PUBLIC_MAIN_ANNOUNCEMENTS = {
    "CAPTURE_DONE",
    "HAS_ANY",
    "NO_ANY",
}

_ALLOWED_PUBLIC_SPECIAL_ANNOUNCEMENTS = {
    "DRAW_TOOMANYREVERSIBLEMOVES",
    "DRAW_STALEMATE",
    "DRAW_INSUFFICIENT",
    "CHECKMATE_WHITE_WINS",
    "CHECKMATE_BLACK_WINS",
    "CHECK_RANK",
    "CHECK_FILE",
    "CHECK_LONG_DIAGONAL",
    "CHECK_SHORT_DIAGONAL",
    "CHECK_KNIGHT",
    "CHECK_DOUBLE",
}


def project_player_fen(*, engine: Any, viewer_color: PlayerColor, game_state: str) -> str:
    if game_state == "completed":
        return full_fen(engine)
    return visible_fen(engine, viewer_color)


def allowed_moves_for_player(*, engine: Any, game_state: str, viewer_color: PlayerColor, turn: str | None) -> list[str]:
    if game_state != "active" or turn != viewer_color:
        return []

    return sorted(
        option.chess_move.uci()
        for option in engine.possible_to_ask
        if option.question_type.name == "COMMON" and option.chess_move is not None
    )


def compute_possible_actions(*, engine: Any, game_state: str, viewer_color: PlayerColor, turn: str | None) -> list[str]:
    if game_state != "active" or turn != viewer_color:
        return []

    has_move = False
    has_ask_any = False
    for option in engine.possible_to_ask:
        question_type = option.question_type.name
        has_move = has_move or question_type == "COMMON"
        has_ask_any = has_ask_any or question_type == "ASK_ANY"

    actions: list[str] = []
    if has_move:
        actions.append("move")
    if has_ask_any:
        actions.append("ask_any")
    return actions


def build_referee_log(moves: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for move in moves:
        announcement = move.get("announcement")
        special_announcement = move.get("special_announcement")
        base_item = {
            "ply": move.get("ply"),
            "timestamp": move.get("timestamp"),
        }

        if announcement in _ALLOWED_PUBLIC_MAIN_ANNOUNCEMENTS:
            out.append(
                {
                    **base_item,
                    "announcement": announcement,
                    "special_announcement": None,
                    "capture_square": move.get("capture_square"),
                }
            )

        if special_announcement in _ALLOWED_PUBLIC_SPECIAL_ANNOUNCEMENTS:
            out.append(
                {
                    **base_item,
                    "announcement": special_announcement,
                    "special_announcement": special_announcement,
                    "capture_square": None,
                }
            )
    return out
