from __future__ import annotations

from typing import Any, Literal

from app.services.engine_adapter import full_fen, visible_fen

PlayerColor = Literal["white", "black"]
_ALLOWED_PUBLIC_ANNOUNCEMENTS = {
    "CAPTURE_DONE",
    "HAS_ANY",
    "NO_ANY",
    "CHECK_RANK",
    "CHECK_FILE",
    "CHECK_LONG_DIAGONAL",
    "CHECK_SHORT_DIAGONAL",
    "CHECK_KNIGHT",
    "CHECKMATE_WHITE_WINS",
    "CHECKMATE_BLACK_WINS",
    "DRAW_STALEMATE",
    "DRAW_INSUFFICIENT_MATERIAL",
    "DRAW_HALFMOVE_LIMIT",
}


def project_player_fen(*, engine: Any, viewer_color: PlayerColor, game_state: str) -> str:
    if game_state == "completed":
        return full_fen(engine)
    return visible_fen(engine, viewer_color)


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
        if announcement not in _ALLOWED_PUBLIC_ANNOUNCEMENTS:
            continue

        out.append(
            {
                "ply": move.get("ply"),
                "announcement": announcement,
                "special_announcement": move.get("special_announcement"),
                "capture_square": move.get("capture_square"),
                "timestamp": move.get("timestamp"),
            }
        )
    return out
