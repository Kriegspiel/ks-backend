from __future__ import annotations

from typing import Any, Literal

from app.services.engine_adapter import full_fen, visible_fen

PlayerColor = Literal["white", "black"]
_ALLOWED_PUBLIC_MAIN_ANNOUNCEMENTS = {
    "REGULAR_MOVE",
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

_PUBLIC_ANNOUNCEMENT_TEXT = {
    "REGULAR_MOVE": "Move complete",
    "CAPTURE_DONE": "Capture done",
    "HAS_ANY": "Has pawn captures",
    "NO_ANY": "No pawn captures",
    "DRAW_TOOMANYREVERSIBLEMOVES": "Draw by too many reversible moves",
    "DRAW_STALEMATE": "Draw by stalemate",
    "DRAW_INSUFFICIENT": "Draw by insufficient material",
    "CHECKMATE_WHITE_WINS": "Checkmate — White wins",
    "CHECKMATE_BLACK_WINS": "Checkmate — Black wins",
    "CHECK_RANK": "Check on rank",
    "CHECK_FILE": "Check on file",
    "CHECK_LONG_DIAGONAL": "Check on long diagonal",
    "CHECK_SHORT_DIAGONAL": "Check on short diagonal",
    "CHECK_KNIGHT": "Check by knight",
    "CHECK_DOUBLE": "Double check",
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


def _format_public_announcement(code: str, capture_square: str | None) -> str:
    text = _PUBLIC_ANNOUNCEMENT_TEXT.get(code)
    if not text:
        return ""
    if code == "CAPTURE_DONE" and capture_square:
        return f"{text} at {capture_square.upper()}"
    return text


def _move_prompt_label(move: dict[str, Any]) -> str:
    question_type = str(move.get("question_type") or "").upper()
    if question_type == "ASK_ANY":
        return "Ask any pawn captures"

    uci = move.get("uci")
    if isinstance(uci, str) and uci.strip():
        return uci.strip().lower()

    return "Move"


def _move_announcements(move: dict[str, Any]) -> list[str]:
    out: list[str] = []
    announcement = move.get("announcement")
    special_announcement = move.get("special_announcement")
    capture_square = move.get("capture_square")

    if announcement in _ALLOWED_PUBLIC_MAIN_ANNOUNCEMENTS:
        out.append(_format_public_announcement(announcement, capture_square))

    if special_announcement in _ALLOWED_PUBLIC_SPECIAL_ANNOUNCEMENTS:
        out.append(_format_public_announcement(special_announcement, None))

    return [item for item in out if item]


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


def build_referee_turns(moves: list[dict[str, Any]]) -> list[dict[str, Any]]:
    turns: dict[int, dict[str, Any]] = {}
    for index, move in enumerate(moves):
        ply = move.get("ply")
        if isinstance(ply, int) and ply > 0:
            turn = (ply + 1) // 2
        else:
            turn = (index // 2) + 1

        color = str(move.get("color") or "").lower()
        if color not in {"white", "black"}:
            color = "white" if index % 2 == 0 else "black"

        announcements = _move_announcements(move)
        if not announcements:
            continue

        label = _move_prompt_label(move)
        entry = turns.setdefault(turn, {"turn": turn, "white": [], "black": []})
        entry[color].append(label + " — " + " · ".join(announcements))

    return [turns[key] for key in sorted(turns)]
