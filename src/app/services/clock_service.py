from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

PlayerColor = Literal["white", "black"]


class ClockService:
    RAPID_BASE_SECONDS = 25 * 60.0
    RAPID_INCREMENT_SECONDS = 10.0

    @staticmethod
    def _seconds_between(start: datetime, end: datetime) -> float:
        return max(0.0, (end - start).total_seconds())

    @classmethod
    def default_time_control(cls, *, now: datetime, active_color: PlayerColor = "white") -> dict[str, Any]:
        return {
            "base": cls.RAPID_BASE_SECONDS,
            "increment": cls.RAPID_INCREMENT_SECONDS,
            "white_remaining": cls.RAPID_BASE_SECONDS,
            "black_remaining": cls.RAPID_BASE_SECONDS,
            "active_color": active_color,
            "last_updated_at": now,
        }

    @classmethod
    def get_remaining(cls, *, time_control: dict[str, Any], now: datetime) -> dict[str, Any]:
        white = float(time_control.get("white_remaining", cls.RAPID_BASE_SECONDS))
        black = float(time_control.get("black_remaining", cls.RAPID_BASE_SECONDS))
        active_color = time_control.get("active_color")
        last_updated_at = time_control.get("last_updated_at")

        if active_color in ("white", "black") and isinstance(last_updated_at, datetime):
            if last_updated_at.tzinfo is None:
                last_updated_at = last_updated_at.replace(tzinfo=UTC)
            elapsed = cls._seconds_between(last_updated_at, now)
            if active_color == "white":
                white = max(0.0, white - elapsed)
            else:
                black = max(0.0, black - elapsed)

        return {
            "white_remaining": white,
            "black_remaining": black,
            "active_color": active_color,
        }

    @classmethod
    def deduct_and_increment(
        cls,
        *,
        time_control: dict[str, Any],
        mover_color: PlayerColor,
        now: datetime,
        move_done: bool,
        next_active_color: PlayerColor,
    ) -> dict[str, Any]:
        projected = cls.get_remaining(time_control=time_control, now=now)
        white = projected["white_remaining"]
        black = projected["black_remaining"]
        increment = float(time_control.get("increment", cls.RAPID_INCREMENT_SECONDS))

        if move_done:
            if mover_color == "white":
                white += increment
            else:
                black += increment

        active_color: PlayerColor = next_active_color if move_done else mover_color
        return {
            "base": float(time_control.get("base", cls.RAPID_BASE_SECONDS)),
            "increment": increment,
            "white_remaining": white,
            "black_remaining": black,
            "active_color": active_color,
            "last_updated_at": now,
        }

    @classmethod
    def check_timeout(cls, *, time_control: dict[str, Any], now: datetime) -> dict[str, Any] | None:
        projected = cls.get_remaining(time_control=time_control, now=now)
        active_color = projected.get("active_color")
        if active_color == "white" and projected["white_remaining"] <= 0:
            return {"winner": "black", "reason": "timeout", "clock": projected}
        if active_color == "black" and projected["black_remaining"] <= 0:
            return {"winner": "white", "reason": "timeout", "clock": projected}
        return None

    @classmethod
    def response_clock(cls, *, time_control: dict[str, Any], now: datetime) -> dict[str, Any]:
        projected = cls.get_remaining(time_control=time_control, now=now)
        return {
            "white_remaining": projected["white_remaining"],
            "black_remaining": projected["black_remaining"],
            "active_color": projected.get("active_color"),
        }
