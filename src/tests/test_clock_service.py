from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.services.clock_service import ClockService


def test_get_remaining_decrements_active_clock() -> None:
    now = datetime(2026, 3, 26, 12, 0, tzinfo=UTC)
    time_control = ClockService.default_time_control(now=now, active_color="white")

    projected = ClockService.get_remaining(time_control=time_control, now=now + timedelta(seconds=3.5))

    assert round(projected["white_remaining"], 1) == 1496.5
    assert projected["black_remaining"] == 1500.0
    assert projected["active_color"] == "white"


def test_deduct_and_increment_applies_increment_on_legal_move() -> None:
    now = datetime(2026, 3, 26, 12, 0, tzinfo=UTC)
    time_control = ClockService.default_time_control(now=now, active_color="white")

    updated = ClockService.deduct_and_increment(
        time_control=time_control,
        mover_color="white",
        now=now + timedelta(seconds=5),
        move_done=True,
        next_active_color="black",
    )

    assert updated["white_remaining"] == 1505.0
    assert updated["black_remaining"] == 1500.0
    assert updated["active_color"] == "black"


def test_check_timeout_reports_winner_from_active_color() -> None:
    now = datetime(2026, 3, 26, 12, 0, tzinfo=UTC)
    time_control = ClockService.default_time_control(now=now, active_color="black")
    time_control["black_remaining"] = 1.0

    timeout = ClockService.check_timeout(time_control=time_control, now=now + timedelta(seconds=2))

    assert timeout is not None
    assert timeout["winner"] == "white"
    assert timeout["reason"] == "timeout"
    assert timeout["clock"]["black_remaining"] == 0.0
