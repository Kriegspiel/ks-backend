from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.models.game import GameReviewResponse
from app.models.tutor import TutorModelOutput, TutorProfileResponse


USER_ID = "507f1f77bcf86cd799439011"
GAME_ID = "507f1f77bcf86cd799439022"
GAME_CODE = "ABC234"
NOW = datetime(2026, 7, 27, 12, 0, tzinfo=UTC)


def _answer(
    main: str,
    *,
    capture_square: str | None = None,
    checks: list[str] | None = None,
) -> dict:
    return {
        "main": main,
        "capture_square": capture_square,
        "checks": checks or [],
    }


def make_review(
    *,
    turns: int = 4,
    completed: bool = True,
    participant: bool = True,
    winner: str | None = "white",
    opponent_role: str = "bot",
    extra_attempts: int = 0,
) -> GameReviewResponse:
    moves: list[dict] = []
    ply = 1
    for turn in range(1, turns + 1):
        if turn == 1:
            moves.append(
                {
                    "ply": ply,
                    "color": "white",
                    "question_type": "ASK_ANY",
                    "uci": None,
                    "answer": _answer("Yes, a pawn capture exists"),
                    "move_done": False,
                    "timestamp": NOW + timedelta(seconds=ply),
                    "replay_fen": {"full": "hidden-full", "white": "white-view", "black": "black-view"},
                }
            )
            ply += 1
            for index in range(extra_attempts + 1):
                moves.append(
                    {
                        "ply": ply,
                        "color": "white",
                        "question_type": "COMMON",
                        "uci": f"a{(index % 7) + 1}a{(index % 7) + 2}",
                        "answer": _answer("No"),
                        "move_done": False,
                    }
                )
                ply += 1
        moves.append(
            {
                "ply": ply,
                "color": "white",
                "question_type": "COMMON",
                "uci": "e2e4",
                "answer": _answer(
                    "Move accepted",
                    capture_square="e4" if turn == 2 else None,
                    checks=["check"] if turn == 3 else [],
                ),
                "move_done": True,
            }
        )
        ply += 1
        moves.append(
            {
                "ply": ply,
                "color": "black",
                "question_type": "COMMON",
                "uci": "e7e5",
                "answer": _answer("Opponent move accepted"),
                "move_done": True,
            }
        )
        ply += 1

    return GameReviewResponse.model_validate(
        {
            "game": {
                "game_id": GAME_ID,
                "game_code": GAME_CODE,
                "rule_variant": "berkeley_any",
                "state": "completed" if completed else "active",
                "opponent_type": "bot" if opponent_role == "bot" else "human",
                "white": {
                    "username": "fil",
                    "connected": True,
                    "role": "user",
                    "elo": 1200,
                    "ratings": {},
                },
                "black": {
                    "username": "opponent",
                    "connected": True,
                    "role": opponent_role,
                    "elo": 1200,
                    "ratings": {},
                },
                "turn": None if completed else "white",
                "move_number": turns + 1,
                "created_at": NOW - timedelta(hours=1),
                "updated_at": NOW,
                "result": {"winner": winner, "reason": "checkmate"} if completed else None,
            },
            "transcript": {
                "game_id": GAME_ID,
                "rule_variant": "berkeley_any",
                "viewer_color": "white" if participant else None,
                "moves": moves,
            },
        }
    )


def sample_model_output() -> TutorModelOutput:
    return TutorModelOutput.model_validate(
        {
            "overview": "You gathered useful information before committing to moves.",
            "result_context": "The result supports the observations but does not prove every decision was optimal.",
            "key_moments": [
                {
                    "turn": 1,
                    "title": "Structured pawn probe",
                    "observation": "You asked Any? before testing a pawn capture.",
                    "why_it_matters": "The sequence reduced uncertainty.",
                    "suggestion": "Keep the probe order consistent.",
                    "evidence": ["T1A1", "T1A2"],
                    "confidence": "high",
                }
            ],
            "strengths": [
                {
                    "skill": "Probe discipline",
                    "evidence": "The first turn used a public question before move attempts.",
                    "action": "Repeat this sequence when Any? is available.",
                }
            ],
            "improvements": [
                {
                    "skill": "Attempt efficiency",
                    "evidence": "The first completed move required multiple attempts.",
                    "action": "Order candidate moves before submitting the first attempt.",
                }
            ],
            "next_drill": {
                "title": "Three-candidate scan",
                "instructions": "Name three plausible moves before the first attempt on each turn.",
                "success_criterion": "Complete five turns without an unplanned second attempt.",
            },
            "profile_update": {
                "summary": "Your probe discipline is emerging; attempt ordering is the next focus.",
                "strengths": ["Probe discipline"],
                "focus_areas": [
                    {
                        "skill": "Attempt efficiency",
                        "reason": "Repeated attempts can reveal an unordered candidate process.",
                        "next_action": "Use a three-candidate scan before moving.",
                    }
                ],
            },
        }
    )


def empty_profile(*, reviewed_games: int = 0) -> TutorProfileResponse:
    return TutorProfileResponse(
        reviewed_games=reviewed_games,
        ready=reviewed_games >= 5,
        games_until_ready=max(0, 5 - reviewed_games),
        summary="Tutor is learning from your reviewed games.",
        strengths=[],
        focus_areas=[],
        updated_at=None,
    )
