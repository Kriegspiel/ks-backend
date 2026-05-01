from __future__ import annotations

from types import SimpleNamespace

from app.services import state_projection as projection


def test_state_projection_public_announcement_helpers_cover_unknown_and_capture_paths() -> None:
    assert projection._format_public_announcement("UNKNOWN", None) == ""
    assert projection._format_public_announcement("CAPTURE_DONE", "d4") == "Capture done at D4"
    assert projection._format_public_announcement("CAPTURE_DONE", "d4", captured_piece_announcement="PAWN") == "Pawn captured at D4"
    assert projection._format_public_announcement("CAPTURE_DONE", "d4", captured_piece_announcement="KNIGHT") == "Knight captured at D4"
    assert projection._next_turn_message(next_turn_pawn_tries=2, next_turn_has_pawn_capture=None, next_turn_pawn_try_squares=None) == "2 pawn tries"
    assert projection._next_turn_message(next_turn_pawn_tries=None, next_turn_has_pawn_capture=True, next_turn_pawn_try_squares=None) == "Has pawn capture"
    assert projection._next_turn_message(next_turn_pawn_tries=None, next_turn_has_pawn_capture=False, next_turn_pawn_try_squares=None) == "No pawn captures"
    assert projection._next_turn_message(next_turn_pawn_tries=None, next_turn_has_pawn_capture=None, next_turn_pawn_try_squares=["e4", "c2"]) == "Pawn tries from E4, C2"
    assert projection._scoresheet_answer_texts(
        {
            "main_announcement": "REGULAR_MOVE",
            "special_announcement": "CHECK_DOUBLE",
            "capture_square": None,
        }
    ) == ["Move complete", "Double check"]


def test_state_projection_turn_and_referee_helpers_skip_unrecognized_entries(monkeypatch) -> None:
    assert projection._build_turn_announcement({"announcement": "SECRET_MOVE"}, perspective="own") is None

    turns = projection.build_referee_turns(
        [
            {"ply": 1, "color": "mystery", "question_type": "COMMON", "announcement": "REGULAR_MOVE"},
            {"ply": 2, "color": "black", "question_type": "COMMON", "announcement": "SECRET_MOVE"},
        ]
    )
    assert turns == [
        {
            "turn": 1,
            "white": [
                {
                    "kind": "move",
                    "actor": "self",
                    "prompt": "Move attempt",
                    "message": "Move attempt — Move complete",
                    "messages": ["Move complete"],
                    "move_uci": None,
                    "question_type": "COMMON",
                }
            ],
            "black": [],
        }
    ]

    monkeypatch.setattr(
        projection,
        "build_viewer_referee_turns",
        lambda **kwargs: [{"turn": 1, "white": "bad", "black": [None, {"message": "ok"}]}],  # noqa: ARG005
    )
    assert projection.build_viewer_referee_log(viewer_color="white", stored_scoresheet=None) == [
        {
            "ply": 2,
            "announcement": "ok",
            "special_announcement": None,
            "capture_square": None,
            "timestamp": None,
        }
    ]


def test_state_projection_scoresheet_reconstruction_and_normalization_cover_fallbacks() -> None:
    reconstructed = projection.reconstruct_scoresheets_from_moves(
        [
            {
                "question_type": "COMMON",
                "announcement": "REGULAR_MOVE",
                "move_done": True,
                "uci": "e2e4",
            }
        ]
    )
    assert reconstructed["white"]["last_move_number"] == 1
    assert reconstructed["black"]["moves_opponent"][0][0]["question"]["question_type"] == "COMMON"

    assert projection._normalize_scoresheet_entry(None, perspective="own") is None
    assert (
        projection._normalize_scoresheet_entry(
            {
                "question": {"question_type": "COMMON", "move_uci": "e2e4"},
                "answer": {"main_announcement": "SECRET"},
            },
            perspective="own",
        )
        is None
    )


def test_state_projection_scoresheet_texts_include_rule_specific_announcements() -> None:
    assert projection._scoresheet_answer_texts(
        {
            "main_announcement": "CAPTURE_DONE",
            "capture_square": "d4",
            "captured_piece_announcement": "PIECE",
            "next_turn_has_pawn_capture": True,
        }
    ) == ["Piece captured at D4"]

    assert projection._scoresheet_answer_texts(
        {
            "main_announcement": "REGULAR_MOVE",
            "next_turn_pawn_tries": 2,
            "dropped_piece_announcement": "KNIGHT",
            "promotion_announced": True,
        }
    ) == ["Move complete", "Knight dropped", "Promotion"]

    assert projection.build_viewer_scoresheet(
        viewer_color="white",
        stored_scoresheet={
            "color": "white",
            "last_move_number": 1,
            "moves_own": [
                [
                    {
                        "question": {"question_type": "COMMON", "move_uci": "e2e4"},
                        "answer": {"main_announcement": "REGULAR_MOVE", "next_turn_pawn_tries": 0},
                    }
                ]
            ],
            "moves_opponent": [[]],
        },
    )["turns"] == [
        {
            "turn": 1,
            "white": [
                {
                    "kind": "move",
                    "actor": "self",
                    "prompt": "Move attempt",
                    "message": "Move attempt — Move complete",
                    "messages": ["Move complete"],
                    "move_uci": "e2e4",
                    "question_type": "COMMON",
                }
            ],
            "black": [
                {
                    "kind": "status",
                    "actor": "opponent",
                    "prompt": None,
                    "message": "No pawn captures",
                    "messages": ["No pawn captures"],
                    "move_uci": None,
                    "question_type": None,
                }
            ],
        }
    ]


def test_state_projection_possible_actions_and_referee_log_cover_remaining_public_branches() -> None:
    engine = SimpleNamespace(
        possible_to_ask=[
            SimpleNamespace(question_type=SimpleNamespace(name="ASK_ANY"), chess_move=None),
        ]
    )

    actions = projection.compute_possible_actions(
        engine=engine,
        game_state="active",
        viewer_color="white",
        turn="white",
    )
    referee_log = projection.build_referee_log(
        [
            {
                "ply": 1,
                "announcement": "REGULAR_MOVE",
                "special_announcement": "CHECK_FILE",
                "capture_square": "d4",
                "timestamp": None,
            },
            {
                "ply": 2,
                "announcement": "SECRET_MOVE",
                "special_announcement": "CHECK_RANK",
                "capture_square": None,
                "timestamp": None,
            }
        ]
    )

    assert actions == ["ask_any"]
    assert projection.compute_possible_actions(
        engine=engine,
        game_state="active",
        viewer_color="white",
        turn="white",
        rule_variant="cincinnati",
    ) == []
    assert projection.compute_possible_actions(
        engine=engine,
        game_state="active",
        viewer_color="white",
        turn="white",
        rule_variant="english",
    ) == ["ask_any"]
    assert projection.compute_possible_actions(
        engine=engine,
        game_state="active",
        viewer_color="white",
        turn="white",
        rule_variant="crazykrieg",
    ) == ["ask_any"]
    assert projection._build_turn_announcement({"announcement": "NONSENSE"}, perspective="own") is None
    assert [item["announcement"] for item in referee_log] == ["REGULAR_MOVE", "CHECK_FILE", "CHECK_RANK"]


def test_state_projection_viewer_scoresheet_skips_empty_turns() -> None:
    scoresheet = projection.build_viewer_scoresheet(
        viewer_color="white",
        stored_scoresheet={
            "color": "white",
            "last_move_number": 2,
            "moves_own": [
                [],
                [
                    {
                        "question": {"question_type": "COMMON", "move_uci": "e2e4"},
                        "answer": {"main_announcement": "REGULAR_MOVE"},
                    }
                ],
            ],
            "moves_opponent": [[], []],
        },
    )

    assert [turn["turn"] for turn in scoresheet["turns"]] == [2]


def test_state_projection_black_viewer_orders_turn_start_status_before_black_attempts() -> None:
    scoresheet = projection.build_viewer_scoresheet(
        viewer_color="black",
        stored_scoresheet={
            "color": "black",
            "last_move_number": 1,
            "moves_own": [
                [
                    {
                        "question": {"question_type": "COMMON", "move_uci": "d7d5"},
                        "answer": {"main_announcement": "ILLEGAL_MOVE"},
                    }
                ]
            ],
            "moves_opponent": [
                [
                    {
                        "question": {"question_type": "COMMON"},
                        "answer": {"main_announcement": "REGULAR_MOVE", "next_turn_pawn_tries": 1},
                    }
                ]
            ],
        },
    )

    assert [entry["message"] for entry in scoresheet["turns"][0]["black"]] == [
        "1 pawn try",
        "Move attempt — Illegal move",
    ]
