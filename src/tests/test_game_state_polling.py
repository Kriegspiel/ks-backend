from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from bson import ObjectId
from fastapi.testclient import TestClient

from app.config import Settings
from app.dependencies import get_current_user
from app.main import create_app
from app.models.user import UserModel
from app.routers.game import get_game_service
from app.services.engine_adapter import (
    INTERMEDIATE_CANONICAL_ENGINE_STATE_SCHEMA_VERSION,
    _serialize_legacy_game_state,
    create_new_game,
    serialize_game_state,
)
from app.services.engine_adapter import ask_any, attempt_move
from app.services.game_service import GameForbiddenError, GameService
from app.services.state_projection import build_referee_log, build_referee_turns


class FakeCursor:
    def __init__(self, docs: list[dict]):
        self._docs = docs

    def sort(self, field: str, direction: int):
        self._docs.sort(key=lambda x: x[field], reverse=direction < 0)
        return self

    def limit(self, count: int):
        self._docs = self._docs[:count]
        return self

    def __aiter__(self):
        self._idx = 0
        return self

    async def __anext__(self):
        if self._idx >= len(self._docs):
            raise StopAsyncIteration
        value = self._docs[self._idx]
        self._idx += 1
        return value


class FakeGamesCollection:
    def __init__(self):
        self.docs: list[dict] = []

    async def find_one(self, query: dict, projection: dict | None = None):
        for doc in self.docs:
            if self._matches(doc, query):
                return doc
        return None

    async def find_one_and_update(self, query: dict, update: dict, return_document=None):
        for doc in self.docs:
            if self._matches(doc, query):
                for key, value in update.get("$set", {}).items():
                    self._assign(doc, key, value)
                return doc
        return None

    def find(self, query: dict):
        return FakeCursor([d for d in self.docs if self._matches(d, query)])

    def _matches(self, doc: dict, query: dict) -> bool:
        if "$or" in query:
            return any(self._matches(doc, branch) for branch in query["$or"])

        for key, expected in query.items():
            value = self._resolve(doc, key)
            if value != expected:
                return False
        return True

    @staticmethod
    def _resolve(doc: dict, key: str):
        current = doc
        for part in key.split("."):
            if not isinstance(current, dict):
                return None
            current = current.get(part)
        return current

    @staticmethod
    def _assign(doc: dict, key: str, value):
        parts = key.split(".")
        current = doc
        for part in parts[:-1]:
            next_value = current.get(part)
            if not isinstance(next_value, dict):
                next_value = {}
                current[part] = next_value
            current = next_value
        current[parts[-1]] = value


@pytest.fixture
def active_game_doc() -> dict:
    gid = ObjectId()
    now = datetime.now(UTC)
    engine = create_new_game(any_rule=True)

    return {
        "_id": gid,
        "game_code": "A7K2M9",
        "rule_variant": "berkeley_any",
        "creator_color": "white",
        "white": {"user_id": "u1", "username": "w", "connected": True},
        "black": {"user_id": "u2", "username": "b", "connected": True},
        "state": "active",
        "turn": "white",
        "move_number": 2,
        "moves": [
            {
                "ply": 1,
                "color": "white",
                "question_type": "COMMON",
                "uci": "e2e4",
                "announcement": "REGULAR_MOVE",
                "special_announcement": None,
                "capture_square": None,
                "move_done": True,
                "timestamp": now,
            },
            {
                "ply": 2,
                "color": "black",
                "question_type": "ASK_ANY",
                "uci": None,
                "announcement": "HAS_ANY",
                "special_announcement": None,
                "capture_square": None,
                "move_done": False,
                "timestamp": now,
            },
        ],
        "engine_state": serialize_game_state(engine),
        "created_at": now,
        "updated_at": now,
    }


@pytest.fixture
def corrupted_has_any_game_doc() -> dict:
    gid = ObjectId()
    now = datetime.now(UTC)
    engine = create_new_game(any_rule=True)
    attempt_move(engine, "e2e4")
    attempt_move(engine, "b7c6")
    attempt_move(engine, "d7d6")
    attempt_move(engine, "e4d5")
    attempt_move(engine, "e4e5")
    attempt_move(engine, "c8e6")
    ask_any(engine)
    payload = _serialize_legacy_game_state(engine)
    payload["possible_to_ask"] = [
        {"question_type": "COMMON", "move_uci": "a2b3"},
        {"question_type": "COMMON", "move_uci": "b2a3"},
        {"question_type": "COMMON", "move_uci": "b2c3"},
        {"question_type": "COMMON", "move_uci": "c2b3"},
        {"question_type": "COMMON", "move_uci": "c2d3"},
        {"question_type": "COMMON", "move_uci": "d2c3"},
        {"question_type": "COMMON", "move_uci": "d2e3"},
        {"question_type": "COMMON", "move_uci": "f2e3"},
        {"question_type": "COMMON", "move_uci": "f2g3"},
        {"question_type": "COMMON", "move_uci": "g2f3"},
        {"question_type": "COMMON", "move_uci": "g2h3"},
        {"question_type": "COMMON", "move_uci": "h2g3"},
    ]

    return {
        "_id": gid,
        "game_code": "A7K2M9",
        "rule_variant": "berkeley_any",
        "creator_color": "white",
        "white": {"user_id": "u1", "username": "fil", "connected": True},
        "black": {"user_id": "u2", "username": "randobot", "connected": True},
        "state": "active",
        "turn": "white",
        "move_number": 5,
        "moves": [
            {"ply": 1, "color": "white", "question_type": "COMMON", "uci": "e2e4", "announcement": "REGULAR_MOVE", "special_announcement": None, "capture_square": None, "move_done": True, "timestamp": now},
            {"ply": 2, "color": "black", "question_type": "COMMON", "uci": "b7c6", "announcement": "ILLEGAL_MOVE", "special_announcement": None, "capture_square": None, "move_done": False, "timestamp": now},
            {"ply": 3, "color": "black", "question_type": "COMMON", "uci": "d7d6", "announcement": "REGULAR_MOVE", "special_announcement": None, "capture_square": None, "move_done": True, "timestamp": now},
            {"ply": 4, "color": "white", "question_type": "COMMON", "uci": "e4d5", "announcement": "ILLEGAL_MOVE", "special_announcement": None, "capture_square": None, "move_done": False, "timestamp": now},
            {"ply": 5, "color": "white", "question_type": "COMMON", "uci": "e4e5", "announcement": "REGULAR_MOVE", "special_announcement": None, "capture_square": None, "move_done": True, "timestamp": now},
            {"ply": 6, "color": "black", "question_type": "COMMON", "uci": "c8e6", "announcement": "REGULAR_MOVE", "special_announcement": None, "capture_square": None, "move_done": True, "timestamp": now},
            {"ply": 7, "color": "white", "question_type": "ASK_ANY", "uci": None, "announcement": "HAS_ANY", "special_announcement": None, "capture_square": None, "move_done": False, "timestamp": now},
        ],
        "engine_state": payload,
        "created_at": now,
        "updated_at": now,
        "time_control": {"white_remaining": 600.0, "black_remaining": 600.0, "increment_seconds": 3, "active_color": "white", "last_updated_at": now},
    }


@pytest.mark.asyncio
async def test_get_game_state_returns_projected_view_and_actions(active_game_doc: dict) -> None:
    games = FakeGamesCollection()
    games.docs.append(active_game_doc)
    service = GameService(games)

    white_state = await service.get_game_state(game_id=str(active_game_doc["_id"]), user_id="u1")
    black_state = await service.get_game_state(game_id=str(active_game_doc["_id"]), user_id="u2")

    assert white_state.your_color == "white"
    assert black_state.your_color == "black"
    assert "p" not in white_state.your_fen.split(" ")[0]
    assert "P" not in black_state.your_fen.split(" ")[0]
    assert white_state.possible_actions == ["move", "ask_any"]
    assert "e2e4" in white_state.allowed_moves
    assert "a2b3" in white_state.allowed_moves
    assert black_state.possible_actions == []
    assert black_state.allowed_moves == []
    assert white_state.scoresheet.viewer_color == "white"
    assert [entry.message for entry in white_state.scoresheet.turns[0].white] == ["Move attempt — Move complete"]
    assert [entry.message for entry in black_state.scoresheet.turns[0].white] == ["Opponent move — Move complete"]
    assert len(white_state.referee_log) == 2
    assert white_state.referee_log[0].announcement == "Move attempt — Move complete"
    assert white_state.referee_log[1].announcement == "Opponent asked any pawn captures — Has pawn captures"
    assert len(black_state.referee_log) == 2
    assert black_state.referee_log[0].announcement == "Opponent move — Move complete"
    assert black_state.referee_log[1].announcement == "Ask any pawn captures — Has pawn captures"
    assert [turn.model_dump() for turn in white_state.referee_turns] == [{"turn": 1, "white": [{"kind": "move", "actor": "self", "prompt": "Move attempt", "message": "Move attempt — Move complete", "messages": ["Move complete"], "move_uci": "e2e4", "question_type": "COMMON"}], "black": [{"kind": "ask_any", "actor": "opponent", "prompt": "Opponent asked any pawn captures", "message": "Opponent asked any pawn captures — Has pawn captures", "messages": ["Has pawn captures"], "move_uci": None, "question_type": "ASK_ANY"}]}]
    assert [turn.model_dump() for turn in black_state.referee_turns] == [{"turn": 1, "white": [{"kind": "move", "actor": "opponent", "prompt": "Opponent move", "message": "Opponent move — Move complete", "messages": ["Move complete"], "move_uci": None, "question_type": "COMMON"}], "black": [{"kind": "ask_any", "actor": "self", "prompt": "Ask any pawn captures", "message": "Ask any pawn captures — Has pawn captures", "messages": ["Has pawn captures"], "move_uci": None, "question_type": "ASK_ANY"}]}]


@pytest.mark.asyncio
async def test_get_game_state_accepts_intermediate_canonical_engine_state(active_game_doc: dict) -> None:
    games = FakeGamesCollection()
    active_game_doc["engine_state"]["schema_version"] = INTERMEDIATE_CANONICAL_ENGINE_STATE_SCHEMA_VERSION
    active_game_doc["engine_state"]["library_version"] = "1.2.6"
    games.docs.append(active_game_doc)
    service = GameService(games)

    state = await service.get_game_state(game_id=str(active_game_doc["_id"]), user_id="u1")

    assert state.state == "active"
    assert state.possible_actions == ["move", "ask_any"]
    assert "e2e4" in state.allowed_moves


@pytest.mark.asyncio
@pytest.mark.parametrize("rule_variant", ["berkeley", "cincinnati", "wild16"])
async def test_get_game_state_hides_stale_ask_any_outside_berkeley_any(active_game_doc: dict, rule_variant: str) -> None:
    games = FakeGamesCollection()
    active_game_doc["rule_variant"] = rule_variant
    games.docs.append(active_game_doc)
    service = GameService(games)

    state = await service.get_game_state(game_id=str(active_game_doc["_id"]), user_id="u1")

    assert state.possible_actions == ["move"]


@pytest.mark.asyncio
async def test_get_game_state_repairs_missing_forced_pawn_captures(corrupted_has_any_game_doc: dict) -> None:
    games = FakeGamesCollection()
    games.docs.append(corrupted_has_any_game_doc)
    service = GameService(games)

    state = await service.get_game_state(game_id=str(corrupted_has_any_game_doc["_id"]), user_id="u1")

    assert state.turn == "white"
    assert state.possible_actions == ["move"]
    assert "e5d6" in state.allowed_moves
    assert "e5f6" in state.allowed_moves


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rule_variant", "expected_message"),
    [
        ("cincinnati", "Has pawn capture"),
        ("wild16", "1 pawn try"),
    ],
)
async def test_get_game_state_surfaces_ruleset_specific_announcements(
    rule_variant: str,
    expected_message: str,
) -> None:
    gid = ObjectId()
    now = datetime.now(UTC)
    engine = create_new_game(rule_variant=rule_variant)
    first = attempt_move(engine, "e2e4")
    second = attempt_move(engine, "d7d5")
    games = FakeGamesCollection()
    games.docs.append(
        {
            "_id": gid,
            "game_code": "A7K2M9",
            "rule_variant": rule_variant,
            "creator_color": "white",
            "white": {"user_id": "u1", "username": "w", "connected": True},
            "black": {"user_id": "u2", "username": "b", "connected": True},
            "state": "active",
            "turn": "white",
            "move_number": 3,
            "moves": [
                {
                    "ply": 1,
                    "color": "white",
                    "question_type": "COMMON",
                    "uci": "e2e4",
                    "announcement": first["announcement"],
                    "special_announcement": first["special_announcement"],
                    "capture_square": first["capture_square"],
                    "captured_piece_announcement": first.get("captured_piece_announcement"),
                    "next_turn_pawn_tries": first.get("next_turn_pawn_tries"),
                    "next_turn_has_pawn_capture": first.get("next_turn_has_pawn_capture"),
                    "move_done": first["move_done"],
                    "timestamp": now,
                },
                {
                    "ply": 2,
                    "color": "black",
                    "question_type": "COMMON",
                    "uci": "d7d5",
                    "announcement": second["announcement"],
                    "special_announcement": second["special_announcement"],
                    "capture_square": second["capture_square"],
                    "captured_piece_announcement": second.get("captured_piece_announcement"),
                    "next_turn_pawn_tries": second.get("next_turn_pawn_tries"),
                    "next_turn_has_pawn_capture": second.get("next_turn_has_pawn_capture"),
                    "move_done": second["move_done"],
                    "timestamp": now,
                },
            ],
            "engine_state": serialize_game_state(engine),
            "created_at": now,
            "updated_at": now,
        }
    )
    service = GameService(games)

    white_state = await service.get_game_state(game_id=str(gid), user_id="u1")

    assert white_state.possible_actions == ["move"]
    assert [entry.message for entry in white_state.referee_turns[0].black] == [
        "No pawn captures",
        "Opponent move — Move complete",
    ]
    assert [entry.message for entry in white_state.referee_turns[1].white] == [expected_message]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rule_variant", "expected_message"),
    [
        ("cincinnati", "No pawn captures"),
        ("wild16", "No pawn captures"),
    ],
)
async def test_get_game_state_blocks_pawn_capture_attempts_after_no_capture_announcement(
    rule_variant: str,
    expected_message: str,
) -> None:
    gid = ObjectId()
    now = datetime.now(UTC)
    engine = create_new_game(rule_variant=rule_variant)
    first = attempt_move(engine, "e2e4")
    second = attempt_move(engine, "e7e5")
    games = FakeGamesCollection()
    games.docs.append(
        {
            "_id": gid,
            "game_code": "A7K2M9",
            "rule_variant": rule_variant,
            "creator_color": "white",
            "white": {"user_id": "u1", "username": "w", "connected": True},
            "black": {"user_id": "u2", "username": "b", "connected": True},
            "state": "active",
            "turn": "white",
            "move_number": 3,
            "moves": [
                {
                    "ply": 1,
                    "color": "white",
                    "question_type": "COMMON",
                    "uci": "e2e4",
                    "announcement": first["announcement"],
                    "special_announcement": first["special_announcement"],
                    "capture_square": first["capture_square"],
                    "captured_piece_announcement": first.get("captured_piece_announcement"),
                    "next_turn_pawn_tries": first.get("next_turn_pawn_tries"),
                    "next_turn_has_pawn_capture": first.get("next_turn_has_pawn_capture"),
                    "move_done": first["move_done"],
                    "timestamp": now,
                },
                {
                    "ply": 2,
                    "color": "black",
                    "question_type": "COMMON",
                    "uci": "e7e5",
                    "announcement": second["announcement"],
                    "special_announcement": second["special_announcement"],
                    "capture_square": second["capture_square"],
                    "captured_piece_announcement": second.get("captured_piece_announcement"),
                    "next_turn_pawn_tries": second.get("next_turn_pawn_tries"),
                    "next_turn_has_pawn_capture": second.get("next_turn_has_pawn_capture"),
                    "move_done": second["move_done"],
                    "timestamp": now,
                },
            ],
            "engine_state": serialize_game_state(engine),
            "created_at": now,
            "updated_at": now,
        }
    )
    service = GameService(games)

    white_state = await service.get_game_state(game_id=str(gid), user_id="u1")

    assert white_state.possible_actions == ["move"]
    assert "e4e5" in white_state.allowed_moves
    assert "e4d5" not in white_state.allowed_moves
    assert "e4f5" not in white_state.allowed_moves
    assert [entry.message for entry in white_state.referee_turns[0].black] == [
        "No pawn captures",
        "Opponent move — Move complete",
    ]
    assert [entry.message for entry in white_state.referee_turns[1].white] == [expected_message]


@pytest.mark.asyncio
async def test_get_game_state_keeps_wild16_private_illegal_attempts_private() -> None:
    gid = ObjectId()
    now = datetime.now(UTC)
    engine = create_new_game(rule_variant="wild16")
    illegal = attempt_move(engine, "e2e5")
    legal = attempt_move(engine, "e2e4")
    games = FakeGamesCollection()
    games.docs.append(
        {
            "_id": gid,
            "game_code": "A7K2M9",
            "rule_variant": "wild16",
            "creator_color": "white",
            "white": {"user_id": "u1", "username": "w", "connected": True},
            "black": {"user_id": "u2", "username": "b", "connected": True},
            "state": "active",
            "turn": "black",
            "move_number": 2,
            "moves": [
                {
                    "ply": 1,
                    "color": "white",
                    "question_type": "COMMON",
                    "uci": "e2e4",
                    "announcement": legal["announcement"],
                    "special_announcement": legal["special_announcement"],
                    "capture_square": legal["capture_square"],
                    "captured_piece_announcement": legal.get("captured_piece_announcement"),
                    "next_turn_pawn_tries": legal.get("next_turn_pawn_tries"),
                    "next_turn_has_pawn_capture": legal.get("next_turn_has_pawn_capture"),
                    "move_done": legal["move_done"],
                    "timestamp": now,
                },
            ],
            "engine_state": serialize_game_state(engine),
            "created_at": now,
            "updated_at": now,
        }
    )
    service = GameService(games)

    white_state = await service.get_game_state(game_id=str(gid), user_id="u1")
    black_state = await service.get_game_state(game_id=str(gid), user_id="u2")

    assert illegal["announcement"] == "ILLEGAL_MOVE"
    assert [entry.message for entry in white_state.scoresheet.turns[0].white] == [
        "Move attempt — Illegal move",
        "Move attempt — Move complete",
    ]
    assert [entry.message for entry in white_state.scoresheet.turns[0].black] == ["No pawn captures"]
    assert [entry.message for entry in black_state.scoresheet.turns[0].white] == [
        "Opponent move — Move complete",
    ]
    assert [entry.message for entry in black_state.scoresheet.turns[0].black] == ["No pawn captures"]


@pytest.mark.asyncio
async def test_wild16_failed_pawn_capture_attempt_disappears_from_allowed_moves() -> None:
    gid = ObjectId()
    now = datetime.now(UTC)
    engine = create_new_game(rule_variant="wild16")
    white_open = attempt_move(engine, "e2e4")
    black_reply = attempt_move(engine, "d7d5")
    games = FakeGamesCollection()
    games.docs.append(
        {
            "_id": gid,
            "game_code": "A7K2M9",
            "rule_variant": "wild16",
            "creator_color": "white",
            "white": {"user_id": "u1", "username": "w", "connected": True},
            "black": {"user_id": "u2", "username": "b", "connected": True},
            "state": "active",
            "turn": "white",
            "move_number": 3,
            "moves": [
                {
                    "ply": 1,
                    "color": "white",
                    "question_type": "COMMON",
                    "uci": "e2e4",
                    "announcement": white_open["announcement"],
                    "special_announcement": white_open["special_announcement"],
                    "capture_square": white_open["capture_square"],
                    "captured_piece_announcement": white_open.get("captured_piece_announcement"),
                    "next_turn_pawn_tries": white_open.get("next_turn_pawn_tries"),
                    "next_turn_has_pawn_capture": white_open.get("next_turn_has_pawn_capture"),
                    "move_done": white_open["move_done"],
                    "timestamp": now,
                },
                {
                    "ply": 2,
                    "color": "black",
                    "question_type": "COMMON",
                    "uci": "d7d5",
                    "announcement": black_reply["announcement"],
                    "special_announcement": black_reply["special_announcement"],
                    "capture_square": black_reply["capture_square"],
                    "captured_piece_announcement": black_reply.get("captured_piece_announcement"),
                    "next_turn_pawn_tries": black_reply.get("next_turn_pawn_tries"),
                    "next_turn_has_pawn_capture": black_reply.get("next_turn_has_pawn_capture"),
                    "move_done": black_reply["move_done"],
                    "timestamp": now,
                },
            ],
            "engine_state": serialize_game_state(engine),
            "created_at": now,
            "updated_at": now,
        }
    )
    service = GameService(games)

    before = await service.get_game_state(game_id=str(gid), user_id="u1")
    response = await service.execute_move(game_id=str(gid), user_id="u1", uci="e4f5")
    after = await service.get_game_state(game_id=str(gid), user_id="u1")

    assert black_reply["next_turn_pawn_tries"] == 1
    assert "e4f5" in before.allowed_moves
    assert response["announcement"] == "ILLEGAL_MOVE"
    assert response["move_done"] is False
    assert after.turn == "white"
    assert "e4f5" not in after.allowed_moves
    assert "e4d5" in after.allowed_moves
    assert len(games.docs[0]["moves"]) == 2


@pytest.mark.asyncio
async def test_get_game_state_rejects_non_participants(active_game_doc: dict) -> None:
    games = FakeGamesCollection()
    games.docs.append(active_game_doc)
    service = GameService(games)

    with pytest.raises(GameForbiddenError) as exc:
        await service.get_game_state(game_id=str(active_game_doc["_id"]), user_id="u3")
    assert exc.value.code == "FORBIDDEN"


@pytest.mark.asyncio
async def test_get_game_state_completed_reveals_full_board(active_game_doc: dict) -> None:
    games = FakeGamesCollection()
    completed = dict(active_game_doc)
    completed["state"] = "completed"
    completed["result"] = {"winner": "white", "reason": "checkmate"}
    games.docs.append(completed)
    service = GameService(games)

    state = await service.get_game_state(game_id=str(completed["_id"]), user_id="u2")

    assert state.state == "completed"
    assert state.result == {"winner": "white", "reason": "checkmate"}
    assert state.your_fen == "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1"
    assert state.possible_actions == []
    assert state.allowed_moves == []


def test_build_referee_log_filters_private_announcements_but_keeps_all_public_announcements() -> None:
    now = datetime.now(UTC)
    log = build_referee_log(
        [
            {"ply": 1, "announcement": "REGULAR_MOVE", "timestamp": now},
            {"ply": 2, "announcement": "ILLEGAL_MOVE", "timestamp": now},
            {
                "ply": 3,
                "announcement": "CAPTURE_DONE",
                "special_announcement": "CHECK_FILE",
                "capture_square": "e4",
                "timestamp": now,
            },
            {
                "ply": 4,
                "announcement": "REGULAR_MOVE",
                "special_announcement": "DRAW_TOOMANYREVERSIBLEMOVES",
                "timestamp": now,
            },
        ]
    )
    assert len(log) == 6
    assert log[0]["announcement"] == "REGULAR_MOVE"
    assert log[1]["announcement"] == "ILLEGAL_MOVE"
    assert log[2]["announcement"] == "CAPTURE_DONE"
    assert log[2]["capture_square"] == "e4"
    assert log[3]["announcement"] == "CHECK_FILE"
    assert log[3]["capture_square"] is None
    assert log[4]["announcement"] == "REGULAR_MOVE"
    assert log[5]["announcement"] == "DRAW_TOOMANYREVERSIBLEMOVES"


def test_build_referee_turns_records_illegal_move_announcements() -> None:
    now = datetime.now(UTC)
    turns = build_referee_turns(
        [
            {"ply": 1, "color": "white", "question_type": "COMMON", "uci": "e2e4", "announcement": "ILLEGAL_MOVE", "special_announcement": None, "timestamp": now},
        ]
    )

    assert turns == [{"turn": 1, "white": [{"kind": "illegal_move", "actor": "self", "prompt": "Move attempt", "message": "Move attempt — Illegal move", "messages": ["Illegal move"], "move_uci": "e2e4", "question_type": "COMMON"}], "black": []}]


def test_build_referee_turns_groups_live_moves_by_turn_and_color() -> None:
    now = datetime.now(UTC)
    turns = build_referee_turns(
        [
            {"ply": 1, "color": "white", "question_type": "COMMON", "uci": "c2c3", "announcement": "REGULAR_MOVE", "special_announcement": "NONE", "timestamp": now},
            {"ply": 2, "color": "black", "question_type": "COMMON", "uci": "e7e5", "announcement": "REGULAR_MOVE", "special_announcement": "NONE", "timestamp": now},
            {"ply": 3, "color": "white", "question_type": "ASK_ANY", "announcement": "HAS_ANY", "special_announcement": "NONE", "timestamp": now},
            {"ply": 4, "color": "black", "question_type": "COMMON", "uci": "e5d4", "announcement": "CAPTURE_DONE", "capture_square": "d4", "special_announcement": "CHECK_FILE", "timestamp": now},
        ]
    )

    assert turns == [
        {"turn": 1, "white": [{"kind": "move", "actor": "self", "prompt": "Move attempt", "message": "Move attempt — Move complete", "messages": ["Move complete"], "move_uci": "c2c3", "question_type": "COMMON"}], "black": [{"kind": "move", "actor": "self", "prompt": "Move attempt", "message": "Move attempt — Move complete", "messages": ["Move complete"], "move_uci": "e7e5", "question_type": "COMMON"}]},
        {"turn": 2, "white": [{"kind": "ask_any", "actor": "self", "prompt": "Ask any pawn captures", "message": "Ask any pawn captures — Has pawn captures", "messages": ["Has pawn captures"], "move_uci": None, "question_type": "ASK_ANY"}], "black": [{"kind": "capture", "actor": "self", "prompt": "Move attempt", "message": "Move attempt — Capture done at D4 · Check on file", "messages": ["Capture done at D4", "Check on file"], "move_uci": "e5d4", "question_type": "COMMON"}]},
    ]


@pytest.fixture
def app_with_state_service() -> tuple:
    app = create_app(Settings(ENVIRONMENT="testing"))

    service = SimpleNamespace(
        get_game_state=AsyncMock(
            return_value={
                "game_id": "gid1",
                "state": "active",
                "turn": "white",
                "move_number": 2,
                "your_color": "white",
                "your_fen": "8/8/8/8/4P3/8/PPPP1PPP/RNBQKBNR w - - 0 1",
                "allowed_moves": ["a2a3", "a2a4"],
                "scoresheet": {"viewer_color": "white", "last_move_number": 1, "turns": [{"turn": 1, "white": ["Move attempt — Move complete", "Ask any pawn captures — Has pawn captures"], "black": []}]},
                "referee_log": [{"ply": 1, "announcement": "REGULAR_MOVE", "timestamp": None}, {"ply": 1, "announcement": "HAS_ANY", "timestamp": None}],
                "referee_turns": [{"turn": 1, "white": ["Move attempt — Move complete", "Ask any pawn captures — Has pawn captures"], "black": []}],
                "possible_actions": ["move", "ask_any"],
                "result": None,
                "clock": {"white_remaining": 1500.0, "black_remaining": 1500.0, "active_color": "white"},
            }
        )
    )

    user = UserModel.from_mongo(
        {
            "_id": "507f1f77bcf86cd799439011",
            "username": "playerone",
            "username_display": "PlayerOne",
            "email": "player@example.com",
            "password_hash": "hash",
            "auth_providers": ["local"],
            "profile": {"bio": "", "avatar_url": None, "country": None},
            "stats": {
                "games_played": 0,
                "games_won": 0,
                "games_lost": 0,
                "games_drawn": 0,
                "elo": 1200,
                "elo_peak": 1200,
            },
            "settings": {
                "board_theme": "default",
                "piece_set": "cburnett",
                "sound_enabled": True,
                "auto_ask_any": False,
            },
            "role": "user",
            "status": "active",
            "last_active_at": datetime.now(UTC),
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        }
    )

    app.dependency_overrides[get_current_user] = lambda: user
    app.dependency_overrides[get_game_service] = lambda: service
    return app, service


def test_get_game_state_route_happy_path(app_with_state_service) -> None:
    app, service = app_with_state_service

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/api/game/gid1/state")

    assert response.status_code == 200
    assert response.json()["possible_actions"] == ["move", "ask_any"]
    assert response.json()["allowed_moves"] == ["a2a3", "a2a4"]
    service.get_game_state.assert_awaited_once()


def test_get_game_state_route_maps_forbidden_error(app_with_state_service) -> None:
    app, service = app_with_state_service
    service.get_game_state = AsyncMock(side_effect=GameForbiddenError(code="FORBIDDEN", message="Only participants"))

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/api/game/gid1/state")

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "FORBIDDEN"


def test_router_surface_smoke_covers_other_game_routes(app_with_state_service) -> None:
    app, service = app_with_state_service
    service.create_game = AsyncMock(
        return_value={
            "game_id": "gid1",
            "game_code": "A7K2M9",
            "play_as": "white",
            "rule_variant": "berkeley_any",
            "state": "waiting",
            "join_url": "https://kriegspiel.org/join/A7K2M9",
        }
    )
    service.join_game = AsyncMock(
        return_value={
            "game_id": "gid1",
            "game_code": "A7K2M9",
            "play_as": "black",
            "rule_variant": "berkeley_any",
            "state": "active",
            "game_url": "https://kriegspiel.org/game/gid1",
        }
    )
    service.execute_move = AsyncMock(
        return_value={
            "move_done": True,
            "announcement": "REGULAR_MOVE",
            "special_announcement": None,
            "capture_square": None,
            "turn": "black",
            "game_over": False,
            "clock": {"white_remaining": 1500.0, "black_remaining": 1500.0, "active_color": "black"},
        }
    )
    service.execute_ask_any = AsyncMock(
        return_value={
            "move_done": False,
            "announcement": "HAS_ANY",
            "special_announcement": None,
            "capture_square": None,
            "turn": "white",
            "game_over": False,
            "has_any": True,
            "clock": {"white_remaining": 1500.0, "black_remaining": 1500.0, "active_color": "white"},
        }
    )
    service.get_open_games = AsyncMock(return_value={"games": []})
    service.get_my_games = AsyncMock(return_value=[])
    service.get_game = AsyncMock(
        return_value={
            "game_id": "gid1",
            "game_code": "A7K2M9",
            "rule_variant": "berkeley_any",
            "state": "active",
            "white": {"username": "w", "connected": True},
            "black": {"username": "b", "connected": True},
            "turn": "white",
            "move_number": 1,
            "created_at": datetime.now(UTC),
        }
    )
    service.resign_game = AsyncMock(return_value={"result": {"winner": "black", "reason": "resignation"}})
    service.delete_waiting_game = AsyncMock(return_value=None)

    with TestClient(app, raise_server_exceptions=False) as client:
        assert (
            client.post(
                "/api/game/create", json={"rule_variant": "berkeley_any", "play_as": "white", "time_control": "rapid"}
            ).status_code
            == 201
        )
        assert client.post("/api/game/join/A7K2M9").status_code == 200
        assert client.post("/api/game/gid1/move", json={"uci": "e2e4"}).status_code == 200
        assert client.post("/api/game/gid1/ask-any").status_code == 200
        assert client.get("/api/game/open").status_code == 200
        assert client.get("/api/game/mine").status_code == 200
        assert client.get("/api/game/gid1").status_code == 200
        assert client.post("/api/game/gid1/resign").status_code == 200
        assert client.delete("/api/game/gid1").status_code == 204


def test_router_error_mapping_for_not_found_and_conflict(app_with_state_service) -> None:
    from app.services.game_service import GameConflictError, GameNotFoundError

    app, service = app_with_state_service
    service.get_game = AsyncMock(side_effect=GameNotFoundError())
    service.join_game = AsyncMock(side_effect=GameConflictError(code="GAME_FULL", message="Game is not joinable"))

    with TestClient(app, raise_server_exceptions=False) as client:
        get_resp = client.get("/api/game/gid404")
        join_resp = client.post("/api/game/join/AAAAAA")

    assert get_resp.status_code == 404
    assert get_resp.json()["error"]["code"] == "GAME_NOT_FOUND"
    assert join_resp.status_code == 409
    assert join_resp.json()["error"]["code"] == "GAME_FULL"


def test_router_error_mapping_for_validation_400(app_with_state_service) -> None:
    from app.services.game_service import GameValidationError

    app, service = app_with_state_service
    service.execute_move = AsyncMock(side_effect=GameValidationError(code="NOT_YOUR_TURN", message="It is not your turn"))

    with TestClient(app, raise_server_exceptions=False) as client:
        move_resp = client.post("/api/game/gid1/move", json={"uci": "e2e4"})

    assert move_resp.status_code == 400
    assert move_resp.json()["error"]["code"] == "NOT_YOUR_TURN"
