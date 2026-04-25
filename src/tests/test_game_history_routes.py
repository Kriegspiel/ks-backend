from __future__ import annotations

from datetime import UTC, datetime, timedelta
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
from app.services.engine_adapter import attempt_move, create_new_game, serialize_game_state
from app.services.game_service import GameForbiddenError, GameService


class FakeCursor:
    def __init__(self, docs: list[dict]):
        self._docs = docs

    def sort(self, field: str, direction: int):
        self._docs.sort(key=lambda x: x.get(field, datetime.fromtimestamp(0, UTC)), reverse=direction < 0)
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


class FakeCollection:
    def __init__(self, docs: list[dict] | None = None):
        self.docs = docs or []

    async def find_one(self, query: dict, projection: dict | None = None):
        for doc in self.docs:
            if self._matches(doc, query):
                return doc
        return None

    def find(self, query: dict):
        return FakeCursor([d for d in self.docs if self._matches(d, query)])

    def _matches(self, doc: dict, query: dict) -> bool:
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


@pytest.fixture
def game_docs() -> tuple[dict, dict, dict]:
    now = datetime.now(UTC)
    active_id = ObjectId()
    archived_id = ObjectId()
    other_archived_id = ObjectId()

    active = {
        "_id": active_id,
        "game_code": "A7K2M9",
        "rule_variant": "berkeley_any",
        "state": "active",
        "white": {"user_id": "u1", "username": "w", "connected": True},
        "black": {"user_id": "u2", "username": "b", "connected": True},
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
            }
        ],
        "created_at": now,
        "updated_at": now,
    }

    archived = {
        "_id": archived_id,
        "game_code": "B7K2M9",
        "rule_variant": "berkeley_any",
        "state": "completed",
        "white": {"user_id": "u1", "username": "w", "connected": True},
        "black": {"user_id": "u2", "username": "b", "connected": True},
        "moves": active["moves"],
        "result": {"winner": "white", "reason": "resignation"},
        "created_at": now,
        "updated_at": now,
    }

    older = {
        "_id": other_archived_id,
        "game_code": "C7K2M9",
        "rule_variant": "berkeley",
        "state": "completed",
        "white": {"user_id": "u9", "username": "x", "connected": True},
        "black": {"user_id": "u8", "username": "y", "connected": True},
        "moves": [],
        "result": {"winner": None, "reason": "stalemate"},
        "created_at": datetime(2025, 1, 1, tzinfo=UTC),
        "updated_at": datetime(2025, 1, 1, tzinfo=UTC),
    }

    return active, archived, older


@pytest.mark.asyncio
async def test_get_game_transcript_access_matrix_and_archive_fallback(game_docs) -> None:
    active, archived, _older = game_docs
    games = FakeCollection([active])
    archives = FakeCollection([archived])
    service = GameService(games, archives)

    participant = await service.get_game_transcript(game_id=str(active["_id"]), user_id="u1")
    assert participant.viewer_color == "white"
    assert participant.moves[0].answer.main == "REGULAR_MOVE"
    assert participant.moves[0].replay_fen is not None
    assert participant.moves[0].replay_fen.full.startswith("rnbqkbnr")

    with pytest.raises(GameForbiddenError) as forbidden:
        await service.get_game_transcript(game_id=str(active["_id"]), user_id="u3")
    assert forbidden.value.code == "FORBIDDEN"

    completed_public = await service.get_game_transcript(game_id=str(archived["_id"]), user_id="u3")
    assert completed_public.game_id == str(archived["_id"])
    assert completed_public.viewer_color is None


@pytest.mark.asyncio
async def test_get_game_transcript_filters_nonsense_attempts_and_renumbers_ply() -> None:
    archived_id = ObjectId()
    archived = {
        "_id": archived_id,
        "game_code": "CLEAN42",
        "rule_variant": "berkeley_any",
        "state": "completed",
        "white": {"user_id": "u1", "username": "white", "connected": True},
        "black": {"user_id": "u2", "username": "black", "connected": True},
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
                "timestamp": datetime(2025, 1, 1, tzinfo=UTC),
            },
            {
                "ply": 2,
                "color": "white",
                "question_type": "COMMON",
                "uci": "f2a8q",
                "announcement": "IMPOSSIBLE_TO_ASK",
                "special_announcement": None,
                "capture_square": None,
                "move_done": False,
                "timestamp": datetime(2025, 1, 1, tzinfo=UTC),
            },
            {
                "ply": 3,
                "color": "black",
                "question_type": "COMMON",
                "uci": "a7a5",
                "announcement": "REGULAR_MOVE",
                "special_announcement": None,
                "capture_square": None,
                "move_done": True,
                "timestamp": datetime(2025, 1, 1, tzinfo=UTC),
            },
        ],
        "result": {"winner": None, "reason": "stalemate"},
        "created_at": datetime(2025, 1, 1, tzinfo=UTC),
        "updated_at": datetime(2025, 1, 1, tzinfo=UTC),
    }
    service = GameService(FakeCollection([]), FakeCollection([archived]))

    transcript = await service.get_game_transcript(game_id=str(archived_id), user_id="u9")

    assert [move.answer.main for move in transcript.moves] == ["REGULAR_MOVE", "REGULAR_MOVE"]
    assert [move.ply for move in transcript.moves] == [1, 2]
    assert transcript.moves[-1].replay_fen is not None


@pytest.mark.asyncio
async def test_completed_wild16_transcript_uses_referee_scoresheet_with_private_illegal_attempts() -> None:
    now = datetime(2026, 4, 25, tzinfo=UTC)
    game_id = ObjectId()
    engine = create_new_game(rule_variant="wild16")
    attempted_moves = [
        ("white", "e2e4"),
        ("black", "d7d5"),
        ("white", "e4f5"),
        ("white", "e4d5"),
    ]
    public_moves: list[dict] = []

    for index, (color, uci) in enumerate(attempted_moves, start=1):
        outcome = attempt_move(engine, uci)
        move_record = {
            "ply": len(public_moves) + 1,
            "color": color,
            "question_type": "COMMON",
            "uci": uci,
            "announcement": outcome["announcement"],
            "special_announcement": outcome["special_announcement"],
            "capture_square": outcome["capture_square"],
            "captured_piece_announcement": outcome.get("captured_piece_announcement"),
            "next_turn_pawn_tries": outcome.get("next_turn_pawn_tries"),
            "next_turn_has_pawn_capture": outcome.get("next_turn_has_pawn_capture"),
            "move_done": outcome["move_done"],
            "timestamp": now + timedelta(seconds=index),
        }
        if not (outcome["announcement"] == "ILLEGAL_MOVE" and not outcome["move_done"]):
            public_moves.append(move_record)

    active = {
        "_id": game_id,
        "game_code": "WILD16",
        "rule_variant": "wild16",
        "state": "active",
        "white": {"user_id": "u1", "username": "white", "connected": True},
        "black": {"user_id": "u2", "username": "black", "connected": True},
        "moves": public_moves,
        "engine_state": serialize_game_state(engine),
        "created_at": now,
        "updated_at": now,
    }
    archived = {
        **active,
        "state": "completed",
        "result": {"winner": "white", "reason": "checkmate"},
    }

    active_transcript = await GameService(FakeCollection([active]), FakeCollection([])).get_game_transcript(
        game_id=str(game_id),
        user_id="u1",
    )
    assert [move.uci for move in active_transcript.moves] == ["e2e4", "d7d5", "e4d5"]

    completed_transcript = await GameService(FakeCollection([]), FakeCollection([archived])).get_game_transcript(
        game_id=str(game_id),
        user_id="spectator",
    )

    assert [move.uci for move in completed_transcript.moves] == ["e2e4", "d7d5", "e4f5", "e4d5"]
    assert [move.answer.main for move in completed_transcript.moves] == [
        "REGULAR_MOVE",
        "REGULAR_MOVE",
        "ILLEGAL_MOVE",
        "CAPTURE_DONE",
    ]
    assert completed_transcript.moves[2].move_done is False
    assert completed_transcript.moves[2].replay_fen == completed_transcript.moves[1].replay_fen
    assert completed_transcript.moves[3].timestamp == public_moves[-1]["timestamp"]


@pytest.mark.asyncio
async def test_completed_wild16_transcript_recomputes_legacy_promotion_pawn_try_counts() -> None:
    now = datetime(2026, 4, 25, tzinfo=UTC)
    game_id = ObjectId()
    engine = create_new_game(rule_variant="wild16")
    attempted_moves = [
        ("white", "g2g4"),
        ("black", "e7e5"),
        ("white", "f1h3"),
        ("black", "e5e4"),
        ("white", "a2a3"),
        ("black", "e4e3"),
        ("white", "h3g2"),
        ("black", "e3d2"),
        ("white", "g2h3"),
        ("white", "g2d5"),
        ("white", "c2c3"),
        ("white", "e1f1"),
    ]
    public_moves: list[dict] = []

    for index, (color, uci) in enumerate(attempted_moves, start=1):
        outcome = attempt_move(engine, uci)
        move_record = {
            "ply": len(public_moves) + 1,
            "color": color,
            "question_type": "COMMON",
            "uci": uci,
            "announcement": outcome["announcement"],
            "special_announcement": outcome["special_announcement"],
            "capture_square": outcome["capture_square"],
            "captured_piece_announcement": outcome.get("captured_piece_announcement"),
            "next_turn_pawn_tries": 4 if uci == "e1f1" else outcome.get("next_turn_pawn_tries"),
            "next_turn_has_pawn_capture": outcome.get("next_turn_has_pawn_capture"),
            "move_done": outcome["move_done"],
            "timestamp": now + timedelta(seconds=index),
        }
        if not (outcome["announcement"] == "ILLEGAL_MOVE" and not outcome["move_done"]):
            public_moves.append(move_record)

    engine_state = serialize_game_state(engine)
    for turn in engine_state["game_state"]["white_scoresheet"]["moves_own"]:
        for move_data, answer_data in turn:
            if move_data.get("chess_move") == "e1f1":
                answer_data["next_turn_pawn_tries"] = 4

    archived = {
        "_id": game_id,
        "game_code": "KZQYR8",
        "rule_variant": "wild16",
        "state": "completed",
        "white": {"user_id": "u1", "username": "white", "connected": True},
        "black": {"user_id": "u2", "username": "black", "connected": True},
        "moves": public_moves,
        "engine_state": engine_state,
        "result": {"winner": None, "reason": "draw"},
        "created_at": now,
        "updated_at": now,
    }

    transcript = await GameService(FakeCollection([]), FakeCollection([archived])).get_game_transcript(
        game_id=str(game_id),
        user_id="spectator",
    )

    e1f1 = next(move for move in transcript.moves if move.uci == "e1f1")
    assert e1f1.answer.next_turn_pawn_tries == 1


@pytest.mark.asyncio
async def test_get_recent_completed_games_uses_archive_order_and_limit_clamp(game_docs) -> None:
    _active, archived, older = game_docs
    service = GameService(FakeCollection([]), FakeCollection([older, archived]))

    recent = await service.get_recent_completed_games(limit=1)
    assert len(recent.games) == 1
    assert recent.games[0].game_id == str(archived["_id"])


@pytest.fixture
def app_with_history_service() -> tuple:
    app = create_app(Settings(ENVIRONMENT="testing"))

    service = SimpleNamespace(
        get_game_transcript=AsyncMock(
            return_value={
                "game_id": "gid1",
                "rule_variant": "berkeley_any",
                "viewer_color": "white",
                "moves": [
                    {
                        "ply": 1,
                        "color": "white",
                        "question_type": "COMMON",
                        "uci": "e2e4",
                        "answer": {"main": "REGULAR_MOVE", "capture_square": None, "special": None},
                        "move_done": True,
                        "timestamp": None,
                        "replay_fen": {
                            "full": "8/8/8/8/8/8/8/8 w - - 0 1",
                            "white": "8/8/8/8/8/8/8/8 w - - 0 1",
                            "black": "8/8/8/8/8/8/8/8 w - - 0 1",
                        },
                    }
                ],
            }
        ),
        get_recent_completed_games=AsyncMock(
            return_value={
                "games": [
                    {
                        "game_id": "gid1",
                        "game_code": "A7K2M9",
                        "rule_variant": "berkeley_any",
                        "white": {"username": "w", "connected": True},
                        "black": {"username": "b", "connected": True},
                        "result": {"winner": "white", "reason": "checkmate"},
                        "completed_at": datetime.now(UTC),
                    }
                ]
            }
        ),
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


def test_history_routes_happy_path(app_with_history_service) -> None:
    app, _service = app_with_history_service

    with TestClient(app, raise_server_exceptions=False) as client:
        transcript = client.get("/api/game/gid1/moves")
        recent = client.get("/api/game/recent")

    assert transcript.status_code == 200
    assert transcript.json()["viewer_color"] == "white"
    assert transcript.json()["moves"][0]["answer"]["main"] == "REGULAR_MOVE"
    assert recent.status_code == 200
    assert len(recent.json()["games"]) == 1


def test_history_transcript_route_maps_forbidden_error(app_with_history_service) -> None:
    app, service = app_with_history_service
    service.get_game_transcript = AsyncMock(side_effect=GameForbiddenError(code="FORBIDDEN", message="Only participants"))

    with TestClient(app, raise_server_exceptions=False) as client:
        transcript = client.get("/api/game/gid1/moves")

    assert transcript.status_code == 403
    assert transcript.json()["error"]["code"] == "FORBIDDEN"
