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
    assert participant.moves[0].answer.main == "REGULAR_MOVE"
    assert participant.moves[0].replay_fen is not None
    assert participant.moves[0].replay_fen.full.startswith("rnbqkbnr")

    with pytest.raises(GameForbiddenError) as forbidden:
        await service.get_game_transcript(game_id=str(active["_id"]), user_id="u3")
    assert forbidden.value.code == "FORBIDDEN"

    completed_public = await service.get_game_transcript(game_id=str(archived["_id"]), user_id="u3")
    assert completed_public.game_id == str(archived["_id"])


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
