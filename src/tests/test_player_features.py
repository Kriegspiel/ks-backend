from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

from bson import ObjectId
from fastapi.testclient import TestClient

import app.dependencies as dependencies
from app.config import Settings
from app.main import create_app
from app.models.user import UserModel
from app.routers.game import get_game_service


class FakeCursor:
    def __init__(self, docs):
        self.docs = list(docs)

    def sort(self, fields, direction=None):
        specs = [(fields, direction)] if isinstance(fields, str) else fields
        for key, direction in reversed(specs):
            self.docs.sort(key=lambda d: _resolve(d, key), reverse=direction < 0)
        return self

    def skip(self, n):
        self.docs = self.docs[n:]
        return self

    def limit(self, n):
        self.docs = self.docs[:n]
        return self

    def __aiter__(self):
        self.i = 0
        return self

    async def __anext__(self):
        if self.i >= len(self.docs):
            raise StopAsyncIteration
        d = self.docs[self.i]
        self.i += 1
        return d


class FakeCollection:
    def __init__(self, docs):
        self.docs = list(docs)

    async def find_one(self, query):
        for d in self.docs:
            if _matches(d, query):
                return d
        return None

    def find(self, query):
        return FakeCursor([d for d in self.docs if _matches(d, query)])

    async def count_documents(self, query):
        return len([d for d in self.docs if _matches(d, query)])

    async def find_one_and_update(self, query, update, return_document=None):
        for d in self.docs:
            if _matches(d, query):
                for k, v in update.get("$set", {}).items():
                    _set_nested(d, k, v)
                return d
        return None


def _resolve(doc, key):
    cur = doc
    for part in key.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _set_nested(doc, key, value):
    cur = doc
    parts = key.split(".")
    for p in parts[:-1]:
        cur = cur.setdefault(p, {})
    cur[parts[-1]] = value


def _same_id(left, right):
    left_id = str(left) if isinstance(left, ObjectId) else left
    right_id = str(right) if isinstance(right, ObjectId) else right
    return left_id == right_id


def _matches(doc, query):
    for k, expected in query.items():
        if k == "$or":
            if any(_matches(doc, q) for q in expected):
                continue
            return False
        val = _resolve(doc, k)
        if isinstance(expected, dict) and "$gte" in expected:
            if val is None or val < expected["$gte"]:
                return False
        elif isinstance(expected, dict) and "$ne" in expected:
            if val == expected["$ne"]:
                return False
        elif not _same_id(val, expected):
            return False
    return True


def _build_app_and_db():
    app = create_app(Settings(ENVIRONMENT="testing"))
    user_id = "507f1f77bcf86cd799439011"

    users = FakeCollection(
        [
            {
                "_id": ObjectId(user_id),
                "username": "playerone",
                "username_display": "Player One",
                "role": "user",
                "profile": {"bio": "hey", "avatar_url": None, "country": "US"},
                "stats": {
                    "games_played": 8,
                    "games_won": 5,
                    "games_lost": 2,
                    "games_drawn": 1,
                    "elo": 1400,
                    "elo_peak": 1410,
                },
                "settings": {
                    "board_theme": "default",
                    "piece_set": "cburnett",
                    "sound_enabled": True,
                    "auto_ask_any": False,
                },
                "status": "active",
                "created_at": datetime(2025, 1, 1, tzinfo=UTC),
            },
            {
                "_id": ObjectId("507f1f77bcf86cd799439012"),
                "username": "alpha",
                "username_display": "Alpha",
                "role": "user",
                "stats": {"games_played": 10, "games_won": 7, "elo": 1500},
                "status": "active",
                "settings": {},
            },
            {
                "_id": ObjectId("507f1f77bcf86cd799439015"),
                "username": "randobot",
                "username_display": "Random Bot",
                "role": "bot",
                "bot_profile": {"display_name": "Random Bot", "listed": True},
                "stats": {"games_played": 9, "games_won": 4, "elo": 1450},
                "status": "active",
                "settings": {},
            },
            {
                "_id": ObjectId("507f1f77bcf86cd799439017"),
                "username": "gptnano",
                "username_display": "GPT Nano",
                "role": "bot",
                "bot_profile": {"display_name": "GPT Nano", "listed": True},
                "stats": {"games_played": 1, "games_won": 0, "elo": 1184},
                "status": "active",
                "settings": {},
            },
            {
                "_id": ObjectId("507f1f77bcf86cd799439016"),
                "username": "hiddenbot",
                "username_display": "Hidden Bot",
                "role": "bot",
                "bot_profile": {"display_name": "Hidden Bot", "listed": False},
                "stats": {"games_played": 11, "games_won": 7, "elo": 1550},
                "status": "active",
                "settings": {},
            },
            {
                "_id": ObjectId("507f1f77bcf86cd799439013"),
                "username": "inactive",
                "username_display": "Inactive",
                "role": "user",
                "stats": {"games_played": 12, "games_won": 6, "elo": 1600},
                "status": "disabled",
                "settings": {},
            },
            {
                "_id": ObjectId("507f1f77bcf86cd799439014"),
                "username": "newbie",
                "username_display": "Newbie",
                "role": "user",
                "stats": {"games_played": 3, "games_won": 3, "elo": 1700},
                "status": "active",
                "settings": {},
            },
        ]
    )
    archives = FakeCollection(
        [
            {
                "_id": "664b2ca7f7f86cd799439011",
                "white": {"user_id": user_id, "username": "playerone"},
                "black": {"user_id": "x", "username": "rival"},
                "result": {"winner": "white", "reason": "checkmate"},
                "moves": [1, 2],
                "created_at": datetime(2026, 3, 1, tzinfo=UTC),
                "updated_at": datetime(2026, 3, 1, tzinfo=UTC),
            },
            {
                "_id": "664b2ca7f7f86cd799439012",
                "white": {"user_id": "x", "username": "rival"},
                "black": {"user_id": user_id, "username": "playerone"},
                "result": {"winner": "white", "reason": "time"},
                "moves": [1, 2, 3],
                "created_at": datetime(2026, 2, 15, tzinfo=UTC),
                "updated_at": datetime(2026, 2, 15, tzinfo=UTC),
            },
        ]
    )

    class FakeDB:
        pass

    db = FakeDB()
    db.users = users
    db.game_archives = archives
    db.games = FakeCollection([])
    db.sessions = FakeCollection([])
    return app, db, user_id


def _fake_authenticated_user(user_id: str = "507f1f77bcf86cd799439011") -> UserModel:
    now = datetime(2026, 3, 1, tzinfo=UTC)
    return UserModel.model_validate(
        {
            "_id": user_id,
            "username": "playerone",
            "username_display": "playerone",
            "email": "player@example.com",
            "password_hash": "hashed",
            "last_active_at": now,
            "created_at": now,
            "updated_at": now,
        }
    )


def test_existing_profile_returns_expected_payload() -> None:
    app, db, _ = _build_app_and_db()
    dependencies.get_db = lambda: db

    with TestClient(app, raise_server_exceptions=False) as client:
        profile = client.get("/api/user/playerone")

    assert profile.status_code == 200
    body = profile.json()
    assert body["username"] == "playerone"
    assert body["display_name"] == "Player One"
    assert body["role"] == "user"
    assert body["is_bot"] is False
    assert body["stats"]["games_played"] == 8


def test_missing_profile_returns_404() -> None:
    app, db, _ = _build_app_and_db()
    dependencies.get_db = lambda: db

    with TestClient(app, raise_server_exceptions=False) as client:
        missing = client.get("/api/user/missing")

    assert missing.status_code == 404


def test_history_paginates_with_deterministic_order() -> None:
    app, db, _ = _build_app_and_db()
    dependencies.get_db = lambda: db

    with TestClient(app, raise_server_exceptions=False) as client:
        page = client.get("/api/user/playerone/games?page=1&per_page=1")

    assert page.status_code == 200
    body = page.json()
    assert body["pagination"] == {"page": 1, "per_page": 1, "total": 2, "pages": 2}
    assert len(body["games"]) == 1
    assert body["games"][0]["game_id"] == "664b2ca7f7f86cd799439011"


def test_history_out_of_range_returns_empty_with_stable_total() -> None:
    app, db, _ = _build_app_and_db()
    dependencies.get_db = lambda: db

    with TestClient(app, raise_server_exceptions=False) as client:
        out = client.get("/api/user/playerone/games?page=9&per_page=20")

    assert out.status_code == 200
    assert out.json()["games"] == []
    assert out.json()["pagination"]["total"] == 2


def test_history_and_leaderboard_per_page_above_100_rejected() -> None:
    app, db, _ = _build_app_and_db()
    dependencies.get_db = lambda: db

    with TestClient(app, raise_server_exceptions=False) as client:
        history = client.get("/api/user/playerone/games?page=1&per_page=101")
        leaderboard = client.get("/api/leaderboard?page=1&per_page=101")

    assert history.status_code == 422
    assert leaderboard.status_code == 422


def test_leaderboard_orders_by_elo_and_filters_min_games() -> None:
    app, db, _ = _build_app_and_db()
    dependencies.get_db = lambda: db

    with TestClient(app, raise_server_exceptions=False) as client:
        leaderboard = client.get("/api/leaderboard?page=1&per_page=20")

    assert leaderboard.status_code == 200
    players = leaderboard.json()["players"]
    assert [p["username"] for p in players] == ["alpha", "randobot", "playerone", "gptnano"]
    assert all(p["games_played"] >= 5 for p in players if not p["is_bot"])
    assert players[1]["is_bot"] is True
    assert players[1]["display_name"] == "Random Bot"
    assert players[1]["profile_path"] == "/players/randobot"
    assert players[3]["is_bot"] is True


def test_settings_patch_requires_authentication() -> None:
    app, db, _ = _build_app_and_db()
    dependencies.get_db = lambda: db

    with TestClient(app, raise_server_exceptions=False) as client:
        unauth = client.patch("/api/user/settings", json={"board_theme": "dark"})

    assert unauth.status_code == 401


def test_settings_patch_persists_with_authenticated_user() -> None:
    app, db, user_id = _build_app_and_db()
    dependencies.get_db = lambda: db
    app.dependency_overrides[dependencies.get_current_user] = lambda: _fake_authenticated_user(user_id)

    with TestClient(app, raise_server_exceptions=False) as client:
        update = client.patch(
            "/api/user/settings",
            json={"board_theme": "dark", "sound_enabled": False, "ignored": "x"},
        )

    assert update.status_code == 200
    assert update.json()["board_theme"] == "dark"
    assert update.json()["sound_enabled"] is False

    stored = next(doc for doc in db.users.docs if str(doc["_id"]) == user_id)
    assert stored["settings"]["board_theme"] == "dark"
    assert stored["settings"]["sound_enabled"] is False
    assert "ignored" not in stored["settings"]


def test_completed_game_moves_transcript_returns_expected_shape() -> None:
    app, db, user_id = _build_app_and_db()
    dependencies.get_db = lambda: db
    app.dependency_overrides[dependencies.get_current_user] = lambda: _fake_authenticated_user(user_id)

    stub_service = type("StubService", (), {})()
    stub_service.get_game_transcript = AsyncMock(
        return_value={
            "game_id": "664b2ca7f7f86cd799439011",
            "rule_variant": "berkeley_any",
            "moves": [
                {
                    "ply": 1,
                    "color": "white",
                    "question_type": "move",
                    "uci": "e2e4",
                    "answer": {"main": "Legal", "capture_square": None, "special": None},
                    "move_done": True,
                    "timestamp": datetime(2026, 3, 1, tzinfo=UTC),
                    "replay_fen": {
                        "full": "fen-full",
                        "white": "fen-white",
                        "black": "fen-black",
                    },
                }
            ],
        }
    )
    app.dependency_overrides[get_game_service] = lambda: stub_service

    with TestClient(app, raise_server_exceptions=False) as client:
        transcript = client.get("/api/game/664b2ca7f7f86cd799439011/moves")

    assert transcript.status_code == 200
    body = transcript.json()
    assert body["game_id"] == "664b2ca7f7f86cd799439011"
    assert body["moves"][0]["replay_fen"] == {
        "full": "fen-full",
        "white": "fen-white",
        "black": "fen-black",
    }
