from __future__ import annotations

from datetime import UTC, datetime

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
import app.dependencies as dependencies


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
        elif val != expected:
            return False
    return True


def _build_app_and_db():
    app = create_app(Settings(ENVIRONMENT="testing"))
    user_id = "507f1f77bcf86cd799439011"

    users = FakeCollection(
        [
            {
                "_id": user_id,
                "username": "playerone",
                "profile": {"bio": "hey", "avatar_url": None, "country": "US"},
                "stats": {"games_played": 8, "games_won": 5, "games_lost": 2, "games_drawn": 1, "elo": 1400, "elo_peak": 1410},
                "settings": {"board_theme": "default", "piece_set": "cburnett", "sound_enabled": True, "auto_ask_any": False},
                "status": "active",
                "created_at": datetime(2025, 1, 1, tzinfo=UTC),
            },
            {
                "_id": "507f1f77bcf86cd799439012",
                "username": "alpha",
                "stats": {"games_played": 10, "games_won": 7, "elo": 1500},
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
            }
        ]
    )

    class FakeDB:
        pass

    db = FakeDB()
    db.users = users
    db.game_archives = archives
    return app, db


def test_profile_and_leaderboard_contracts() -> None:
    app, db = _build_app_and_db()
    dependencies.get_db = lambda: db
    with TestClient(app, raise_server_exceptions=False) as client:
        profile = client.get("/api/user/playerone")
        leaderboard = client.get("/api/leaderboard?page=1&per_page=200")

    assert profile.status_code == 200
    assert profile.json()["stats"]["games_played"] == 8

    assert leaderboard.status_code == 422  # per_page clamp via validation


def test_profile_missing_and_history_out_of_range() -> None:
    app, db = _build_app_and_db()
    dependencies.get_db = lambda: db
    with TestClient(app, raise_server_exceptions=False) as client:
        missing = client.get("/api/user/missing")
        out = client.get("/api/user/playerone/games?page=9&per_page=20")

    assert missing.status_code == 404
    assert out.status_code == 200
    assert out.json()["games"] == []
    assert out.json()["pagination"]["total"] == 1
