from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

import app.dependencies as dependencies
from app.config import Settings
from app.main import create_app
from app.routers.user import get_user_service


class StubService:
    def __init__(self) -> None:
        self.get_public_profile = AsyncMock(
            return_value={
                "username": "playerone",
                "display_name": "Player One",
                "role": "user",
                "is_bot": False,
                "profile": {"bio": "Hello", "avatar_url": None, "country": "US"},
                "stats": {"games_played": 7, "elo": 1337},
                "member_since": datetime(2025, 1, 1, tzinfo=UTC),
            }
        )
        self.get_game_history = AsyncMock(
            return_value=(
                [
                    {
                        "game_id": "gid1",
                        "game_code": "A7K2M9",
                        "rule_variant": "berkeley_any",
                        "opponent": "rival",
                        "opponent_role": "bot",
                        "play_as": "white",
                        "result": "win",
                        "reason": "checkmate",
                        "move_count": 45,
                        "turn_count": 20,
                        "played_at": datetime(2026, 1, 1, tzinfo=UTC),
                    }
                ],
                1,
            )
        )
        self.get_leaderboard = AsyncMock(
            return_value=(
                [{"rank": 1, "username": "alpha", "display_name": "Alpha", "role": "user", "is_bot": False, "profile_path": "/players/alpha", "elo": 1500, "games_played": 10, "win_rate": 0.6}],
                1,
            )
        )
        self.update_settings = AsyncMock(
            return_value={
                "board_theme": "dark",
                "piece_set": "cburnett",
                "sound_enabled": False,
                "auto_ask_any": True,
            }
        )

    @staticmethod
    def canonical_username(username: str) -> str:
        return username.lower()


def test_user_routes_profile_games_leaderboard_and_settings_auth_gate() -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    app.dependency_overrides[get_user_service] = lambda: StubService()

    class FakeUsers:
        async def find_one(self, query):
            return {"_id": "507f1f77bcf86cd799439011", "username": "playerone"}

    class FakeDB:
        users = FakeUsers()
        sessions = object()

    dependencies.get_db = lambda: FakeDB()

    with TestClient(app, raise_server_exceptions=False) as client:
        profile = client.get("/api/user/playerone")
        history = client.get("/api/user/playerone/games?page=1&per_page=20")
        leaderboard = client.get("/api/leaderboard?page=1&per_page=20")
        unauth = client.patch("/api/user/settings", json={"board_theme": "dark"})

    assert profile.status_code == 200
    assert profile.json()["username"] == "playerone"

    assert history.status_code == 200
    assert history.json()["pagination"]["total"] == 1

    assert leaderboard.status_code == 200
    assert leaderboard.json()["players"][0]["rank"] == 1

    assert unauth.status_code == 401
