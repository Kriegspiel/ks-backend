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
        self.get_rating_history = AsyncMock(return_value={"track": "overall", "points": []})
        self.get_leaderboard = AsyncMock(
            return_value=(
                [{"rank": 1, "username": "alpha", "display_name": "Alpha", "role": "user", "is_bot": False, "profile_path": "/players/alpha", "elo": 1500, "games_played": 10, "win_rate": 0.6}],
                1,
            )
        )
        self.get_listed_bot_daily_report = AsyncMock(
            return_value={
                "timezone": "America/New_York",
                "bots": [
                    {
                        "username": "gptnano",
                        "rows": [
                            {
                                "date": "2026-04-08",
                                "stats": {
                                    "overall": {"total_games": 2, "wins": 1, "win_rate": 0.5},
                                    "vs_humans": {"total_games": 0, "wins": 0, "win_rate": 0.0},
                                    "vs_bots": {"total_games": 2, "wins": 1, "win_rate": 0.5},
                                },
                            },
                        ],
                    },
                ],
            }
        )
        self.get_guest_report = AsyncMock(
            return_value={
                "guests": [
                    {
                        "name": "guest_mikhail_tal",
                        "username": "guest_mikhail_tal",
                        "day_started": "2026-04-01",
                        "last_game": "2026-04-04T13:00:00+00:00",
                        "number_of_games": 2,
                    }
                ],
                "total": 1,
                "available_guest_accounts": 39999,
            }
        )
        self.get_user_activity_report = AsyncMock(
            return_value={
                "timezone": "America/New_York",
                "sections": [
                    {
                        "key": "dau",
                        "title": "DAU",
                        "rows": [
                            {
                                "label": "2026-05-01",
                                "active_users": 2,
                                "active_bots": 1,
                                "total_games": 3,
                            }
                        ],
                    }
                ],
                "last_games": [{"game_code": "USER01"}],
            }
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


def test_user_routes_profile_games_leaderboard_bots_report_and_settings_auth_gate() -> None:
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
        bots_report = client.get("/api/tech/bots-report?days=10")
        guests_report = client.get("/api/tech/guests-report")
        users_report = client.get("/api/tech/users-report")
        unauth = client.patch("/api/user/settings", json={"board_theme": "dark"})

    assert profile.status_code == 200
    assert profile.json()["username"] == "playerone"

    assert history.status_code == 200
    assert history.json()["pagination"]["total"] == 1

    assert leaderboard.status_code == 200
    assert leaderboard.json()["players"][0]["rank"] == 1

    assert bots_report.status_code == 200
    assert bots_report.json()["bots"][0]["username"] == "gptnano"

    assert guests_report.status_code == 200
    assert guests_report.json()["guests"][0]["username"] == "guest_mikhail_tal"
    assert guests_report.json()["available_guest_accounts"] == 39999

    assert users_report.status_code == 200
    assert users_report.json()["sections"][0]["key"] == "dau"
    assert users_report.json()["last_games"][0]["game_code"] == "USER01"

    assert unauth.status_code == 401


def test_user_games_defaults_to_100_per_page() -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    service = StubService()
    app.dependency_overrides[get_user_service] = lambda: service

    class FakeUsers:
        async def find_one(self, query):
            return {"_id": "507f1f77bcf86cd799439011", "username": "playerone"}

    class FakeDB:
        users = FakeUsers()
        sessions = object()

    db = FakeDB()
    dependencies.get_db = lambda: db

    with TestClient(app, raise_server_exceptions=False) as client:
        history = client.get("/api/user/playerone/games")

    assert history.status_code == 200
    service.get_game_history.assert_awaited_once_with(db, "507f1f77bcf86cd799439011", 1, 100)


def test_user_routes_return_404_for_missing_profile_and_history_targets(monkeypatch) -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    service = StubService()
    service.get_public_profile = AsyncMock(return_value=None)
    app.dependency_overrides[get_user_service] = lambda: service

    class MissingUsers:
        async def find_one(self, query):  # noqa: ANN001
            return None

    db = type("FakeDB", (), {"users": MissingUsers(), "sessions": object()})()
    monkeypatch.setattr(dependencies, "get_db", lambda: db)

    with TestClient(app, raise_server_exceptions=False) as client:
        profile = client.get("/api/user/missing")
        history = client.get("/api/user/missing/games")
        rating_history = client.get("/api/user/missing/rating-history")

    assert profile.status_code == 404
    assert history.status_code == 404
    assert rating_history.status_code == 404


def test_user_rating_history_route_returns_service_payload(monkeypatch) -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    service = StubService()
    service.get_rating_history = AsyncMock(
        return_value={
            "track": "overall",
            "series": {
                "game": [{"label": "Game 1", "elo": 1216}],
                "date": [{"label": "2026-04-15", "elo": 1216}],
            },
        }
    )
    app.dependency_overrides[get_user_service] = lambda: service

    class FakeUsers:
        async def find_one(self, query):  # noqa: ANN001
            return {"_id": "507f1f77bcf86cd799439011", "username": "playerone"}

    db = type("FakeDB", (), {"users": FakeUsers(), "sessions": object()})()
    monkeypatch.setattr(dependencies, "get_db", lambda: db)

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/api/user/playerone/rating-history?track=overall&limit=100")

    assert response.status_code == 200
    assert response.json()["series"]["game"][0]["elo"] == 1216
    service.get_rating_history.assert_awaited_once_with(db, "507f1f77bcf86cd799439011", track="overall", limit=100)
