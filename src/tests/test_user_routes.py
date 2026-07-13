from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock

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
                {
                    "opponent": [{"value": "rival", "group": "Bots", "count": 1}],
                    "rule_set": [{"value": "berkeley_any", "group": "", "count": 1}],
                    "color": [{"value": "white", "group": "", "count": 1}],
                    "result": [{"value": "win", "group": "", "count": 1}],
                    "reason": [{"value": "checkmate", "group": "", "count": 1}],
                },
            )
        )
        self.get_game_history_filter_options = AsyncMock(
            return_value={
                "opponent": [{"value": "rival", "group": "Bots", "count": 1}],
                "rule_set": [{"value": "berkeley_any", "group": "", "count": 1}],
                "color": [{"value": "white", "group": "", "count": 1}],
                "result": [{"value": "win", "group": "", "count": 1}],
                "reason": [{"value": "checkmate", "group": "", "count": 1}],
            }
        )
        self.get_rating_history = AsyncMock(return_value={"track": "overall", "points": []})
        self.get_leaderboard = AsyncMock(
            return_value=(
                [
                    {
                        "rank": 1,
                        "username": "alpha",
                        "display_name": "Alpha",
                        "role": "user",
                        "is_bot": False,
                        "profile_path": "/players/alpha",
                        "elo": 1500,
                        "games_played": 10,
                        "win_rate": 0.6,
                    }
                ],
                1,
            )
        )
        self.get_leaderboard_filter_options = AsyncMock(
            return_value={
                "username": [{"value": "alpha", "label": "alpha", "group": "Humans", "count": 1}],
                "type": [{"value": "human", "label": "Human", "group": "", "count": 1}],
            }
        )
        self.get_listed_bot_daily_report = AsyncMock(
            return_value={
                "timezone": "America/New_York",
                "bots": [
                    {
                        "username": "llm_gptnano",
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
        self.get_bot_matrix_report = AsyncMock(
            return_value={
                "period": "lifetime",
                "players": [{"username": "llm_gptnano", "name": "LLM GPT-4.5 Nano (bot)"}],
                "matrix_rows": [],
                "end_condition_rows": [],
                "total_rows": {"all": [], "humans": [], "bots": []},
                "unique_game_count": 27348,
                "row_record_count": 54696,
                "usage_available": False,
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
                        "non_timeout_games": 1,
                        "total_time_played_seconds": 900,
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
    assert history.json()["filter_options"]["opponent"][0]["value"] == "rival"

    assert leaderboard.status_code == 200
    assert leaderboard.json()["players"][0]["rank"] == 1
    assert leaderboard.json()["filter_options"]["type"][0]["value"] == "human"

    assert unauth.status_code == 401


def test_leaderboard_route_passes_sort_filters_and_facets() -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    service = StubService()
    app.dependency_overrides[get_user_service] = lambda: service

    class FakeDB:
        users = object()
        sessions = object()

    db = FakeDB()
    dependencies.get_db = lambda: db

    with TestClient(app, raise_server_exceptions=False) as client:
        leaderboard = client.get(
            "/api/leaderboard"
            "?page=2&per_page=50&sort=games&dir=asc"
            "&username=randobot,llm_haiku&type=bot"
            "&include_filter_options=false"
        )
        filter_options = client.get("/api/leaderboard/filter-options")

    assert leaderboard.status_code == 200
    assert leaderboard.json()["filter_options"] == {}
    service.get_leaderboard.assert_awaited_once_with(
        db,
        2,
        50,
        filters={"username": ["randobot", "llm_haiku"], "type": ["bot"]},
        sort_key="games",
        sort_direction="asc",
    )
    service.get_leaderboard_filter_options.assert_awaited_once_with(db)
    assert filter_options.status_code == 200
    assert filter_options.json()["filter_options"]["username"][0]["value"] == "alpha"


def test_user_games_route_passes_sort_filters_and_facets() -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    service = StubService()
    app.dependency_overrides[get_user_service] = lambda: service

    class FakeUsers:
        async def find_one(self, query):
            return {"_id": "507f1f77bcf86cd799439011", "username": "playerone"}

    class FakeDB:
        users = FakeUsers()
        sessions = object()

    dependencies.get_db = lambda: FakeDB()

    with TestClient(app, raise_server_exceptions=False) as client:
        history = client.get(
            "/api/user/playerone/games"
            "?page=2&per_page=500&sort=turns&dir=asc"
            "&opponent=randobot,bot%3Abot_gemini31_lite"
            "&result=win&rule_set=berkeley_any&color=white&reason=timeout"
            "&include_filter_options=false"
        )

    assert history.status_code == 200
    assert history.json()["filter_options"]["opponent"][0]["value"] == "rival"
    service.get_game_history.assert_awaited_once_with(
        ANY,
        "507f1f77bcf86cd799439011",
        2,
        500,
        filters={
            "rule_set": ["berkeley_any"],
            "color": ["white"],
            "opponent": ["randobot", "bot:bot_gemini31_lite"],
            "result": ["win"],
            "reason": ["timeout"],
        },
        sort_key="turns",
        sort_direction="asc",
        include_filter_options=False,
    )


def test_user_game_filter_options_route_returns_facets() -> None:
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
        response = client.get("/api/user/playerone/games/filter-options")

    assert response.status_code == 200
    assert response.json()["filter_options"]["opponent"][0]["value"] == "rival"
    service.get_game_history_filter_options.assert_awaited_once_with(db, "507f1f77bcf86cd799439011")


def test_user_game_filter_options_route_404s_for_missing_user() -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    service = StubService()
    app.dependency_overrides[get_user_service] = lambda: service

    class FakeUsers:
        async def find_one(self, query):  # noqa: ARG002
            return None

    class FakeDB:
        users = FakeUsers()
        sessions = object()

    dependencies.get_db = lambda: FakeDB()

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/api/user/missing/games/filter-options")

    assert response.status_code == 404
    service.get_game_history_filter_options.assert_not_awaited()


def test_tech_report_routes_require_operator_access() -> None:
    app = create_app(Settings(ENVIRONMENT="testing", TECH_REPORT_USERNAMES="playerone"))
    service = StubService()
    app.dependency_overrides[get_user_service] = lambda: service

    class FakeDB:
        sessions = object()

    dependencies.get_db = lambda: FakeDB()
    app.dependency_overrides[dependencies.get_current_user] = lambda: SimpleNamespace(username="playerone", role="user")

    with TestClient(app, raise_server_exceptions=False) as client:
        bots_report = client.get("/api/tech/bots-report?days=10")
        bot_matrix_report = client.get(
            "/api/tech/bot-matrix-report?period=lifetime&outcomes=checkmate,insufficient&outcomes=time"
        )
        guests_report = client.get("/api/tech/guests-report")
        users_report = client.get("/api/tech/users-report")

    assert bots_report.status_code == 200
    assert bots_report.json()["bots"][0]["username"] == "llm_gptnano"

    assert bot_matrix_report.status_code == 200
    assert bot_matrix_report.json()["unique_game_count"] == 27348
    service.get_bot_matrix_report.assert_awaited_once_with(
        ANY,
        period="lifetime",
        outcomes=["checkmate", "insufficient", "time"],
    )

    assert guests_report.status_code == 200
    assert guests_report.json()["guests"][0]["username"] == "guest_mikhail_tal"
    assert guests_report.json()["guests"][0]["non_timeout_games"] == 1
    assert guests_report.json()["guests"][0]["total_time_played_seconds"] == 900
    assert guests_report.json()["available_guest_accounts"] == 39999

    assert users_report.status_code == 200
    assert users_report.json()["sections"][0]["key"] == "dau"
    assert users_report.json()["last_games"][0]["game_code"] == "USER01"

    app.dependency_overrides[dependencies.get_current_user] = lambda: SimpleNamespace(username="outsider", role="user")

    with TestClient(app, raise_server_exceptions=False) as client:
        forbidden = client.get("/api/tech/users-report")

    assert forbidden.status_code == 403
    assert forbidden.json()["detail"] == "Tech reports are private"

    app.dependency_overrides.pop(dependencies.get_current_user)

    with TestClient(app, raise_server_exceptions=False) as client:
        unauthenticated = client.get("/api/tech/users-report")

    assert unauthenticated.status_code == 401


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
    service.get_game_history.assert_awaited_once_with(
        db,
        "507f1f77bcf86cd799439011",
        1,
        100,
        filters={"rule_set": [], "color": [], "opponent": [], "result": [], "reason": []},
        sort_key=None,
        sort_direction="desc",
        include_filter_options=True,
    )


def test_user_games_accepts_10000_per_page() -> None:
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
        history = client.get("/api/user/playerone/games?page=2&per_page=10000")

    assert history.status_code == 200
    service.get_game_history.assert_awaited_once_with(
        db,
        "507f1f77bcf86cd799439011",
        2,
        10000,
        filters={"rule_set": [], "color": [], "opponent": [], "result": [], "reason": []},
        sort_key=None,
        sort_direction="desc",
        include_filter_options=True,
    )


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
