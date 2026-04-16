from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.config import Settings
from app.dependencies import get_current_user
from app.main import create_app
from app.models.user import UserModel
from app.routers.game import get_game_service
from app.services.game_service import (
    GameConflictError,
    GameForbiddenError,
    GameNotFoundError,
    GameServiceError,
    GameValidationError,
)


def _user() -> UserModel:
    return UserModel.from_mongo(
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


@pytest.fixture
def app_with_game_service() -> tuple:
    app = create_app(Settings(ENVIRONMENT="testing"))
    service = SimpleNamespace(
        create_game=AsyncMock(
            return_value={
                "game_id": "gid1",
                "game_code": "A7K2M9",
                "play_as": "white",
                "rule_variant": "berkeley_any",
                "state": "waiting",
                "join_url": "https://kriegspiel.org/join/A7K2M9",
            }
        ),
        join_game=AsyncMock(
            return_value={
                "game_id": "gid1",
                "game_code": "A7K2M9",
                "play_as": "black",
                "rule_variant": "berkeley_any",
                "state": "active",
                "game_url": "https://kriegspiel.org/game/A7K2M9",
            }
        ),
        get_open_games=AsyncMock(
            return_value={
                "games": [
                    {
                        "game_code": "A7K2M9",
                        "rule_variant": "berkeley_any",
                        "created_by": "playerone",
                        "created_at": datetime.now(UTC),
                        "available_color": "black",
                    }
                ]
            }
        ),
        get_my_games=AsyncMock(
            return_value=[
                {
                    "game_id": "gid1",
                    "game_code": "A7K2M9",
                    "rule_variant": "berkeley_any",
                    "state": "active",
                    "white": {"username": "playerone", "connected": True},
                    "black": {"username": "opponent", "connected": True},
                    "turn": "white",
                    "move_number": 1,
                    "created_at": datetime.now(UTC),
                    "updated_at": datetime.now(UTC),
                }
            ]
        ),
        get_game=AsyncMock(
            return_value={
                "game_id": "gid1",
                "game_code": "A7K2M9",
                "rule_variant": "berkeley_any",
                "state": "active",
                "white": {"username": "playerone", "connected": True},
                "black": {"username": "opponent", "connected": True},
                "turn": "white",
                "move_number": 1,
                "created_at": datetime.now(UTC),
                "updated_at": datetime.now(UTC),
            }
        ),
        resign_game=AsyncMock(return_value={"result": {"winner": "black", "reason": "resignation"}}),
        delete_waiting_game=AsyncMock(return_value=None),
    )

    app.dependency_overrides[get_current_user] = lambda: _user()
    app.dependency_overrides[get_game_service] = lambda: service
    return app, service


@pytest.mark.parametrize(
    "method,path,payload",
    [
        ("post", "/api/game/create", {"rule_variant": "berkeley_any", "play_as": "white", "time_control": "rapid"}),
        ("post", "/api/game/join/A7K2M9", None),
        ("get", "/api/game/open", None),
        ("get", "/api/game/mine", None),
        ("get", "/api/game/gid1", None),
        ("post", "/api/game/gid1/resign", None),
        ("delete", "/api/game/gid1", None),
    ],
)
def test_game_endpoints_require_auth(method: str, path: str, payload: dict | None) -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))

    def raise_unauth():
        raise HTTPException(status_code=401, detail="Authentication required")

    app.dependency_overrides[get_current_user] = raise_unauth

    with TestClient(app) as client:
        response = getattr(client, method)(path, json=payload) if payload else getattr(client, method)(path)

    assert response.status_code == 401


def test_game_router_happy_path_shapes(app_with_game_service) -> None:
    app, _service = app_with_game_service

    with TestClient(app) as client:
        create = client.post(
            "/api/game/create",
            json={"rule_variant": "berkeley_any", "play_as": "white", "time_control": "rapid"},
        )
        join = client.post("/api/game/join/A7K2M9")
        open_games = client.get("/api/game/open")
        mine = client.get("/api/game/mine")

    assert create.status_code == 201
    assert create.json()["game_code"] == "A7K2M9"
    assert join.status_code == 200
    assert join.json()["state"] == "active"
    assert open_games.status_code == 200
    assert isinstance(open_games.json()["games"], list)
    assert mine.status_code == 200
    assert isinstance(mine.json()["games"], list)


def test_game_router_maps_domain_errors_to_standard_envelope(app_with_game_service) -> None:
    app, service = app_with_game_service
    service.join_game = AsyncMock(
        side_effect=GameConflictError(code="CANNOT_JOIN_OWN_GAME", message="Cannot join your own game")
    )

    with TestClient(app) as client:
        response = client.post("/api/game/join/A7K2M9")

    assert response.status_code == 409
    assert response.json() == {
        "error": {
            "code": "CANNOT_JOIN_OWN_GAME",
            "message": "Cannot join your own game",
            "details": {},
        }
    }


def test_game_router_get_game_and_resign_and_delete_success(app_with_game_service) -> None:
    app, _service = app_with_game_service

    with TestClient(app) as client:
        game = client.get("/api/game/gid1")
        resign = client.post("/api/game/gid1/resign")
        delete = client.delete("/api/game/gid1")

    assert game.status_code == 200
    assert game.json()["game_id"] == "gid1"
    assert resign.status_code == 200
    assert resign.json()["result"]["reason"] == "resignation"
    assert delete.status_code == 204


@pytest.mark.parametrize(
    "endpoint,method,error,status_code,code",
    [
        ("/api/game/join/A7K2M9", "post", GameNotFoundError("No game with code A7K2M9 exists."), 404, "GAME_NOT_FOUND"),
        (
            "/api/game/gid1/resign",
            "post",
            GameValidationError(code="GAME_NOT_ACTIVE", message="Game is not active"),
            400,
            "GAME_NOT_ACTIVE",
        ),
        (
            "/api/game/gid1",
            "delete",
            GameForbiddenError(code="FORBIDDEN", message="Only the creator can delete this waiting game"),
            403,
            "FORBIDDEN",
        ),
        (
            "/api/game/create",
            "post",
            GameServiceError(code="UNKNOWN_GAME_ERROR", message="Unexpected game failure"),
            400,
            "UNKNOWN_GAME_ERROR",
        ),
    ],
)
def test_game_router_error_mapping_matrix(
    app_with_game_service,
    endpoint: str,
    method: str,
    error: Exception,
    status_code: int,
    code: str,
) -> None:
    app, service = app_with_game_service

    if endpoint.endswith("/create"):
        service.create_game = AsyncMock(side_effect=error)
        payload = {"rule_variant": "berkeley_any", "play_as": "white", "time_control": "rapid"}
    elif endpoint.endswith("/resign"):
        service.resign_game = AsyncMock(side_effect=error)
        payload = None
    elif method == "delete":
        service.delete_waiting_game = AsyncMock(side_effect=error)
        payload = None
    else:
        service.join_game = AsyncMock(side_effect=error)
        payload = None

    with TestClient(app) as client:
        response = getattr(client, method)(endpoint, json=payload) if payload else getattr(client, method)(endpoint)

    assert response.status_code == status_code
    assert response.json()["error"]["code"] == code
    assert "message" in response.json()["error"]
    assert response.json()["error"]["details"] == {}


def test_game_router_open_and_mine_error_paths(app_with_game_service) -> None:
    app, service = app_with_game_service
    service.get_open_games = AsyncMock(side_effect=GameConflictError(code="GAME_FULL", message="Game is not joinable"))
    service.get_my_games = AsyncMock(side_effect=GameNotFoundError("No game found"))

    with TestClient(app) as client:
        open_games = client.get("/api/game/open")
        mine = client.get("/api/game/mine")

    assert open_games.status_code == 409
    assert open_games.json()["error"]["code"] == "GAME_FULL"
    assert mine.status_code == 404
    assert mine.json()["error"]["code"] == "GAME_NOT_FOUND"


def test_get_game_service_prefers_app_state_and_falls_back_to_database(monkeypatch: pytest.MonkeyPatch) -> None:
    existing_service = object()
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                game_service=existing_service,
                settings=SimpleNamespace(SITE_ORIGIN="https://kriegspiel.org"),
            )
        )
    )

    assert get_game_service(request) is existing_service

    fake_db = SimpleNamespace(games=object(), users=object(), game_archives=object())
    captured: dict[str, object] = {}

    class FakeGameService:
        def __init__(self, games, *, users_collection, archives_collection, site_origin) -> None:  # noqa: ANN001
            captured["games"] = games
            captured["users"] = users_collection
            captured["archives"] = archives_collection
            captured["site_origin"] = site_origin

    import app.routers.game as game_router_module

    monkeypatch.setattr(game_router_module, "get_db", lambda: fake_db)
    monkeypatch.setattr(game_router_module, "GameService", FakeGameService)

    fallback_request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                game_service=None,
                settings=SimpleNamespace(SITE_ORIGIN="https://kriegspiel.org"),
            )
        )
    )

    service = get_game_service(fallback_request)

    assert isinstance(service, FakeGameService)
    assert captured == {
        "games": fake_db.games,
        "users": fake_db.users,
        "archives": fake_db.game_archives,
        "site_origin": "https://kriegspiel.org",
    }
