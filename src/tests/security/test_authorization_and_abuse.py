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
from app.services.game_service import GameForbiddenError


def _user() -> UserModel:
    now = datetime.now(UTC)
    return UserModel.from_mongo(
        {
            "_id": "507f1f77bcf86cd799439011",
            "username": "playerone",
            "username_display": "PlayerOne",
            "email": "player@example.com",
            "password_hash": "hash",
            "auth_providers": ["local"],
            "profile": {"bio": "", "avatar_url": None, "country": None},
            "stats": {"games_played": 0, "games_won": 0, "games_lost": 0, "games_drawn": 0, "elo": 1200, "elo_peak": 1200},
            "settings": {"board_theme": "default", "piece_set": "cburnett", "sound_enabled": True, "auto_ask_any": False},
            "role": "user",
            "status": "active",
            "last_active_at": now,
            "created_at": now,
            "updated_at": now,
        }
    )


def test_non_participant_gets_403_for_protected_game_state() -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    service = SimpleNamespace(
        get_game_state=AsyncMock(side_effect=GameForbiddenError(code="FORBIDDEN", message="Only participants"))
    )
    app.dependency_overrides[get_current_user] = lambda: _user()
    app.dependency_overrides[get_game_service] = lambda: service

    with TestClient(app) as client:
        response = client.get("/api/game/gid1/state")

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "FORBIDDEN"


def test_malformed_move_payload_rejected_with_4xx() -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    app.dependency_overrides[get_current_user] = lambda: _user()
    app.dependency_overrides[get_game_service] = lambda: SimpleNamespace(execute_move=AsyncMock())

    with TestClient(app) as client:
        response = client.post("/api/game/gid1/move", json={"uci": 12345})

    assert response.status_code in (400, 422)


def test_unauthenticated_game_route_returns_401() -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    app.dependency_overrides[get_current_user] = lambda: (_ for _ in ()).throw(
        HTTPException(status_code=401, detail="Authentication required")
    )

    with TestClient(app) as client:
        response = client.get("/api/game/open")

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_hidden_piece_leakage_absent_for_opponents() -> None:
    from bson import ObjectId
    from app.services.engine_adapter import create_new_game, serialize_game_state
    from app.services.game_service import GameService

    class _Games:
        def __init__(self, doc):
            self.doc = doc

        async def find_one(self, query: dict, projection: dict | None = None):
            return self.doc if query.get("_id") == self.doc["_id"] else None

        def find(self, _query: dict):
            return []

    now = datetime.now(UTC)
    gid = ObjectId()
    service = GameService(
        _Games(
            {
                "_id": gid,
                "game_code": "A7K2M9",
                "rule_variant": "berkeley_any",
                "creator_color": "white",
                "white": {"user_id": "u1", "username": "w", "connected": True},
                "black": {"user_id": "u2", "username": "b", "connected": True},
                "state": "active",
                "turn": "white",
                "move_number": 1,
                "moves": [],
                "engine_state": serialize_game_state(create_new_game(any_rule=True)),
                "created_at": now,
                "updated_at": now,
            }
        )
    )

    white_state = await service.get_game_state(game_id=str(gid), user_id="u1")
    black_state = await service.get_game_state(game_id=str(gid), user_id="u2")
    assert "p" not in white_state.your_fen.split(" ")[0]
    assert "P" not in black_state.your_fen.split(" ")[0]
