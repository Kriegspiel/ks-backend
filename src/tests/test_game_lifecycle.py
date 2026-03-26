from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from bson import ObjectId
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.config import Settings
from app.dependencies import get_current_user
from app.main import create_app
from app.models.game import CreateGameRequest
from app.models.user import UserModel
from app.routers.game import get_game_service
from app.services.game_service import (
    GameConflictError,
    GameForbiddenError,
    GameNotFoundError,
    GameService,
    GameServiceError,
    GameValidationError,
)


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


class FakeInsertResult:
    def __init__(self, inserted_id):
        self.inserted_id = inserted_id


class FakeDeleteResult:
    def __init__(self, deleted_count: int):
        self.deleted_count = deleted_count


class FakeGamesCollection:
    def __init__(self):
        self.docs: list[dict] = []

    async def find_one(self, query: dict, projection: dict | None = None):
        for doc in self.docs:
            if self._matches(doc, query):
                return doc
        return None

    async def insert_one(self, document: dict):
        doc = dict(document)
        doc["_id"] = ObjectId()
        self.docs.append(doc)
        return FakeInsertResult(doc["_id"])

    def find(self, query: dict):
        return FakeCursor([d for d in self.docs if self._matches(d, query)])

    async def find_one_and_update(self, query: dict, update: dict, return_document=None):  # noqa: ANN001
        for doc in self.docs:
            if self._matches(doc, query):
                for key, value in update.get("$set", {}).items():
                    doc[key] = value
                return doc
        return None

    async def delete_one(self, query: dict):
        for idx, doc in enumerate(self.docs):
            if self._matches(doc, query):
                self.docs.pop(idx)
                return FakeDeleteResult(1)
        return FakeDeleteResult(0)

    def _matches(self, doc: dict, query: dict) -> bool:
        if "$or" in query:
            return any(self._matches(doc, branch) for branch in query["$or"])
        for key, expected in query.items():
            if self._resolve(doc, key) != expected:
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


def _user(user_id: str, username: str) -> UserModel:
    return UserModel.from_mongo(
        {
            "_id": user_id,
            "username": username,
            "username_display": username,
            "email": f"{username}@example.com",
            "password_hash": "hash",
            "auth_providers": ["local"],
            "profile": {"bio": "", "avatar_url": None, "country": None},
            "stats": {"games_played": 0, "games_won": 0, "games_lost": 0, "games_drawn": 0, "elo": 1200, "elo_peak": 1200},
            "settings": {"board_theme": "default", "piece_set": "cburnett", "sound_enabled": True, "auto_ask_any": False},
            "role": "user",
            "status": "active",
            "last_active_at": datetime.now(UTC),
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        }
    )


@pytest.mark.asyncio
async def test_lifecycle_service_full_flow_and_delete_guardrails() -> None:
    games = FakeGamesCollection()
    service = GameService(games)

    created = await service.create_game(
        user_id="u1",
        username="creator",
        request=CreateGameRequest(rule_variant="berkeley_any", play_as="white", time_control="rapid"),
    )
    joined = await service.join_game(user_id="u2", username="joiner", game_code=created.game_code)
    assert joined.state == "active"

    game = await service.get_game(game_id=created.game_id)
    assert game.state == "active"
    assert game.white.username == "creator"
    assert game.black and game.black.username == "joiner"

    mine_creator = await service.get_my_games(user_id="u1")
    mine_joiner = await service.get_my_games(user_id="u2")
    assert len(mine_creator) == 1 and len(mine_joiner) == 1

    resign = await service.resign_game(game_id=created.game_id, user_id="u2")
    assert resign["result"]["winner"] == "white"

    waiting = await service.create_game(
        user_id="u3",
        username="owner",
        request=CreateGameRequest(rule_variant="berkeley_any", play_as="black", time_control="rapid"),
    )
    with pytest.raises(GameForbiddenError):
        await service.delete_waiting_game(game_id=waiting.game_id, user_id="u4")

    await service.delete_waiting_game(game_id=waiting.game_id, user_id="u3")


def test_lifecycle_router_integration_matrix_with_auth_regression() -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    games = FakeGamesCollection()
    service = GameService(games)
    current_user = {"value": _user("u1", "creator")}

    def _get_user() -> UserModel:
        return current_user["value"]

    app.dependency_overrides[get_current_user] = _get_user
    app.dependency_overrides[get_game_service] = lambda: service

    with TestClient(app) as client:
        create = client.post(
            "/api/game/create",
            json={"rule_variant": "berkeley_any", "play_as": "white", "time_control": "rapid"},
        )
        assert create.status_code == 201
        game_id = create.json()["game_id"]
        game_code = create.json()["game_code"]

        open_games = client.get("/api/game/open")
        assert open_games.status_code == 200
        assert any(g["game_code"] == game_code for g in open_games.json()["games"])

        current_user["value"] = _user("u2", "joiner")
        join = client.post(f"/api/game/join/{game_code}")
        assert join.status_code == 200
        assert join.json()["state"] == "active"

        mine = client.get("/api/game/mine")
        assert mine.status_code == 200
        assert any(g["game_id"] == game_id for g in mine.json()["games"])

        game = client.get(f"/api/game/{game_id}")
        assert game.status_code == 200
        assert game.json()["state"] == "active"

        resign = client.post(f"/api/game/{game_id}/resign")
        assert resign.status_code == 200
        assert resign.json()["result"] == {"winner": "white", "reason": "resignation"}

        waiting = client.post(
            "/api/game/create",
            json={"rule_variant": "berkeley_any", "play_as": "black", "time_control": "rapid"},
        )
        waiting_id = waiting.json()["game_id"]

        current_user["value"] = _user("u3", "outsider")
        denied_delete = client.delete(f"/api/game/{waiting_id}")
        assert denied_delete.status_code == 403
        assert denied_delete.json()["error"]["code"] == "FORBIDDEN"

    app_unauth = create_app(Settings(ENVIRONMENT="testing"))

    def raise_unauth():
        raise HTTPException(status_code=401, detail="Authentication required")

    app_unauth.dependency_overrides[get_current_user] = raise_unauth
    with TestClient(app_unauth) as client:
        unauthorized = client.get("/api/game/open")
        assert unauthorized.status_code == 401


@pytest.mark.asyncio
async def test_lifecycle_service_error_paths_raise_expected_codes() -> None:
    games = FakeGamesCollection()
    service = GameService(games)

    created = await service.create_game(
        user_id="u1",
        username="creator",
        request=CreateGameRequest(rule_variant="berkeley_any", play_as="white", time_control="rapid"),
    )

    with pytest.raises(GameConflictError):
        await service.join_game(user_id="u1", username="creator", game_code=created.game_code)

    with pytest.raises(GameNotFoundError):
        await service.join_game(user_id="u2", username="joiner", game_code="XXXXXX")

    await service.join_game(user_id="u2", username="joiner", game_code=created.game_code)

    with pytest.raises(GameConflictError):
        await service.join_game(user_id="u3", username="late", game_code=created.game_code)

    with pytest.raises(GameForbiddenError):
        await service.resign_game(game_id=created.game_id, user_id="u3")

    with pytest.raises(GameConflictError):
        await service.delete_waiting_game(game_id=created.game_id, user_id="u1")


@pytest.mark.asyncio
async def test_lifecycle_service_race_and_invalid_id_paths() -> None:
    games = FakeGamesCollection()
    service = GameService(games)
    created = await service.create_game(
        user_id="u1",
        username="creator",
        request=CreateGameRequest(rule_variant="berkeley_any", play_as="black", time_control="rapid"),
    )

    with pytest.raises(GameNotFoundError):
        await service.get_game(game_id="invalid")

    original = games.find_one_and_update

    async def none_update(*args, **kwargs):  # noqa: ANN002, ANN003
        return None

    games.find_one_and_update = none_update  # type: ignore[method-assign]
    with pytest.raises(GameConflictError):
        await service.join_game(user_id="u2", username="joiner", game_code=created.game_code)

    games.find_one_and_update = original  # type: ignore[method-assign]
    await service.join_game(user_id="u2", username="joiner", game_code=created.game_code)

    games.find_one_and_update = none_update  # type: ignore[method-assign]
    with pytest.raises(GameValidationError):
        await service.resign_game(game_id=created.game_id, user_id="u2")


def test_lifecycle_router_error_envelope_matrix() -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    app.dependency_overrides[get_current_user] = lambda: _user("u1", "creator")

    service = type("Svc", (), {})()
    service.create_game = AsyncMock(side_effect=GameServiceError(code="UNKNOWN_GAME_ERROR", message="boom"))
    service.join_game = AsyncMock(side_effect=GameNotFoundError("No game with code MISSING exists."))
    service.get_open_games = AsyncMock(side_effect=GameValidationError(code="BAD_REQUEST", message="bad"))
    service.get_my_games = AsyncMock(side_effect=GameConflictError(code="CONFLICT", message="conflict"))
    service.get_game = AsyncMock(side_effect=GameForbiddenError(code="FORBIDDEN", message="forbidden"))
    service.resign_game = AsyncMock(side_effect=GameValidationError(code="GAME_NOT_ACTIVE", message="inactive"))
    service.delete_waiting_game = AsyncMock(side_effect=GameConflictError(code="GAME_NOT_WAITING", message="gone"))
    app.dependency_overrides[get_game_service] = lambda: service

    with TestClient(app) as client:
        assert (
            client.post(
                "/api/game/create", json={"rule_variant": "berkeley_any", "play_as": "white", "time_control": "rapid"}
            ).status_code
            == 400
        )
        assert client.post("/api/game/join/MISSING").status_code == 404
        assert client.get("/api/game/open").status_code == 400
        assert client.get("/api/game/mine").status_code == 409
        assert client.get("/api/game/507f1f77bcf86cd799439011").status_code == 403
        assert client.post("/api/game/507f1f77bcf86cd799439011/resign").status_code == 400
        assert client.delete("/api/game/507f1f77bcf86cd799439011").status_code == 409
