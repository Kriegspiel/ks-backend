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
from app.services.engine_adapter import create_new_game, serialize_game_state
from app.services.game_service import GameConflictError, GameForbiddenError, GameService, GameValidationError


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
    def __init__(self, inserted_id: ObjectId):
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
                for key, value in update.get("$inc", {}).items():
                    doc[key] = doc.get(key, 0) + value
                for key, value in update.get("$push", {}).items():
                    doc.setdefault(key, []).append(value)
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
def active_game_doc() -> dict:
    gid = ObjectId()
    now = datetime.now(UTC)
    return {
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


@pytest.mark.asyncio
async def test_execute_move_persists_and_flips_turn(active_game_doc: dict) -> None:
    games = FakeGamesCollection()
    games.docs.append(active_game_doc)
    service = GameService(games)

    response = await service.execute_move(game_id=str(active_game_doc["_id"]), user_id="u1", uci="e2e4")

    assert response["move_done"] is True
    assert response["turn"] == "black"
    assert games.docs[0]["turn"] == "black"
    assert games.docs[0]["move_number"] == 2
    assert games.docs[0]["moves"][0]["question_type"] == "COMMON"
    assert games.docs[0]["white_scoresheet"]["moves_own"]
    assert games.docs[0]["black_scoresheet"]["moves_opponent"]


@pytest.mark.asyncio
async def test_execute_move_illegal_is_deterministic_and_non_terminal(active_game_doc: dict) -> None:
    games = FakeGamesCollection()
    games.docs.append(active_game_doc)
    service = GameService(games)

    response = await service.execute_move(game_id=str(active_game_doc["_id"]), user_id="u1", uci="e2e5")

    assert response["move_done"] is False
    assert response["game_over"] is False
    assert response["announcement"]
    assert games.docs[0]["move_number"] == 1
    assert games.docs[0]["state"] == "active"


@pytest.mark.asyncio
async def test_execute_move_rejects_non_participant_and_out_of_turn(active_game_doc: dict) -> None:
    games = FakeGamesCollection()
    games.docs.append(active_game_doc)
    service = GameService(games)

    with pytest.raises(GameForbiddenError) as non_participant:
        await service.execute_move(game_id=str(active_game_doc["_id"]), user_id="u3", uci="e2e4")
    assert non_participant.value.code == "FORBIDDEN"

    with pytest.raises(GameValidationError) as out_of_turn:
        await service.execute_move(game_id=str(active_game_doc["_id"]), user_id="u2", uci="e7e5")
    assert out_of_turn.value.code == "NOT_YOUR_TURN"


@pytest.mark.asyncio
async def test_execute_ask_any_records_result_without_uci_leak(active_game_doc: dict) -> None:
    games = FakeGamesCollection()
    games.docs.append(active_game_doc)
    service = GameService(games)

    response = await service.execute_ask_any(game_id=str(active_game_doc["_id"]), user_id="u1")

    assert "has_any" in response
    assert games.docs[0]["moves"][0]["question_type"] == "ASK_ANY"
    assert games.docs[0]["moves"][0]["uci"] is None


@pytest.fixture
def app_with_move_service() -> tuple:
    app = create_app(Settings(ENVIRONMENT="testing"))

    service = SimpleNamespace(
        execute_move=AsyncMock(
            return_value={
                "move_done": True,
                "announcement": "REGULAR_MOVE",
                "special_announcement": None,
                "capture_square": None,
                "turn": "black",
                "game_over": False,
                "clock": {"white_remaining": 1500.0, "black_remaining": 1500.0, "active_color": "black"},
            }
        ),
        execute_ask_any=AsyncMock(
            return_value={
                "move_done": False,
                "announcement": "HAS_ANY",
                "special_announcement": None,
                "capture_square": None,
                "turn": "white",
                "game_over": False,
                "has_any": True,
                "clock": {"white_remaining": 1500.0, "black_remaining": 1500.0, "active_color": "white"},
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


def test_move_and_ask_any_routes_happy_path(app_with_move_service) -> None:
    app, _service = app_with_move_service

    with TestClient(app, raise_server_exceptions=False) as client:
        move = client.post("/api/game/gid1/move", json={"uci": "e2e4"})
        ask_any = client.post("/api/game/gid1/ask-any")

    assert move.status_code == 200
    assert move.json()["turn"] == "black"
    assert ask_any.status_code == 200
    assert ask_any.json()["has_any"] is True


def test_move_and_ask_any_routes_map_errors(app_with_move_service) -> None:
    app, service = app_with_move_service
    service.execute_move = AsyncMock(side_effect=GameValidationError(code="NOT_YOUR_TURN", message="It is not your turn"))
    service.execute_ask_any = AsyncMock(side_effect=GameForbiddenError(code="FORBIDDEN", message="Only participants"))

    with TestClient(app, raise_server_exceptions=False) as client:
        move = client.post("/api/game/gid1/move", json={"uci": "e2e4"})
        ask_any = client.post("/api/game/gid1/ask-any")

    assert move.status_code == 400
    assert move.json()["error"]["code"] == "NOT_YOUR_TURN"
    assert ask_any.status_code == 403
    assert ask_any.json()["error"]["code"] == "FORBIDDEN"


@pytest.mark.asyncio
async def test_create_join_open_mine_get_and_hydrate_paths() -> None:
    games = FakeGamesCollection()
    service = GameService(games, rng=type("Rng", (), {"choice": lambda self, values: "white"})())

    created = await service.create_game(
        user_id="u1",
        username="creator",
        request=__import__("app.models.game", fromlist=["CreateGameRequest"]).CreateGameRequest(
            rule_variant="berkeley_any", play_as="random", time_control="rapid"
        ),
    )
    assert created.play_as == "white"

    joined = await service.join_game(user_id="u2", username="joiner", game_code=created.game_code)
    assert joined.state == "active"

    open_games = await service.get_open_games()
    assert len(open_games.games) == 0

    mine = await service.get_my_games(user_id="u2")
    assert len(mine) == 1

    fetched = await service.get_game(game_id=joined.game_id)
    assert fetched.game_code == created.game_code

    hydrated = await service.hydrate_document(game_id=joined.game_id)
    assert hydrated.game_code == created.game_code


@pytest.mark.asyncio
async def test_join_conflicts_and_not_found_and_delete_paths() -> None:
    games = FakeGamesCollection()
    now = datetime.now(UTC)
    gid = ObjectId()
    games.docs.append(
        {
            "_id": gid,
            "game_code": "B3H7Q2",
            "rule_variant": "berkeley_any",
            "white": {"user_id": "u1", "username": "creator", "connected": True},
            "black": None,
            "state": "waiting",
            "turn": None,
            "move_number": 1,
            "created_at": now,
            "updated_at": now,
        }
    )
    service = GameService(games)

    with pytest.raises(Exception):
        await service.join_game(user_id="u1", username="creator", game_code="B3H7Q2")
    with pytest.raises(Exception):
        await service.join_game(user_id="u2", username="joiner", game_code="ZZZZZZ")

    await service.delete_waiting_game(game_id=str(gid), user_id="u1")
    assert games.docs == []


@pytest.mark.asyncio
async def test_resign_and_delete_error_guards() -> None:
    games = FakeGamesCollection()
    now = datetime.now(UTC)
    gid = ObjectId()
    games.docs.append(
        {
            "_id": gid,
            "game_code": "C4N7P2",
            "rule_variant": "berkeley_any",
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
    service = GameService(games)

    resigned = await service.resign_game(game_id=str(gid), user_id="u1")
    assert resigned["result"]["winner"] == "black"

    with pytest.raises(GameValidationError):
        await service.resign_game(game_id=str(gid), user_id="u2")

    games.docs[0]["state"] = "waiting"
    with pytest.raises(GameValidationError):
        await service.execute_move(game_id=str(gid), user_id="u1", uci="e2e4")


def test_router_more_paths_and_error_mapping() -> None:
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
                "game_url": "https://kriegspiel.org/game/gid1",
            }
        ),
        get_open_games=AsyncMock(return_value={"games": []}),
        get_my_games=AsyncMock(return_value=[]),
        get_game=AsyncMock(
            return_value={
                "game_id": "gid1",
                "game_code": "A7K2M9",
                "rule_variant": "berkeley_any",
                "state": "active",
                "white": {"username": "w", "connected": True},
                "black": {"username": "b", "connected": True},
                "turn": "white",
                "move_number": 1,
                "created_at": datetime.now(UTC),
            }
        ),
        resign_game=AsyncMock(return_value={"result": {"winner": "black", "reason": "resignation"}}),
        delete_waiting_game=AsyncMock(return_value=None),
        execute_move=AsyncMock(side_effect=GameValidationError(code="NOT_YOUR_TURN", message="It is not your turn")),
        execute_ask_any=AsyncMock(side_effect=GameForbiddenError(code="FORBIDDEN", message="Only participants")),
    )
    app.dependency_overrides[get_current_user] = lambda: UserModel.from_mongo(
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
            "last_active_at": datetime.now(UTC),
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        }
    )
    app.dependency_overrides[get_game_service] = lambda: service

    with TestClient(app, raise_server_exceptions=False) as client:
        assert (
            client.post(
                "/api/game/create", json={"rule_variant": "berkeley_any", "play_as": "white", "time_control": "rapid"}
            ).status_code
            == 201
        )
        assert client.post("/api/game/join/A7K2M9").status_code == 200
        assert client.get("/api/game/open").status_code == 200
        assert client.get("/api/game/mine").status_code == 200
        assert client.get("/api/game/gid1").status_code == 200
        assert client.post("/api/game/gid1/resign").status_code == 200
        assert client.delete("/api/game/gid1").status_code == 204
        assert client.post("/api/game/gid1/move", json={"uci": "e2e4"}).status_code == 400
        assert client.post("/api/game/gid1/ask-any").status_code == 403


@pytest.mark.asyncio
async def test_service_not_found_and_id_validation_paths() -> None:
    games = FakeGamesCollection()
    service = GameService(games)
    with pytest.raises(Exception):
        await service.get_game(game_id="bad-id")
    with pytest.raises(Exception):
        await service.execute_move(game_id=str(ObjectId()), user_id="u1", uci="e2e4")
    with pytest.raises(Exception):
        await service.execute_ask_any(game_id=str(ObjectId()), user_id="u1")
    with pytest.raises(Exception):
        await service.delete_waiting_game(game_id=str(ObjectId()), user_id="u1")


@pytest.mark.asyncio
async def test_service_race_conditions_on_updates() -> None:
    games = FakeGamesCollection()
    gid = ObjectId()
    now = datetime.now(UTC)
    games.docs.append(
        {
            "_id": gid,
            "game_code": "R4C3ZZ",
            "rule_variant": "berkeley_any",
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
    service = GameService(games)

    async def none_update(*args, **kwargs):
        return None

    games.find_one_and_update = none_update  # type: ignore[method-assign]
    with pytest.raises(GameValidationError):
        await service.execute_move(game_id=str(gid), user_id="u1", uci="e2e4")
    with pytest.raises(GameValidationError):
        await service.execute_ask_any(game_id=str(gid), user_id="u1")
    with pytest.raises(GameValidationError):
        await service.resign_game(game_id=str(gid), user_id="u1")


def test_router_maps_more_error_classes() -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    service = SimpleNamespace(
        create_game=AsyncMock(side_effect=Exception("boom")),
        join_game=AsyncMock(side_effect=Exception("boom")),
        get_open_games=AsyncMock(side_effect=Exception("boom")),
        get_my_games=AsyncMock(side_effect=Exception("boom")),
        get_game=AsyncMock(side_effect=Exception("boom")),
        resign_game=AsyncMock(side_effect=Exception("boom")),
        delete_waiting_game=AsyncMock(side_effect=Exception("boom")),
        execute_move=AsyncMock(side_effect=Exception("boom")),
        execute_ask_any=AsyncMock(side_effect=Exception("boom")),
    )
    app.dependency_overrides[get_current_user] = lambda: UserModel.from_mongo(
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
            "last_active_at": datetime.now(UTC),
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        }
    )
    app.dependency_overrides[get_game_service] = lambda: service

    with TestClient(app, raise_server_exceptions=False) as client:
        assert (
            client.post(
                "/api/game/create", json={"rule_variant": "berkeley_any", "play_as": "white", "time_control": "rapid"}
            ).status_code
            == 500
        )


@pytest.mark.asyncio
async def test_service_additional_branches_for_join_delete_and_metadata() -> None:
    games = FakeGamesCollection()
    now = datetime.now(UTC)
    gid = ObjectId()
    games.docs.append(
        {
            "_id": gid,
            "game_code": "K7K2M9",
            "rule_variant": "berkeley_any",
            "creator_color": "black",
            "white": {"user_id": "u1", "username": "creator", "connected": True},
            "black": None,
            "state": "waiting",
            "turn": None,
            "move_number": 1,
            "created_at": now,
            "updated_at": now,
        }
    )
    service = GameService(games)

    # waiting+creator black branch in _resolve_players
    mine = await service.get_my_games(user_id="u1")
    assert mine[0].black and mine[0].black.username == "creator"

    # joiner gets white when creator picked black
    joined = await service.join_game(user_id="u2", username="joiner", game_code="K7K2M9")
    assert joined.play_as == "white"

    with pytest.raises(GameConflictError):
        await service.join_game(user_id="u3", username="x", game_code="K7K2M9")

    games.docs[0]["state"] = "waiting"
    with pytest.raises(GameForbiddenError):
        await service.delete_waiting_game(game_id=str(gid), user_id="u9")

    async def zero_delete(*args, **kwargs):
        return FakeDeleteResult(0)

    games.delete_one = zero_delete  # type: ignore[method-assign]
    with pytest.raises(GameConflictError):
        await service.delete_waiting_game(game_id=str(gid), user_id="u2")


@pytest.mark.asyncio
async def test_service_game_over_result_mapping_branches(monkeypatch) -> None:
    games = FakeGamesCollection()
    now = datetime.now(UTC)
    gid = ObjectId()
    games.docs.append(
        {
            "_id": gid,
            "game_code": "M7K2M9",
            "rule_variant": "berkeley_any",
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
    service = GameService(games)

    def fake_attempt(_game, _uci):
        return {
            "move_done": True,
            "announcement": "REGULAR_MOVE",
            "special_announcement": "CHECKMATE_WHITE_WINS",
            "capture_square": None,
            "turn": "black",
            "game_over": True,
        }

    monkeypatch.setattr("app.services.game_service.attempt_move", fake_attempt)
    out = await service.execute_move(game_id=str(gid), user_id="u1", uci="e2e4")
    assert out["game_over"] is True
    assert games.docs[0]["state"] == "completed"
    assert games.docs[0]["result"]["winner"] == "white"


def test_router_auth_required_for_new_mutation_endpoints() -> None:
    from fastapi import HTTPException

    app = create_app(Settings(ENVIRONMENT="testing"))

    def raise_unauth():
        raise HTTPException(status_code=401, detail="Authentication required")

    app.dependency_overrides[get_current_user] = raise_unauth
    with TestClient(app, raise_server_exceptions=False) as client:
        assert client.post("/api/game/gid1/move", json={"uci": "e2e4"}).status_code == 401
        assert client.post("/api/game/gid1/ask-any").status_code == 401
