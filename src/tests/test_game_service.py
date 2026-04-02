from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from bson import ObjectId

from app.models.game import CreateGameRequest
from app.services.game_service import (
    GameConflictError,
    GameForbiddenError,
    GameNotFoundError,
    GameService,
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


@pytest.mark.asyncio
async def test_create_game_assigns_black_when_requested() -> None:
    games = FakeGamesCollection()
    service = GameService(games, site_origin="https://kriegspiel.org")

    response = await service.create_game(
        user_id="u1",
        username="creator",
        request=CreateGameRequest(rule_variant="berkeley_any", play_as="black", time_control="rapid"),
    )

    assert response.play_as == "black"
    saved = games.docs[0]
    assert saved["state"] == "waiting"
    assert saved["creator_color"] == "black"


@pytest.mark.asyncio
async def test_create_game_random_uses_injected_rng_choice() -> None:
    games = FakeGamesCollection()
    rng = type("Rng", (), {"choice": lambda self, values: "white"})()
    service = GameService(games, rng=rng)

    response = await service.create_game(
        user_id="u1",
        username="creator",
        request=CreateGameRequest(rule_variant="berkeley_any", play_as="random", time_control="rapid"),
    )

    assert response.play_as == "white"


@pytest.mark.asyncio
async def test_join_game_transitions_waiting_to_active_and_assigns_opposite_color() -> None:
    games = FakeGamesCollection()
    now = datetime.now(UTC)
    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "A7K2M9",
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

    response = await service.join_game(user_id="u2", username="joiner", game_code="a7k2m9")

    assert response.state == "active"
    assert response.play_as == "white"
    saved = games.docs[0]
    assert saved["state"] == "active"
    assert saved["white"]["user_id"] == "u2"
    assert saved["black"]["user_id"] == "u1"
    assert saved["white_scoresheet"]["color"] == "white"
    assert saved["black_scoresheet"]["color"] == "black"
    assert saved["white_scoresheet"]["moves_own"] == []
    assert saved["black_scoresheet"]["moves_opponent"] == []


@pytest.mark.asyncio
async def test_join_game_rejects_creator_missing_non_waiting_and_race() -> None:
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

    with pytest.raises(GameConflictError) as own:
        await service.join_game(user_id="u1", username="creator", game_code="B3H7Q2")
    assert own.value.code == "CANNOT_JOIN_OWN_GAME"

    with pytest.raises(GameNotFoundError):
        await service.join_game(user_id="u2", username="joiner", game_code="XXXXXX")

    games.docs[0]["state"] = "active"
    with pytest.raises(GameConflictError):
        await service.join_game(user_id="u2", username="joiner", game_code="B3H7Q2")

    games.docs[0]["state"] = "waiting"

    async def none_update(*args, **kwargs):  # noqa: ANN002, ANN003
        return None

    games.find_one_and_update = none_update  # type: ignore[method-assign]
    with pytest.raises(GameConflictError):
        await service.join_game(user_id="u2", username="joiner", game_code="B3H7Q2")


@pytest.mark.asyncio
async def test_get_open_games_returns_waiting_newest_first_and_bounded() -> None:
    games = FakeGamesCollection()
    now = datetime.now(UTC)
    for i in range(3):
        games.docs.append(
            {
                "_id": ObjectId(),
                "game_code": ["A7K2M9", "B7K2M8", "C7K2M7"][i],
                "rule_variant": "berkeley_any",
                "creator_color": "white",
                "white": {"user_id": f"u{i}", "username": f"user{i}", "connected": True},
                "black": None,
                "state": "waiting",
                "turn": None,
                "move_number": 1,
                "created_at": now + timedelta(minutes=i),
                "updated_at": now,
            }
        )

    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "ZZZZZZ",
            "rule_variant": "berkeley_any",
            "white": {"user_id": "u9", "username": "ignored", "connected": True},
            "black": {"user_id": "u8", "username": "full", "connected": True},
            "state": "active",
            "turn": "white",
            "move_number": 5,
            "created_at": now + timedelta(minutes=10),
            "updated_at": now,
        }
    )

    service = GameService(games)
    response = await service.get_open_games(limit=2)

    assert len(response.games) == 2
    assert [item.game_code for item in response.games] == ["C7K2M7", "B7K2M8"]


@pytest.mark.asyncio
async def test_get_game_and_my_games_include_only_participant_games() -> None:
    games = FakeGamesCollection()
    now = datetime.now(UTC)
    gid = ObjectId()
    games.docs.extend(
        [
            {
                "_id": gid,
                "game_code": "F4N7P2",
                "rule_variant": "berkeley_any",
                "white": {"user_id": "u1", "username": "w", "connected": True},
                "black": {"user_id": "u2", "username": "b", "connected": True},
                "state": "active",
                "turn": "white",
                "move_number": 1,
                "created_at": now,
                "updated_at": now,
            },
            {
                "_id": ObjectId(),
                "game_code": "G4N7P2",
                "rule_variant": "berkeley_any",
                "white": {"user_id": "u3", "username": "x", "connected": True},
                "black": None,
                "state": "waiting",
                "turn": None,
                "move_number": 1,
                "created_at": now + timedelta(minutes=1),
                "updated_at": now,
            },
        ]
    )
    service = GameService(games)

    game = await service.get_game(game_id=str(gid))
    mine = await service.get_my_games(user_id="u2", limit=10)

    assert game.game_code == "F4N7P2"
    assert [item.game_code for item in mine] == ["F4N7P2"]

    with pytest.raises(GameNotFoundError):
        await service.get_game(game_id="invalid")


@pytest.mark.asyncio
async def test_get_game_and_resign_and_delete_not_found_paths() -> None:
    games = FakeGamesCollection()
    service = GameService(games)

    with pytest.raises(GameNotFoundError):
        await service.get_game(game_id=str(ObjectId()))
    with pytest.raises(GameNotFoundError):
        await service.resign_game(game_id=str(ObjectId()), user_id="u1")
    with pytest.raises(GameNotFoundError):
        await service.delete_waiting_game(game_id=str(ObjectId()), user_id="u1")


@pytest.mark.asyncio
async def test_resign_requires_active_participant_and_completes_game_and_race() -> None:
    games = FakeGamesCollection()
    gid = ObjectId()
    now = datetime.now(UTC)
    games.docs.append(
        {
            "_id": gid,
            "game_code": "C4N7P2",
            "rule_variant": "berkeley_any",
            "white": {"user_id": "u1", "username": "w", "connected": True},
            "black": {"user_id": "u2", "username": "b", "connected": True},
            "state": "active",
            "turn": "white",
            "move_number": 7,
            "created_at": now,
            "updated_at": now,
        }
    )
    service = GameService(games)

    result = await service.resign_game(game_id=str(gid), user_id="u1")
    assert result["result"] == {"winner": "black", "reason": "resignation"}
    assert games.docs[0]["state"] == "completed"

    with pytest.raises(GameValidationError):
        await service.resign_game(game_id=str(gid), user_id="u2")

    games.docs[0]["state"] = "active"

    async def none_update(*args, **kwargs):  # noqa: ANN002, ANN003
        return None

    games.find_one_and_update = none_update  # type: ignore[method-assign]
    with pytest.raises(GameValidationError):
        await service.resign_game(game_id=str(gid), user_id="u2")


@pytest.mark.asyncio
async def test_resign_rejects_non_participant() -> None:
    games = FakeGamesCollection()
    gid = ObjectId()
    now = datetime.now(UTC)
    games.docs.append(
        {
            "_id": gid,
            "game_code": "D4N7P2",
            "rule_variant": "berkeley_any",
            "white": {"user_id": "u1", "username": "w", "connected": True},
            "black": {"user_id": "u2", "username": "b", "connected": True},
            "state": "active",
            "turn": "white",
            "move_number": 7,
            "created_at": now,
            "updated_at": now,
        }
    )
    service = GameService(games)

    with pytest.raises(GameForbiddenError):
        await service.resign_game(game_id=str(gid), user_id="u3")


@pytest.mark.asyncio
async def test_delete_waiting_game_requires_creator_waiting_and_handles_race() -> None:
    games = FakeGamesCollection()
    gid = ObjectId()
    now = datetime.now(UTC)
    games.docs.append(
        {
            "_id": gid,
            "game_code": "E4N7P2",
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

    with pytest.raises(GameForbiddenError):
        await service.delete_waiting_game(game_id=str(gid), user_id="u2")

    await service.delete_waiting_game(game_id=str(gid), user_id="u1")
    assert games.docs == []

    games.docs.append(
        {
            "_id": gid,
            "game_code": "E4N7P2",
            "rule_variant": "berkeley_any",
            "white": {"user_id": "u1", "username": "creator", "connected": True},
            "black": None,
            "state": "active",
            "turn": None,
            "move_number": 1,
            "created_at": now,
            "updated_at": now,
        }
    )
    with pytest.raises(GameConflictError):
        await service.delete_waiting_game(game_id=str(gid), user_id="u1")

    games.docs[0]["state"] = "waiting"

    async def zero_delete(*args, **kwargs):  # noqa: ANN002, ANN003
        return FakeDeleteResult(0)

    games.delete_one = zero_delete  # type: ignore[method-assign]
    with pytest.raises(GameConflictError):
        await service.delete_waiting_game(game_id=str(gid), user_id="u1")


@pytest.mark.asyncio
async def test_hydrate_document_and_waiting_black_player_mapping() -> None:
    games = FakeGamesCollection()
    gid = ObjectId()
    now = datetime.now(UTC)
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

    hydrated = await service.hydrate_document(game_id=str(gid))
    mine = await service.get_my_games(user_id="u1")

    assert hydrated.game_code == "K7K2M9"
    assert mine[0].black and mine[0].black.username == "creator"
