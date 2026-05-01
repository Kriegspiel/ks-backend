from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from bson import ObjectId

from app.models.game import CreateGameRequest
from app.models.user import default_user_stats_payload
from app.services.engine_adapter import create_new_game, serialize_game_state
from app.services.game_service import (
    BOT_GAME_FLUSH_PLIES,
    BOT_GAME_IDLE_FLUSH,
    CachedGameEntry,
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

    async def count_documents(self, query: dict):
        return sum(1 for doc in self.docs if self._matches(doc, query))

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
            if isinstance(expected, dict):
                if "$gte" in expected and not (value is not None and value >= expected["$gte"]):
                    return False
                if "$gt" in expected and not (value is not None and value > expected["$gt"]):
                    return False
                if "$lte" in expected and not (value is not None and value <= expected["$lte"]):
                    return False
                if "$lt" in expected and not (value is not None and value < expected["$lt"]):
                    return False
                continue
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


class FakeUsersCollection:
    def __init__(self, docs: list[dict] | None = None):
        self.docs = docs or []

    async def find_one(self, query: dict, projection: dict | None = None):
        for doc in self.docs:
            matches = True
            for key, expected in query.items():
                current = doc
                for part in key.split("."):
                    if not isinstance(current, dict):
                        matches = False
                        break
                    current = current.get(part)
                if not matches or current != expected:
                    matches = False
                    break
            if matches:
                return doc
        return None

    async def find_one_and_update(self, query: dict, update: dict, return_document=None):  # noqa: ANN001
        for doc in self.docs:
            matches = True
            for key, expected in query.items():
                current = doc
                for part in key.split("."):
                    if not isinstance(current, dict):
                        matches = False
                        break
                    current = current.get(part)
                if not matches or current != expected:
                    matches = False
                    break
            if not matches:
                continue
            for key, value in update.get("$set", {}).items():
                current = doc
                parts = key.split(".")
                for part in parts[:-1]:
                    current = current.setdefault(part, {})
                current[parts[-1]] = value
            return doc
        return None


def active_game_doc(
    *,
    white_role: str = "user",
    black_role: str = "user",
    turn: str = "white",
    rule_variant: str = "berkeley_any",
) -> dict:
    now = datetime.now(UTC)
    engine = create_new_game(rule_variant=rule_variant)
    return {
        "_id": ObjectId(),
        "game_code": "ACTIVE1",
        "rule_variant": rule_variant,
        "white": {"user_id": "u1", "username": "w", "connected": True, "role": white_role},
        "black": {"user_id": "u2", "username": "b", "connected": True, "role": black_role},
        "state": "active",
        "turn": turn,
        "move_number": 1,
        "moves": [],
        "engine_state": serialize_game_state(engine),
        "time_control": {
            "base": 900.0,
            "increment": 10.0,
            "white_remaining": 900.0,
            "black_remaining": 900.0,
            "active_color": turn,
            "last_updated_at": now,
        },
        "created_at": now,
        "updated_at": now,
    }


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
    assert saved["expires_at"] - saved["created_at"] == timedelta(minutes=10)


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
    assert saved["time_control"]["active_color"] is None
    assert "white_scoresheet" not in saved


@pytest.mark.asyncio
async def test_bot_game_clock_waits_for_white_first_move() -> None:
    bot_id = ObjectId()
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    users.docs.append({"_id": bot_id, "username": "randobot", "role": "bot", "status": "active"})
    service = GameService(games, users_collection=users)

    response = await service.create_game(
        user_id="u1",
        username="creator",
        request=CreateGameRequest(opponent_type="bot", bot_id=str(bot_id), play_as="white", time_control="rapid"),
    )

    assert response.state == "active"
    assert games.docs[0]["time_control"]["active_color"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("rule_variant", ["cincinnati", "wild16"])
async def test_join_game_bootstraps_matching_ruleset_engine(rule_variant: str) -> None:
    games = FakeGamesCollection()
    now = datetime.now(UTC)
    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "A7K2M9",
            "rule_variant": rule_variant,
            "creator_color": "white",
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

    await service.join_game(user_id="u2", username="joiner", game_code="A7K2M9")

    assert games.docs[0]["engine_state"]["game_state"]["ruleset_id"] == rule_variant


@pytest.mark.asyncio
async def test_execute_move_hides_wild16_private_illegal_attempts_from_shared_history() -> None:
    games = FakeGamesCollection()
    games.docs.append(active_game_doc(rule_variant="wild16"))
    service = GameService(games)

    response = await service.execute_move(game_id=str(games.docs[0]["_id"]), user_id="u1", uci="e2e5")
    await service.flush_all()

    assert response["announcement"] == "ILLEGAL_MOVE"
    assert response["move_done"] is False
    assert games.docs[0]["moves"] == []
    own_scoresheet = games.docs[0]["engine_state"]["game_state"]["white_scoresheet"]["moves_own"]
    assert own_scoresheet[0][0][1]["main_announcement"] == "ILLEGAL_MOVE"
    assert games.docs[0]["engine_state"]["game_state"]["black_scoresheet"]["moves_opponent"] == []


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
            "expires_at": now + timedelta(minutes=10),
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
async def test_join_game_rejects_and_deletes_expired_waiting_game() -> None:
    now = datetime.now(UTC)

    class FrozenGameService(GameService):
        @staticmethod
        def utcnow() -> datetime:
            return now

    games = FakeGamesCollection()
    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "Z7K2M9",
            "rule_variant": "berkeley_any",
            "creator_color": "white",
            "white": {"user_id": "u1", "username": "creator", "connected": True},
            "black": None,
            "state": "waiting",
            "turn": None,
            "move_number": 1,
            "created_at": now - timedelta(minutes=11),
            "updated_at": now - timedelta(minutes=11),
            "expires_at": now - timedelta(seconds=1),
        }
    )
    service = FrozenGameService(games)

    with pytest.raises(GameNotFoundError):
        await service.join_game(user_id="u2", username="joiner", game_code="Z7K2M9")

    assert games.docs == []


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
                "expires_at": now + timedelta(minutes=10 + i),
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
async def test_expire_waiting_games_removes_expired_entries() -> None:
    now = datetime.now(UTC)
    games = FakeGamesCollection()
    expired_id = ObjectId()
    fresh_id = ObjectId()
    games.docs.extend(
        [
            {
                "_id": expired_id,
                "game_code": "E7K2M9",
                "rule_variant": "berkeley_any",
                "creator_color": "white",
                "white": {"user_id": "u1", "username": "creator", "connected": True},
                "black": None,
                "state": "waiting",
                "created_at": now - timedelta(minutes=11),
                "updated_at": now - timedelta(minutes=11),
                "expires_at": now - timedelta(seconds=1),
            },
            {
                "_id": fresh_id,
                "game_code": "F7K2M9",
                "rule_variant": "berkeley_any",
                "creator_color": "white",
                "white": {"user_id": "u2", "username": "fresh", "connected": True},
                "black": None,
                "state": "waiting",
                "created_at": now,
                "updated_at": now,
                "expires_at": now + timedelta(minutes=10),
            },
        ]
    )
    service = GameService(games)

    await service._expire_waiting_games(now=now)

    remaining_ids = {doc["_id"] for doc in games.docs}
    assert expired_id not in remaining_ids
    assert fresh_id in remaining_ids


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
async def test_get_game_uses_live_user_elo_over_stale_embedded_elo() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection(
        docs=[
            {"_id": "u1", "username": "w", "stats": {"elo": 1333}},
            {"_id": "u2", "username": "b", "stats": {"elo": 1444}},
        ]
    )
    now = datetime.now(UTC)
    gid = ObjectId()
    games.docs.append(
        {
            "_id": gid,
            "game_code": "R4N7P2",
            "rule_variant": "berkeley_any",
            "white": {"user_id": "u1", "username": "w", "connected": True, "elo": 1200},
            "black": {"user_id": "u2", "username": "b", "connected": True, "elo": 1200},
            "state": "active",
            "turn": "white",
            "move_number": 12,
            "created_at": now,
            "updated_at": now,
        }
    )
    service = GameService(games, users_collection=users)

    game = await service.get_game(game_id=str(gid))

    assert game.white.elo == 1333
    assert game.black.elo == 1444


@pytest.mark.asyncio
async def test_get_game_uses_archive_and_derives_missing_insufficient_reason() -> None:
    games = FakeGamesCollection()
    archives = FakeGamesCollection()
    now = datetime.now(UTC)
    gid = ObjectId()
    archives.docs.append(
        {
            "_id": gid,
            "game_code": "H4N7P2",
            "rule_variant": "berkeley_any",
            "white": {"user_id": "u1", "username": "w", "connected": True, "role": "user"},
            "black": {"user_id": "u2", "username": "b", "connected": True, "role": "bot"},
            "state": "completed",
            "turn": None,
            "move_number": 9,
            "moves": [
                {
                    "ply": 8,
                    "color": "black",
                    "question_type": "COMMON",
                    "uci": "a1a2",
                    "announcement": "REGULAR_MOVE",
                    "special_announcement": "DRAW_INSUFFICIENT",
                    "move_done": True,
                    "timestamp": now,
                }
            ],
            "result": {"winner": None, "reason": None},
            "created_at": now,
            "updated_at": now,
        }
    )
    service = GameService(games, archives_collection=archives)

    game = await service.get_game(game_id=str(gid))

    assert game.game_code == "H4N7P2"
    assert game.result == {"winner": None, "reason": "insufficient"}


@pytest.mark.asyncio
async def test_get_lobby_stats_counts_active_and_completed_windows() -> None:
    games = FakeGamesCollection()
    archives = FakeGamesCollection()
    now = datetime(2026, 4, 3, 22, 0, tzinfo=UTC)
    games.docs.extend(
        [
            {"_id": ObjectId(), "state": "active", "updated_at": now},
            {"_id": ObjectId(), "state": "active", "updated_at": now},
            {"_id": ObjectId(), "state": "waiting", "updated_at": now},
        ]
    )
    archives.docs.extend(
        [
            {"_id": ObjectId(), "state": "completed", "updated_at": now - timedelta(minutes=15)},
            {"_id": ObjectId(), "state": "completed", "updated_at": now - timedelta(hours=3)},
            {"_id": ObjectId(), "state": "completed", "updated_at": now - timedelta(days=2)},
        ]
    )
    service = GameService(games, archives_collection=archives)
    service.utcnow = lambda: now  # type: ignore[method-assign]

    stats = await service.get_lobby_stats()

    assert stats.active_games_now == 2
    assert stats.completed_last_hour == 1
    assert stats.completed_last_24_hours == 2
    assert stats.completed_total == 3


@pytest.mark.asyncio
async def test_stored_scoresheets_prefers_engine_state_only() -> None:
    game = {
        "engine_state": serialize_game_state(create_new_game(any_rule=True)),
        "moves": [],
    }

    scoresheets = GameService._stored_scoresheets(game)

    assert scoresheets["white"]["color"] == "white"
    assert scoresheets["black"]["color"] == "black"


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
    game = active_game_doc()
    game["_id"] = ObjectId()
    game["game_code"] = "C4N7P2"
    game["move_number"] = 7
    games.docs.append(game)
    service = GameService(games)

    result = await service.resign_game(game_id=str(game["_id"]), user_id="u1")
    assert result["result"] == {"winner": "black", "reason": "resignation"}
    assert games.docs[0]["state"] == "active"
    await service.flush_all()
    assert games.docs[0]["state"] == "completed"

    with pytest.raises(GameValidationError):
        await service.resign_game(game_id=str(game["_id"]), user_id="u2")

    games.docs[0]["state"] = "active"
    games.docs[0]["result"] = None
    games.docs[0]["turn"] = "white"
    games.docs[0]["time_control"]["active_color"] = "white"
    service = GameService(games)
    await service.resign_game(game_id=str(game["_id"]), user_id="u2")
    await service.shutdown()
    assert games.docs[0]["result"] == {"winner": "white", "reason": "resignation"}


@pytest.mark.asyncio
async def test_resign_rejects_non_participant() -> None:
    games = FakeGamesCollection()
    game = active_game_doc()
    game["_id"] = ObjectId()
    game["game_code"] = "D4N7P2"
    games.docs.append(game)
    service = GameService(games)

    with pytest.raises(GameForbiddenError):
        await service.resign_game(game_id=str(game["_id"]), user_id="u3")


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


@pytest.mark.asyncio
async def test_human_visible_action_stays_in_cache_until_flushed() -> None:
    games = FakeGamesCollection()
    game = active_game_doc()
    games.docs.append(game)
    service = GameService(games)

    response = await service.execute_ask_any(game_id=str(game["_id"]), user_id="u1")

    assert response["announcement"] in {"HAS_ANY", "NO_ANY"}
    assert games.docs[0]["moves"] == []
    await service.flush_all()
    assert len(games.docs[0]["moves"]) == 1
    assert games.docs[0]["moves"][0]["question_type"] == "ASK_ANY"


@pytest.mark.asyncio
async def test_shutdown_flushes_dirty_cached_games() -> None:
    games = FakeGamesCollection()
    game = active_game_doc(white_role="bot", black_role="bot")
    game["moves"] = [{"ply": index + 1, "question_type": "COMMON"} for index in range(19)]
    games.docs.append(game)
    service = GameService(games)

    await service.execute_ask_any(game_id=str(game["_id"]), user_id="u1")

    assert len(games.docs[0]["moves"]) == 19
    await service.shutdown()
    assert len(games.docs[0]["moves"]) == 20


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("winner", "white_bucket", "black_bucket"),
    [
        ("white", "games_won", "games_lost"),
        ("black", "games_lost", "games_won"),
        (None, "games_drawn", "games_drawn"),
    ],
)
async def test_finalize_completed_game_updates_stats_and_archive_for_all_outcomes(
    winner: str | None,
    white_bucket: str,
    black_bucket: str,
) -> None:
    games = FakeGamesCollection()
    archives = FakeGamesCollection()
    users = FakeUsersCollection(
        docs=[
            {
                "_id": "white-user",
                "username": "white",
                "role": "user",
                "stats": default_user_stats_payload(),
                "bot_profile": None,
            },
            {
                "_id": "black-bot",
                "username": "blackbot",
                "role": "bot",
                "stats": default_user_stats_payload(),
                "bot_profile": {"display_name": "Black Bot"},
            },
        ]
    )
    now = datetime(2026, 4, 7, tzinfo=UTC)
    game = {
        "_id": ObjectId(),
        "game_code": "DONE42",
        "rule_variant": "berkeley_any",
        "white": {"user_id": "white-user", "username": "white", "role": "user"},
        "black": {"user_id": "black-bot", "username": "blackbot", "role": "bot"},
        "state": "completed",
        "result": {"winner": winner, "reason": "stalemate" if winner is None else "checkmate"},
        "moves": [],
        "created_at": now,
        "updated_at": now,
    }
    games.docs.append(game)
    service = GameService(games, archives_collection=archives, users_collection=users)

    finalized = await service._finalize_completed_game(game)

    assert finalized["stats_recorded_at"] is not None
    assert finalized["rating_snapshot"]["white_track"] == "vs_bots"
    assert finalized["rating_snapshot"]["black_track"] == "vs_humans"
    assert archives.docs[0]["_id"] == game["_id"]

    white_stats = users.docs[0]["stats"]
    black_stats = users.docs[1]["stats"]
    assert white_stats["games_played"] == 1
    assert black_stats["games_played"] == 1
    assert white_stats[white_bucket] == 1
    assert black_stats[black_bucket] == 1
    assert white_stats["results"]["overall"][white_bucket] == 1
    assert black_stats["results"]["overall"][black_bucket] == 1
    assert white_stats["results"]["vs_bots"][white_bucket] == 1
    assert black_stats["results"]["vs_humans"][black_bucket] == 1


@pytest.mark.asyncio
async def test_finalize_completed_game_short_circuits_when_not_completed_or_already_recorded() -> None:
    games = FakeGamesCollection()
    archives = FakeGamesCollection()
    service = GameService(games, archives_collection=archives)
    waiting_game = {"_id": ObjectId(), "state": "waiting"}
    recorded_game = {
        "_id": ObjectId(),
        "state": "completed",
        "stats_recorded_at": datetime(2026, 4, 8, tzinfo=UTC),
        "white": {"user_id": "u1", "username": "white"},
        "black": {"user_id": "u2", "username": "black"},
        "result": {"winner": "white", "reason": "checkmate"},
    }

    assert await service._finalize_completed_game(waiting_game) is waiting_game
    await service._finalize_completed_game(recorded_game)

    assert archives.docs[0]["_id"] == recorded_game["_id"]


@pytest.mark.asyncio
async def test_game_service_user_lookup_update_and_join_cooldown_support_object_id_strings() -> None:
    oid = ObjectId()
    users = FakeUsersCollection(
        docs=[
            {
                "_id": oid,
                "username": "joinerbot",
                "role": "bot",
                "stats": default_user_stats_payload(),
                "bot_profile": {"display_name": "Joiner Bot"},
            }
        ]
    )
    service = GameService(FakeGamesCollection(), users_collection=users)

    assert (await service._find_user_doc(str(oid)))["username"] == "joinerbot"
    assert await service._find_user_doc("not-an-object-id") is None

    updated_stats = default_user_stats_payload()
    updated_stats["elo"] = 1300
    updated_stats["ratings"]["overall"]["elo"] = 1300
    await service._update_user_stats(user_id=str(oid), stats=updated_stats)
    now = datetime(2026, 4, 8, 12, tzinfo=UTC)
    await service._set_bot_join_cooldown(user_id=str(oid), now=now)

    assert users.docs[0]["stats"]["elo"] == 1300
    assert users.docs[0]["bot_profile"]["last_bot_game_joined_at"] == now


@pytest.mark.asyncio
async def test_count_documents_and_game_id_resolution_cover_fallback_paths() -> None:
    class ListOnlyCollection:
        def __init__(self, docs: list[dict]):
            self.docs = docs

    class CursorOnlyCollection:
        def __init__(self, docs: list[dict]):
            self._docs = docs

        def find(self, query: dict):
            matched = []
            for doc in self._docs:
                if doc.get("state") == query.get("state"):
                    matched.append(doc)
            return FakeCursor(matched)

    now = datetime(2026, 4, 9, tzinfo=UTC)
    service = GameService(FakeGamesCollection())
    list_only = ListOnlyCollection(
        [
            {"_id": 1, "state": "completed", "updated_at": now},
            {"_id": 2, "state": "completed", "updated_at": now - timedelta(days=2)},
            {"_id": 3, "state": "active", "updated_at": now},
        ]
    )
    cursor_only = CursorOnlyCollection(
        [
            {"_id": 4, "state": "completed"},
            {"_id": 5, "state": "completed"},
            {"_id": 6, "state": "waiting"},
        ]
    )

    assert await service._count_documents(None, {"state": "completed"}) == 0
    assert await service._count_documents(list_only, {"state": "completed", "updated_at": {"$gte": now - timedelta(hours=1)}}) == 1
    assert await service._count_documents(cursor_only, {"state": "completed"}) == 2

    games = FakeGamesCollection()
    archives = FakeGamesCollection()
    local_id = ObjectId()
    archive_id = ObjectId()
    games.docs.append({"_id": local_id, "game_code": "LOCAL1"})
    archives.docs.append({"_id": archive_id, "game_code": "ARCH1"})
    resolving_service = GameService(games, archives_collection=archives)

    assert await resolving_service._resolve_game_object_id("local1") == local_id
    assert await resolving_service._resolve_game_object_id("arch1") == archive_id
    with pytest.raises(GameNotFoundError):
        await resolving_service._resolve_game_object_id("   ")


def test_result_scoresheet_and_bot_variant_helpers_cover_uncommon_branches() -> None:
    assert GameService._final_result_from_special("CHECKMATE_BLACK_WINS") == {"winner": "black", "reason": "checkmate"}
    assert GameService._final_result_from_special("DRAW_STALEMATE") == {"winner": None, "reason": "stalemate"}
    assert GameService._final_result_from_special("DRAW_TOOMANYREVERSIBLEMOVES") == {
        "winner": None,
        "reason": "too_many_reversible_moves",
    }
    assert GameService._final_result_from_special("UNKNOWN") is None
    assert GameService._normalized_result(
        result={"winner": "white", "reason": "timeout"},
        moves=[{"special_announcement": "DRAW_STALEMATE"}],
    ) == {"winner": "white", "reason": "timeout"}
    assert GameService._normalized_result(
        result={"winner": "black"},
        moves=[
            {"special_announcement": None},
            {"special_announcement": "UNKNOWN"},
            {"special_announcement": "DRAW_STALEMATE"},
        ],
    ) == {"winner": "black", "reason": "stalemate"}

    engine = create_new_game(any_rule=True)
    engine_scoresheets = GameService._stored_scoresheets({"moves": []}, engine)
    empty_scoresheets = GameService._stored_scoresheets({"moves": []})
    assert engine_scoresheets["white"]["color"] == "white"
    assert set(empty_scoresheets) == {"white", "black"}

    bootstrap = GameService(FakeGamesCollection())._load_or_bootstrap_engine({"state": "waiting", "rule_variant": "berkeley_any"})
    assert bootstrap is not None

    class PawnStub:
        must_use_pawns = True

    GameService._repair_forced_pawn_capture_state(game={"state": "active", "moves": []}, engine=PawnStub())
    GameService._repair_forced_pawn_capture_state(
        game={"state": "active", "moves": [{"question_type": "COMMON", "announcement": "REGULAR_MOVE", "move_done": True}]},
        engine=PawnStub(),
    )
    GameService._repair_forced_pawn_capture_state(
        game={"state": "active", "moves": [{"question_type": "ASK_ANY", "announcement": "HAS_ANY", "move_done": False}]},
        engine=PawnStub(),
    )

    assert GameService._bot_supported_rule_variants({"username": "randobotany", "bot_profile": {}}) == ["berkeley_any"]
    assert GameService._bot_supported_rule_variants({"username": "randobot", "bot_profile": {}}) == [
        "berkeley",
        "berkeley_any",
        "cincinnati",
        "wild16",
        "rand",
        "english",
        "crazykrieg",
    ]


@pytest.mark.asyncio
async def test_waiting_game_and_bot_join_helpers_cover_list_and_missing_user_branches() -> None:
    class ListOnlyGames:
        def __init__(self, docs: list[dict]):
            self.docs = docs

        async def find_one(self, query: dict):
            for doc in self.docs:
                if doc.get("state") == query.get("state") and doc.get("white", {}).get("user_id") == query.get("white.user_id"):
                    return doc
            return None

    now = datetime(2026, 4, 9, 15, tzinfo=UTC)
    gid = ObjectId()
    games = ListOnlyGames(
        [
            {
                "_id": gid,
                "state": "waiting",
                "expires_at": now - timedelta(seconds=1),
                "white": {"user_id": "creator", "username": "creatorbot", "role": "bot"},
            }
        ]
    )
    service = GameService(games, users_collection=FakeUsersCollection())
    service.utcnow = lambda: now  # type: ignore[method-assign]

    assert await service._find_waiting_game_for_creator(user_id="creator") is None
    assert games.docs == []

    with pytest.raises(GameForbiddenError) as exc:
        await service._enforce_bot_join_rules(
            user_id="missing-bot",
            game={"white": {"user_id": "creator", "role": "bot"}},
            now=now,
        )
    assert exc.value.code == "FORBIDDEN"


@pytest.mark.asyncio
async def test_load_bot_validation_errors_cover_unavailable_invalid_and_missing_paths() -> None:
    with pytest.raises(GameValidationError) as unavailable_exc:
        await GameService(FakeGamesCollection(), users_collection=None)._load_bot(str(ObjectId()))
    assert unavailable_exc.value.code == "BOT_UNAVAILABLE"

    service = GameService(FakeGamesCollection(), users_collection=FakeUsersCollection())
    with pytest.raises(GameValidationError) as invalid_exc:
        await service._load_bot("bad-id")
    assert invalid_exc.value.code == "BOT_NOT_FOUND"

    with pytest.raises(GameValidationError) as missing_exc:
        await service._load_bot(str(ObjectId()))
    assert missing_exc.value.code == "BOT_NOT_FOUND"


@pytest.mark.asyncio
async def test_create_game_with_bot_as_black_places_bot_on_white_side() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection(
        docs=[
            {
                "_id": ObjectId("507f1f77bcf86cd799439011"),
                "username": "randobot",
                "username_display": "Random Bot",
                "role": "bot",
                "status": "active",
                "bot_profile": {"display_name": "Random Bot", "supported_rule_variants": ["berkeley", "berkeley_any"]},
                "stats": {"elo": 1315},
            }
        ]
    )
    service = GameService(games, users_collection=users)

    response = await service.create_game(
        user_id="u1",
        username="creator",
        request=CreateGameRequest(
            opponent_type="bot",
            bot_id="507f1f77bcf86cd799439011",
            play_as="black",
            time_control="rapid",
        ),
    )

    assert response.play_as == "black"
    assert games.docs[0]["white"]["username"] == "randobot"
    assert games.docs[0]["black"]["username"] == "creator"


@pytest.mark.asyncio
async def test_get_game_or_archive_removes_expired_waiting_games_from_cache_and_db() -> None:
    now = datetime(2026, 4, 10, tzinfo=UTC)

    class FrozenGameService(GameService):
        @staticmethod
        def utcnow() -> datetime:
            return now

    games = FakeGamesCollection()
    cached_id = ObjectId()
    uncached_id = ObjectId()
    games.docs.extend(
        [
            {
                "_id": cached_id,
                "game_code": "CACHE1",
                "state": "waiting",
                "white": {"user_id": "u1", "username": "creator"},
                "expires_at": now - timedelta(seconds=1),
            },
            {
                "_id": uncached_id,
                "game_code": "DBONLY1",
                "state": "waiting",
                "white": {"user_id": "u1", "username": "creator"},
                "expires_at": now - timedelta(seconds=1),
            },
        ]
    )
    service = FrozenGameService(games)
    await service._prime_cache(games.docs[0], persisted=True)

    assert await service.get_game_or_archive(game_id=str(cached_id)) is None
    assert await service.get_game_or_archive(game_id=str(uncached_id)) is None
    assert games.docs == []


@pytest.mark.asyncio
async def test_get_recent_completed_games_skips_missing_black_and_handles_absent_archive() -> None:
    no_archive_service = GameService(FakeGamesCollection(), archives_collection=None)
    assert (await no_archive_service.get_recent_completed_games(limit=5)).games == []

    archives = FakeGamesCollection()
    now = datetime(2026, 4, 10, tzinfo=UTC)
    archives.docs.extend(
        [
            {
                "_id": ObjectId(),
                "game_code": "MSSBK2",
                "state": "completed",
                "white": {"username": "white", "connected": True},
                "black": None,
                "updated_at": now,
            },
            {
                "_id": ObjectId(),
                "game_code": "R4DY22",
                "rule_variant": "berkeley_any",
                "state": "completed",
                "white": {"username": "white", "connected": True, "role": "user"},
                "black": {"username": "black", "connected": True, "role": "bot"},
                "result": {"winner": "black", "reason": "checkmate"},
                "updated_at": now - timedelta(minutes=1),
                "created_at": now - timedelta(minutes=2),
            },
        ]
    )
    service = GameService(FakeGamesCollection(), archives_collection=archives)

    recent = await service.get_recent_completed_games(limit=5)

    assert [item.game_code for item in recent.games] == ["R4DY22"]


@pytest.mark.asyncio
async def test_background_flush_helpers_and_sweep_guards_cover_due_entry_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 11, tzinfo=UTC)
    service = GameService(FakeGamesCollection())
    service.utcnow = lambda: now  # type: ignore[method-assign]
    entry = CachedGameEntry(
        game={
            "_id": ObjectId(),
            "state": "active",
            "moves": [],
            "white": {"role": "bot"},
            "black": {"role": "bot"},
        },
        dirty=False,
        last_activity_at=now,
        last_persisted_ply=0,
    )

    assert await service._should_flush_entry(entry) is False
    entry.dirty = True
    entry.game["state"] = "completed"
    assert await service._should_flush_entry(entry) is True
    entry.game["state"] = "active"
    entry.game["white"]["role"] = "user"
    assert await service._should_flush_entry(entry) is True
    entry.game["white"]["role"] = "bot"
    entry.game["moves"] = [{"ply": index + 1} for index in range(BOT_GAME_FLUSH_PLIES)]
    assert await service._should_flush_entry(entry) is True
    entry.game["moves"] = []
    entry.last_activity_at = now - BOT_GAME_IDLE_FLUSH
    assert await service._should_flush_entry(entry) is True

    flushed: list[str] = []
    service._cache[entry.game["_id"]] = entry

    async def always_due(item: CachedGameEntry) -> bool:  # noqa: ARG001
        return True

    monkeypatch.setattr(service, "_should_flush_entry", always_due)
    monkeypatch.setattr(service, "_schedule_flush", lambda item, reason: flushed.append(reason))  # noqa: ARG005
    await service._flush_due_entries()
    assert flushed == ["background"]

    sweep_calls: list[tuple[str, datetime]] = []

    async def expire(*, now: datetime) -> None:
        sweep_calls.append(("expire", now))

    async def sweep(*, now: datetime) -> None:
        sweep_calls.append(("timeout", now))

    monkeypatch.setattr(service, "_expire_waiting_games", expire)
    monkeypatch.setattr(service, "_sweep_timeouts", sweep)
    service._last_waiting_game_sweep_at = None
    service._last_timeout_sweep_at = None
    await service._maybe_expire_waiting_games()
    await service._maybe_expire_waiting_games()
    await service._maybe_sweep_timeouts()
    await service._maybe_sweep_timeouts()

    assert sweep_calls == [("expire", now), ("timeout", now)]


@pytest.mark.asyncio
async def test_flush_loop_and_assert_active_fallbacks_cover_nonstandard_collections(monkeypatch: pytest.MonkeyPatch) -> None:
    service = GameService(FakeGamesCollection())
    loop_calls: list[str] = []

    async def one_tick(_seconds: float) -> None:
        service._shutdown = True

    monkeypatch.setattr("app.services.game_service.asyncio.sleep", one_tick)
    monkeypatch.setattr(service, "_flush_due_entries", lambda: loop_calls.append("flush_due"))
    monkeypatch.setattr(service, "_maybe_expire_waiting_games", lambda: loop_calls.append("expire"))
    monkeypatch.setattr(service, "_maybe_sweep_timeouts", lambda: loop_calls.append("timeouts"))

    async def call_recorder(name: str):
        async def _inner() -> None:
            loop_calls.append(name)
        return _inner

    monkeypatch.setattr(service, "_flush_due_entries", await call_recorder("flush_due"))
    monkeypatch.setattr(service, "_maybe_expire_waiting_games", await call_recorder("expire"))
    monkeypatch.setattr(service, "_maybe_sweep_timeouts", await call_recorder("timeouts"))
    await service._flush_loop()
    assert loop_calls == ["flush_due", "expire", "timeouts"]

    cancel_service = GameService(FakeGamesCollection())

    async def cancel_sleep(_seconds: float) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr("app.services.game_service.asyncio.sleep", cancel_sleep)
    with pytest.raises(asyncio.CancelledError):
        await cancel_service._flush_loop()

    gid = ObjectId()
    list_games = type("ListGames", (), {"docs": [{"_id": gid, "state": "active"}]})()
    list_service = GameService(list_games)
    now = datetime(2026, 4, 11, 12, tzinfo=UTC)
    await list_service._assert_active_game_still_current(game_id=gid, now=now)
    assert list_games.docs[0]["updated_at"] == now

    class PlainGames:
        async def find_one(self, query: dict):  # noqa: ARG002
            return {"_id": gid, "state": "waiting"}

    with pytest.raises(GameValidationError):
        await GameService(PlainGames())._assert_active_game_still_current(game_id=gid, now=now)


@pytest.mark.asyncio
async def test_persist_game_document_and_terminal_entry_cover_fallback_collections() -> None:
    class ListFallbackGames:
        def __init__(self) -> None:
            self.docs: list[dict] = []

    games = ListFallbackGames()
    service = GameService(games)
    gid = ObjectId()
    await service._persist_game_document({"_id": gid, "state": "active", "moves": []})
    assert games.docs[0]["_id"] == gid

    entry = CachedGameEntry(
        game={"_id": gid, "state": "active", "moves": [], "white": {"role": "bot"}, "black": {"role": "bot"}},
        dirty=True,
        version=1,
    )
    persisted = await service._persist_terminal_entry(entry, expected_previous_state="active")
    assert persisted["state"] == "active"

    waiting_id = ObjectId()
    games.docs.append({"_id": waiting_id, "state": "waiting", "moves": []})
    bad_entry = CachedGameEntry(game={"_id": waiting_id, "state": "active", "moves": []}, dirty=True, version=1)
    with pytest.raises(GameValidationError):
        await service._persist_terminal_entry(bad_entry, expected_previous_state="active")


def test_matches_query_comparison_operators_cover_all_supported_ranges() -> None:
    doc = {"score": 5, "nested": {"value": 7}}

    assert GameService._matches_query(doc, {"score": {"$gt": 4, "$lt": 6, "$lte": 5}})
    assert GameService._matches_query(doc, {"nested.value": {"$gte": 7}})
    assert GameService._matches_query(doc, {"nested.value": {"$lt": 8}})
    assert not GameService._matches_query(doc, {"score": {"$gt": 5}})
    assert not GameService._matches_query(doc, {"nested.value": {"$lte": 6}})


@pytest.mark.asyncio
async def test_get_open_games_and_not_found_paths_cover_expired_and_missing_cases(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 12, tzinfo=UTC)

    class FrozenGameService(GameService):
        @staticmethod
        def utcnow() -> datetime:
            return now

    games = FakeGamesCollection()
    games.docs.extend(
        [
            {
                "_id": ObjectId(),
                "game_code": "EXP222",
                "rule_variant": "berkeley_any",
                "creator_color": "white",
                "white": {"user_id": "u1", "username": "expired", "connected": True},
                "black": None,
                "state": "waiting",
                "created_at": now - timedelta(minutes=1),
                "updated_at": now - timedelta(minutes=1),
                "expires_at": now - timedelta(seconds=1),
            },
            {
                "_id": ObjectId(),
                "game_code": "HVR222",
                "rule_variant": "berkeley_any",
                "creator_color": "white",
                "white": {"user_id": "u2", "username": "live", "connected": True},
                "black": None,
                "state": "waiting",
                "created_at": now,
                "updated_at": now,
                "expires_at": now + timedelta(minutes=10),
            },
        ]
    )
    service = FrozenGameService(games)

    open_games = await service.get_open_games(limit=10)
    assert [item.game_code for item in open_games.games] == ["HVR222"]

    with pytest.raises(GameNotFoundError):
        await service.get_game_transcript(game_id=str(ObjectId()), user_id="u1")
    with pytest.raises(GameNotFoundError):
        await service.hydrate_document(game_id=str(ObjectId()))


@pytest.mark.asyncio
async def test_execute_move_and_ask_any_cover_timeout_and_validation_edges(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 12, 12, tzinfo=UTC)

    def timeout_payload(winner: str = "black") -> dict:
        return {
            "winner": winner,
            "clock": {
                "white_remaining": 0.0,
                "black_remaining": 1.0,
                "active_color": None,
            },
        }

    move_games = FakeGamesCollection()
    move_game = active_game_doc(white_role="bot", black_role="bot")
    move_games.docs.append(move_game)
    move_service = GameService(move_games)
    move_service.utcnow = lambda: now  # type: ignore[method-assign]
    move_reasons: list[str] = []
    timeout_checks = iter([None, timeout_payload("black")])
    monkeypatch.setattr(
        "app.services.game_service.attempt_move",
        lambda engine, uci: {  # noqa: ARG005
            "move_done": True,
            "announcement": "REGULAR_MOVE",
            "special_announcement": None,
            "capture_square": None,
            "full_fen": "full",
            "white_fen": "white",
            "black_fen": "black",
            "turn": "black",
            "game_over": False,
        },
    )
    monkeypatch.setattr(move_service._clock, "check_timeout", lambda **kwargs: next(timeout_checks))
    monkeypatch.setattr(move_service, "_schedule_flush", lambda entry, reason: move_reasons.append(reason))  # noqa: ARG005

    move_response = await move_service.execute_move(game_id=str(move_game["_id"]), user_id="u1", uci="e2e4")
    assert move_response["game_over"] is True
    assert move_reasons == ["completion"]

    ask_games = FakeGamesCollection()
    completed = active_game_doc()
    completed["state"] = "completed"
    ask_games.docs.append(completed)
    ask_service = GameService(ask_games)
    ask_service.utcnow = lambda: now  # type: ignore[method-assign]

    with pytest.raises(GameValidationError) as inactive_exc:
        await ask_service.execute_ask_any(game_id=str(completed["_id"]), user_id="u1")
    assert inactive_exc.value.code == "GAME_NOT_ACTIVE"

    waiting_turn = active_game_doc(turn="black")
    ask_games_turn = FakeGamesCollection()
    ask_games_turn.docs.append(waiting_turn)
    turn_service = GameService(ask_games_turn)
    turn_service.utcnow = lambda: now  # type: ignore[method-assign]
    with pytest.raises(GameValidationError) as turn_exc:
        await turn_service.execute_ask_any(game_id=str(waiting_turn["_id"]), user_id="u1")
    assert turn_exc.value.code == "NOT_YOUR_TURN"

    human_games = FakeGamesCollection()
    human_game = active_game_doc()
    human_games.docs.append(human_game)
    human_service = GameService(human_games)
    human_service.utcnow = lambda: now  # type: ignore[method-assign]
    ask_timeout_checks = iter([None, timeout_payload("black")])
    persisted: list[str] = []
    monkeypatch.setattr(
        "app.services.game_service.ask_any",
        lambda engine: {  # noqa: ARG005
            "move_done": False,
            "announcement": "HAS_ANY",
            "special_announcement": None,
            "capture_square": None,
            "full_fen": "full",
            "white_fen": "white",
            "black_fen": "black",
            "turn": "black",
            "game_over": False,
            "has_any": True,
        },
    )
    monkeypatch.setattr(human_service._clock, "check_timeout", lambda **kwargs: next(ask_timeout_checks))

    async def persist_entry(entry: CachedGameEntry, *, expected_previous_state: str = "active"):  # noqa: ARG001
        persisted.append(expected_previous_state)
        return entry.game

    monkeypatch.setattr(human_service, "_persist_terminal_entry", persist_entry)
    ask_response = await human_service.execute_ask_any(game_id=str(human_game["_id"]), user_id="u1")
    assert ask_response["game_over"] is True
    assert persisted == ["active"]


@pytest.mark.asyncio
async def test_small_helper_branches_cover_collection_and_cache_fallbacks() -> None:
    class ReplaceResult:
        def __init__(self, matched_count: int) -> None:
            self.matched_count = matched_count

    class ReplaceOnlyGames:
        def __init__(self, matched_count: int = 1) -> None:
            self.calls: list[tuple[dict, dict, bool]] = []
            self.matched_count = matched_count

        async def replace_one(self, query: dict, document: dict, upsert: bool = False):
            self.calls.append((query, document, upsert))
            return ReplaceResult(self.matched_count)

    class ReplaceOnlyArchives:
        def __init__(self) -> None:
            self.calls: list[tuple[dict, dict, bool]] = []

        async def replace_one(self, query: dict, document: dict, upsert: bool = False):
            self.calls.append((query, document, upsert))
            return ReplaceResult(1)

    class ListOnlyGames:
        def __init__(self, docs: list[dict]) -> None:
            self.docs = docs

    replace_games = ReplaceOnlyGames()
    replace_service = GameService(replace_games)
    gid = ObjectId()
    await replace_service._persist_game_document({"_id": gid, "state": "active", "moves": []})
    assert replace_games.calls[0][0] == {"_id": gid}
    assert replace_games.calls[0][2] is True

    entry = CachedGameEntry(game={"_id": gid, "state": "active", "moves": []}, dirty=True, version=1)
    persisted = await replace_service._persist_terminal_entry(entry, expected_previous_state="active")
    assert persisted["_id"] == gid

    with pytest.raises(GameValidationError):
        await GameService(ReplaceOnlyGames(matched_count=0))._persist_terminal_entry(
            CachedGameEntry(game={"_id": gid, "state": "active", "moves": []}, dirty=True, version=1),
            expected_previous_state="active",
        )

    archive_doc = {"_id": gid, "state": "completed"}
    replace_archives = ReplaceOnlyArchives()
    archive_service = GameService(FakeGamesCollection(), archives_collection=replace_archives)
    await archive_service._upsert_archive(archive_doc)
    assert replace_archives.calls[0][0] == {"_id": gid}
    assert replace_archives.calls[0][2] is True

    archive_list = ListOnlyGames([{"_id": gid, "state": "waiting"}])
    list_archive_service = GameService(FakeGamesCollection(), archives_collection=archive_list)
    await list_archive_service._upsert_archive({"_id": gid, "state": "completed"})
    assert archive_list.docs == [{"_id": gid, "state": "completed"}]

    waiting_id = ObjectId()
    list_games = ListOnlyGames(
        [
            {"_id": waiting_id, "state": "waiting"},
            {"_id": ObjectId(), "state": "active"},
        ]
    )
    list_service = GameService(list_games)
    await list_service._delete_waiting_game_document(game_id=waiting_id)
    await list_service._evict_cached_game(None)
    assert [doc["state"] for doc in list_games.docs] == ["active"]


@pytest.mark.asyncio
async def test_branch_helpers_cover_user_noops_finalize_and_cache_scan_paths() -> None:
    now = datetime(2026, 4, 13, tzinfo=UTC)

    noop_service = GameService(FakeGamesCollection())
    await noop_service._update_user_stats(user_id=None, stats=default_user_stats_payload())
    await noop_service._set_bot_join_cooldown(user_id="u1", now=now)

    users = FakeUsersCollection(docs=[{"_id": "user-1", "stats": default_user_stats_payload(), "bot_profile": {}}])
    invalid_id_service = GameService(FakeGamesCollection(), users_collection=users)
    await invalid_id_service._update_user_stats(user_id="not-an-object-id", stats=default_user_stats_payload())
    await invalid_id_service._set_bot_join_cooldown(user_id="not-an-object-id", now=now)
    assert users.docs[0]["stats"]["games_played"] == 0
    assert users.docs[0]["bot_profile"] == {}

    with pytest.raises(GameConflictError) as own_game_exc:
        await invalid_id_service._enforce_bot_join_rules(
            user_id="creator",
            game={"white": {"user_id": "creator", "role": "bot"}},
            now=now,
        )
    assert own_game_exc.value.code == "CANNOT_JOIN_OWN_GAME"

    finalize_games = FakeGamesCollection()
    finalize_archives = FakeGamesCollection()
    completed = {
        "_id": ObjectId(),
        "game_code": "MISSU1",
        "rule_variant": "berkeley_any",
        "white": {"user_id": "white-only", "username": "white", "role": "user"},
        "black": {"user_id": "missing-black", "username": "black", "role": "user"},
        "state": "completed",
        "result": {"winner": "white", "reason": "checkmate"},
        "moves": [],
        "created_at": now,
        "updated_at": now,
    }
    finalize_games.docs.append(completed)
    finalize_service = GameService(
        finalize_games,
        users_collection=FakeUsersCollection([{"_id": "white-only", "stats": default_user_stats_payload(), "role": "user"}]),
        archives_collection=finalize_archives,
    )
    finalized = await finalize_service._finalize_completed_game(completed)
    assert finalized["stats_recorded_at"] is not None
    assert "rating_snapshot" not in finalized
    assert finalize_archives.docs[0]["_id"] == completed["_id"]

    list_only_games = type(
        "ListOnlyGames",
        (),
        {"docs": [{"_id": ObjectId(), "state": "active"}, {"_id": ObjectId(), "state": "waiting"}]},
    )()
    cache_scan_service = GameService(list_only_games)
    uncached = await cache_scan_service._active_games_not_in_cache()
    assert len(uncached) == 1
    assert uncached[0]["state"] == "active"
    assert await GameService(object())._active_games_not_in_cache() == []


@pytest.mark.asyncio
async def test_runtime_branches_cover_bot_completion_timeout_and_stale_list_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 14, 12, tzinfo=UTC)

    move_games = FakeGamesCollection()
    move_game = active_game_doc(white_role="bot", black_role="bot")
    move_games.docs.append(move_game)
    move_service = GameService(move_games)
    move_service.utcnow = lambda: now  # type: ignore[method-assign]
    move_reasons: list[str] = []
    monkeypatch.setattr(
        "app.services.game_service.attempt_move",
        lambda engine, uci: {  # noqa: ARG005
            "move_done": True,
            "announcement": "CHECKMATE",
            "special_announcement": "CHECKMATE_WHITE_WINS",
            "capture_square": None,
            "full_fen": "full",
            "white_fen": "white",
            "black_fen": "black",
            "turn": None,
            "game_over": True,
        },
    )
    monkeypatch.setattr(move_service, "_schedule_flush", lambda entry, reason: move_reasons.append(reason))  # noqa: ARG005
    move_response = await move_service.execute_move(game_id=str(move_game["_id"]), user_id="u1", uci="e2e4")
    assert move_response["game_over"] is True
    assert move_reasons == ["completion"]

    ask_games = FakeGamesCollection()
    ask_game = active_game_doc(white_role="bot", black_role="bot")
    ask_games.docs.append(ask_game)
    ask_service = GameService(ask_games)
    ask_service.utcnow = lambda: now  # type: ignore[method-assign]
    timeout_reasons: list[str] = []
    monkeypatch.setattr(
        ask_service._clock,
        "check_timeout",
        lambda **kwargs: {  # noqa: ARG005
            "winner": "black",
            "clock": {"white_remaining": 0.0, "black_remaining": 1.0, "active_color": None},
        },
    )
    monkeypatch.setattr(ask_service, "_schedule_flush", lambda entry, reason: timeout_reasons.append(reason))  # noqa: ARG005
    with pytest.raises(GameValidationError) as timeout_exc:
        await ask_service.execute_ask_any(game_id=str(ask_game["_id"]), user_id="u1")
    assert timeout_exc.value.code == "GAME_NOT_ACTIVE"
    assert timeout_reasons == ["timeout"]

    bot_ask_games = FakeGamesCollection()
    bot_ask_game = active_game_doc(white_role="bot", black_role="bot")
    bot_ask_games.docs.append(bot_ask_game)
    bot_ask_service = GameService(bot_ask_games)
    bot_ask_service.utcnow = lambda: now  # type: ignore[method-assign]
    completion_reasons: list[str] = []
    monkeypatch.setattr(
        "app.services.game_service.ask_any",
        lambda engine: {  # noqa: ARG005
            "move_done": False,
            "announcement": "HAS_ANY",
            "special_announcement": None,
            "capture_square": None,
            "full_fen": "full",
            "white_fen": "white",
            "black_fen": "black",
            "turn": "black",
            "game_over": True,
            "has_any": True,
        },
    )
    timeout_checks = iter([None, None])
    monkeypatch.setattr(bot_ask_service._clock, "check_timeout", lambda **kwargs: next(timeout_checks))
    monkeypatch.setattr(bot_ask_service, "_schedule_flush", lambda entry, reason: completion_reasons.append(reason))  # noqa: ARG005
    ask_response = await bot_ask_service.execute_ask_any(game_id=str(bot_ask_game["_id"]), user_id="u1")
    assert ask_response["game_over"] is True
    assert completion_reasons == ["completion"]

    stale_list_games = type("ListGames", (), {"docs": [{"_id": ObjectId(), "state": "waiting"}]})()
    with pytest.raises(GameValidationError):
        await GameService(stale_list_games)._assert_active_game_still_current(game_id=ObjectId(), now=now)


@pytest.mark.asyncio
async def test_misc_branch_helpers_cover_timeout_passthrough_and_replay_edge_cases(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 15, tzinfo=UTC)
    game = {"_id": ObjectId(), "state": "active", "turn": "white"}
    service = GameService(FakeGamesCollection())
    monkeypatch.setattr(service._clock, "check_timeout", lambda **kwargs: None)
    assert await service._adjudicate_timeout_if_needed(game=game, now=now) is game

    replay = GameService._build_replay_fens(
        [
            {"question_type": "COMMON", "move_done": True, "uci": "not-uci"},
            {"question_type": "ASK_ANY", "move_done": False, "uci": None},
        ]
    )
    assert len(replay) == 2

    assert not GameService._matches_query({"white": None}, {"white.user_id": "u1"})
