from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from bson import ObjectId
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.models.game import CreateGameRequest
from app.services.bot_service import BotService
from app.services.game_service import GameConflictError, GameForbiddenError, GameService, GameValidationError
from app.services.user_service import UserService
from tests.test_game_service import FakeGamesCollection


class FakeUsersCollection:
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
        return type("InsertResult", (), {"inserted_id": doc["_id"]})

    async def find_one_and_update(self, query: dict, update: dict, return_document=None):  # noqa: ANN001
        for doc in self.docs:
            if self._matches(doc, query):
                for key, value in update.get("$set", {}).items():
                    current = doc
                    parts = key.split(".")
                    for part in parts[:-1]:
                        current = current.setdefault(part, {})
                    current[parts[-1]] = value
                return doc
        return None

    def find(self, query: dict):
        rows = [doc for doc in self.docs if self._matches(doc, query)]

        class Cursor:
            def __init__(self, items):
                self.items = items

            def sort(self, field, direction):
                self.items.sort(key=lambda row: row[field], reverse=direction < 0)
                return self

            def __aiter__(self):
                self.index = 0
                return self

            async def __anext__(self):
                if self.index >= len(self.items):
                    raise StopAsyncIteration
                item = self.items[self.index]
                self.index += 1
                return item

        return Cursor(rows)

    @staticmethod
    def _matches(doc: dict, query: dict) -> bool:
        for key, expected in query.items():
            current = doc
            for part in key.split("."):
                if not isinstance(current, dict):
                    return False
                current = current.get(part)
            if current != expected:
                return False
        return True


@pytest.mark.asyncio
async def test_create_game_with_bot_immediately_activates() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    bot_id = ObjectId()
    users.docs.append(
        {
            "_id": bot_id,
            "username": "randobot",
            "username_display": "Random Bot",
            "role": "bot",
            "status": "active",
            "bot_profile": {
                "display_name": "Random Bot",
                "owner_email": "owner@example.com",
                "description": "Plays random moves",
                "supported_rule_variants": ["berkeley", "berkeley_any"],
            },
            "stats": {"elo": 1315},
        }
    )
    service = GameService(games, users_collection=users, site_origin="https://kriegspiel.org")

    response = await service.create_game(
        user_id="u1",
        username="creator",
        request=CreateGameRequest(opponent_type="bot", bot_id=str(bot_id), play_as="white", time_control="rapid"),
    )

    assert response.state == "active"
    assert response.bot == {"bot_id": str(bot_id), "username": "randobot"}
    assert games.docs[0]["black"]["role"] == "bot"
    assert games.docs[0]["state"] == "active"


@pytest.mark.asyncio
async def test_create_game_with_bot_rejects_unsupported_ruleset() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    bot_id = ObjectId()
    users.docs.append(
        {
            "_id": bot_id,
            "username": "randobotany",
            "username_display": "Random Any Bot",
            "role": "bot",
            "status": "active",
            "bot_profile": {
                "display_name": "Random Any Bot",
                "owner_email": "owner@example.com",
                "description": "Asks any first",
                "supported_rule_variants": ["berkeley_any"],
            },
        }
    )
    service = GameService(games, users_collection=users, site_origin="https://kriegspiel.org")

    with pytest.raises(GameValidationError) as exc:
        await service.create_game(
            user_id="u1",
            username="creator",
            request=CreateGameRequest(rule_variant="berkeley", opponent_type="bot", bot_id=str(bot_id), play_as="white", time_control="rapid"),
        )

    assert exc.value.code == "BOT_RULE_VARIANT_UNSUPPORTED"


@pytest.mark.asyncio
async def test_bot_can_create_one_open_lobby_game_only() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    bot_id = ObjectId()
    users.docs.append(
        {
            "_id": bot_id,
            "username": "randobot",
            "role": "bot",
            "status": "active",
            "bot_profile": {"display_name": "Random Bot", "description": "Plays random moves"},
        }
    )
    service = GameService(games, users_collection=users, site_origin="https://kriegspiel.org")

    response = await service.create_game(
        user_id=str(bot_id),
        username="randobot",
        request=CreateGameRequest(opponent_type="human", play_as="white", time_control="rapid"),
        role="bot",
    )

    assert response.state == "waiting"
    assert games.docs[0]["white"]["role"] == "bot"
    assert games.docs[0]["expires_at"] - games.docs[0]["created_at"] == timedelta(minutes=10)

    with pytest.raises(GameConflictError) as exc:
        await service.create_game(
            user_id=str(bot_id),
            username="randobot",
            request=CreateGameRequest(opponent_type="human", play_as="black", time_control="rapid"),
            role="bot",
        )

    assert exc.value.code == "BOT_ALREADY_HAS_OPEN_GAME"


@pytest.mark.asyncio
async def test_bot_cannot_create_selected_bot_game() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    creator_id = ObjectId()
    opponent_id = ObjectId()
    users.docs.extend(
        [
            {
                "_id": creator_id,
                "username": "botcreator",
                "role": "bot",
                "status": "active",
                "bot_profile": {"display_name": "Bot Creator", "description": "Creates games"},
            },
            {
                "_id": opponent_id,
                "username": "otherbot",
                "role": "bot",
                "status": "active",
                "bot_profile": {"display_name": "Other Bot", "description": "Other bot"},
            },
        ]
    )
    service = GameService(games, users_collection=users)

    with pytest.raises(GameValidationError) as exc:
        await service.create_game(
            user_id=str(creator_id),
            username="botcreator",
            request=CreateGameRequest(opponent_type="bot", bot_id=str(opponent_id), time_control="rapid"),
            role="bot",
        )

    assert exc.value.code == "BOT_CREATE_REQUIRES_HUMAN_OPPONENT"


@pytest.mark.asyncio
async def test_join_rejects_bot_reserved_game() -> None:
    games = FakeGamesCollection()
    now = datetime.now(UTC)
    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "A7K2M9",
            "rule_variant": "berkeley_any",
            "creator_color": "white",
            "opponent_type": "bot",
            "selected_bot_id": "507f1f77bcf86cd799439012",
            "white": {"user_id": "u1", "username": "creator", "connected": True, "role": "user"},
            "black": None,
            "state": "waiting",
            "turn": None,
            "move_number": 1,
            "created_at": now,
            "updated_at": now,
        }
    )
    service = GameService(games)

    with pytest.raises(GameConflictError) as exc:
        await service.join_game(user_id="u2", username="joiner", game_code="A7K2M9")

    assert exc.value.code == "GAME_RESERVED_FOR_BOT"


@pytest.mark.asyncio
async def test_bot_cannot_join_human_waiting_game() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    bot_id = ObjectId()
    users.docs.append(
        {
            "_id": bot_id,
            "username": "randobot",
            "role": "bot",
            "status": "active",
            "bot_profile": {"display_name": "Random Bot", "description": "Bot"},
        }
    )
    now = datetime.now(UTC)
    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "H7K2M9",
            "rule_variant": "berkeley_any",
            "creator_color": "white",
            "opponent_type": "human",
            "white": {"user_id": "u1", "username": "creator", "connected": True, "role": "user"},
            "black": None,
            "state": "waiting",
            "turn": None,
            "move_number": 1,
            "created_at": now,
            "updated_at": now,
        }
    )
    service = GameService(games, users_collection=users)

    with pytest.raises(GameForbiddenError) as exc:
        await service.join_game(user_id=str(bot_id), username="randobot", game_code="H7K2M9", role="bot")

    assert exc.value.code == "FORBIDDEN"


@pytest.mark.asyncio
async def test_bot_can_join_another_bot_game_once_per_minute_but_not_its_own() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    creator_id = ObjectId()
    joiner_id = ObjectId()
    users.docs.extend(
        [
            {
                "_id": creator_id,
                "username": "creatorbot",
                "role": "bot",
                "status": "active",
                "bot_profile": {"display_name": "Creator Bot", "description": "Creates"},
            },
            {
                "_id": joiner_id,
                "username": "joinerbot",
                "role": "bot",
                "status": "active",
                "bot_profile": {"display_name": "Joiner Bot", "description": "Joins"},
            },
        ]
    )
    now = datetime.now(UTC)
    games.docs.extend(
        [
            {
                "_id": ObjectId(),
                "game_code": "J7K2M9",
                "rule_variant": "berkeley_any",
                "creator_color": "white",
                "opponent_type": "human",
                "white": {"user_id": str(creator_id), "username": "creatorbot", "connected": True, "role": "bot"},
                "black": None,
                "state": "waiting",
                "turn": None,
                "move_number": 1,
                "created_at": now,
                "updated_at": now,
            },
            {
                "_id": ObjectId(),
                "game_code": "K7K2M9",
                "rule_variant": "berkeley_any",
                "creator_color": "white",
                "opponent_type": "human",
                "white": {"user_id": str(joiner_id), "username": "joinerbot", "connected": True, "role": "bot"},
                "black": None,
                "state": "waiting",
                "turn": None,
                "move_number": 1,
                "created_at": now,
                "updated_at": now,
            },
        ]
    )
    service = GameService(games, users_collection=users)
    service.utcnow = lambda: now  # type: ignore[method-assign]

    joined = await service.join_game(user_id=str(joiner_id), username="joinerbot", game_code="J7K2M9", role="bot")
    assert joined.state == "active"
    assert users.docs[1]["bot_profile"]["last_bot_game_joined_at"] == now

    with pytest.raises(GameConflictError) as own_exc:
        await service.join_game(user_id=str(joiner_id), username="joinerbot", game_code="K7K2M9", role="bot")
    assert own_exc.value.code == "CANNOT_JOIN_OWN_GAME"

    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "L7K2M9",
            "rule_variant": "berkeley_any",
            "creator_color": "white",
            "opponent_type": "human",
            "white": {"user_id": str(creator_id), "username": "creatorbot", "connected": True, "role": "bot"},
            "black": None,
            "state": "waiting",
            "turn": None,
            "move_number": 1,
            "created_at": now,
            "updated_at": now,
        }
    )

    with pytest.raises(GameConflictError) as cooldown_exc:
        await service.join_game(user_id=str(joiner_id), username="joinerbot", game_code="L7K2M9", role="bot")
    assert cooldown_exc.value.code == "BOT_JOIN_COOLDOWN"

    service.utcnow = lambda: now + timedelta(minutes=1, seconds=1)  # type: ignore[method-assign]
    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "M7K2M9",
            "rule_variant": "berkeley_any",
            "creator_color": "white",
            "opponent_type": "human",
            "white": {"user_id": str(creator_id), "username": "creatorbot", "connected": True, "role": "bot"},
            "black": None,
            "state": "waiting",
            "turn": None,
            "move_number": 1,
            "created_at": now,
            "updated_at": now,
        }
    )
    joined_again = await service.join_game(user_id=str(joiner_id), username="joinerbot", game_code="M7K2M9", role="bot")
    assert joined_again.state == "active"


@pytest.mark.asyncio
async def test_bot_join_cooldown_accepts_naive_stored_datetime() -> None:
    games = FakeGamesCollection()
    users = FakeUsersCollection()
    creator_id = ObjectId()
    joiner_id = ObjectId()
    now = datetime.now(UTC)
    users.docs.extend(
        [
            {
                "_id": creator_id,
                "username": "creatorbot",
                "role": "bot",
                "status": "active",
                "bot_profile": {"display_name": "Creator Bot", "description": "Creates"},
            },
            {
                "_id": joiner_id,
                "username": "joinerbot",
                "role": "bot",
                "status": "active",
                "bot_profile": {
                    "display_name": "Joiner Bot",
                    "description": "Joins",
                    "last_bot_game_joined_at": now.replace(tzinfo=None),
                },
            },
        ]
    )
    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "N7K2M9",
            "rule_variant": "berkeley_any",
            "creator_color": "white",
            "opponent_type": "human",
            "white": {"user_id": str(creator_id), "username": "creatorbot", "connected": True, "role": "bot"},
            "black": None,
            "state": "waiting",
            "turn": None,
            "move_number": 1,
            "created_at": now,
            "updated_at": now,
        }
    )
    service = GameService(games, users_collection=users)
    service.utcnow = lambda: now  # type: ignore[method-assign]

    with pytest.raises(GameConflictError) as exc:
        await service.join_game(user_id=str(joiner_id), username="joinerbot", game_code="N7K2M9", role="bot")

    assert exc.value.code == "BOT_JOIN_COOLDOWN"


@pytest.mark.asyncio
async def test_user_service_can_issue_and_authenticate_bot_token() -> None:
    users = FakeUsersCollection()
    service = UserService(users)
    user, token = await service.create_bot(
        type(
            "Payload",
            (),
            {
                "username": "randobot",
                "display_name": "Random Bot",
                "owner_email": "Owner@Example.com",
                "description": "Plays random moves",
            },
        )()
    )

    assert user.role == "bot"
    assert user.bot_profile is not None
    assert user.bot_profile.owner_email == "owner@example.com"
    assert user.bot_profile.supported_rule_variants == [
        "berkeley",
        "berkeley_any",
        "cincinnati",
        "wild16",
        "rand",
        "english",
        "crazykrieg",
    ]
    authenticated = await service.authenticate_bot_token(token)
    assert authenticated is not None
    assert authenticated.username == "randobot"


@pytest.mark.asyncio
async def test_bot_service_lists_active_bots() -> None:
    users = FakeUsersCollection()
    users.docs.extend(
        [
            {
                "_id": ObjectId(),
                "username": "randobot",
                "username_display": "Random Bot",
                "role": "bot",
                "status": "active",
                "bot_profile": {
                    "display_name": "Random Bot",
                    "owner_email": "owner@example.com",
                    "description": "Plays random moves",
                    "listed": True,
                    "supported_rule_variants": ["berkeley", "berkeley_any"],
                },
            },
            {
                "_id": ObjectId(),
                "username": "sleepybot",
                "username_display": "Sleepy Bot",
                "role": "bot",
                "status": "inactive",
                "bot_profile": {
                    "display_name": "Sleepy Bot",
                    "owner_email": "owner@example.com",
                    "description": "Offline",
                    "listed": True,
                },
            },
        ]
    )
    service = BotService(users)

    listed = await service.list_bots()
    assert [bot.username for bot in listed.bots] == ["randobot"]
    assert listed.bots[0].elo == 1200
    assert listed.bots[0].supported_rule_variants == ["berkeley", "berkeley_any"]


@pytest.mark.asyncio
async def test_bot_service_hides_unlisted_bots() -> None:
    users = FakeUsersCollection()
    users.docs.extend(
        [
            {
                "_id": ObjectId(),
                "username": "randobot",
                "username_display": "Random Bot",
                "role": "bot",
                "status": "active",
                "bot_profile": {
                    "display_name": "Random Bot",
                    "owner_email": "owner@example.com",
                    "description": "Plays random moves",
                    "listed": True,
                },
            },
            {
                "_id": ObjectId(),
                "username": "randobot_e2eabcd",
                "username_display": "Random Bot e2eabcd",
                "role": "bot",
                "status": "active",
                "bot_profile": {
                    "display_name": "Random Bot e2eabcd",
                    "owner_email": "owner@example.com",
                    "description": "E2E random bot",
                    "listed": False,
                },
            },
        ]
    )
    service = BotService(users)

    listed = await service.list_bots()
    assert [bot.username for bot in listed.bots] == ["randobot"]


@pytest.mark.asyncio
async def test_bot_registration_marks_e2e_bots_unlisted() -> None:
    users = FakeUsersCollection()
    service = UserService(users)

    user, _token = await service.create_bot(
        type(
            "Payload",
            (),
            {
                "username": "randobot_e2eabcd",
                "display_name": "Random Bot e2eabcd",
                "owner_email": "owner@example.com",
                "description": "E2E random bot",
                "listed": None,
            },
        )()
    )

    assert user.bot_profile is not None
    assert user.bot_profile.listed is False


def test_bot_registration_route_returns_api_token() -> None:
    app = create_app(Settings(ENVIRONMENT="testing", BOT_REGISTRATION_KEY="secret-key"))
    users = FakeUsersCollection()

    from app.routers import auth as auth_router_module

    auth_router_module.require_db = lambda: type("Db", (), {"users": users})()

    with TestClient(app) as client:
        registered = client.post(
            "/api/auth/bots/register",
            json={
                "username": "randobot",
                "display_name": "Random Bot",
                "owner_email": "owner@example.com",
                "description": "Plays random moves",
            },
            headers={"X-Bot-Registration-Key": "secret-key"},
        )

    assert registered.status_code == 201
    assert registered.json()["api_token"].startswith("ksbot_")
    assert registered.json()["owner_email"] == "owner@example.com"


def test_bot_registration_route_requires_owner_email() -> None:
    app = create_app(Settings(ENVIRONMENT="testing", BOT_REGISTRATION_KEY="secret-key"))
    users = FakeUsersCollection()

    from app.routers import auth as auth_router_module

    auth_router_module.require_db = lambda: type("Db", (), {"users": users})()

    with TestClient(app) as client:
        registered = client.post(
            "/api/auth/bots/register",
            json={"username": "randobot", "display_name": "Random Bot", "description": "Plays random moves"},
            headers={"X-Bot-Registration-Key": "secret-key"},
        )

    assert registered.status_code == 422


@pytest.mark.asyncio
async def test_user_service_authenticates_legacy_bot_without_owner_email() -> None:
    users = FakeUsersCollection()
    service = UserService(users)
    _user, token = await service.create_bot(
        type(
            "Payload",
            (),
            {
                "username": "legacybot",
                "display_name": "Legacy Bot",
                "owner_email": "owner@example.com",
                "description": "Legacy registration",
            },
        )()
    )
    users.docs[0]["bot_profile"].pop("owner_email", None)

    authenticated = await service.authenticate_bot_token(token)

    assert authenticated is not None
    assert authenticated.username == "legacybot"
    assert authenticated.bot_profile is not None
    assert authenticated.bot_profile.owner_email == "bots@kriegspiel.org"


@pytest.mark.asyncio
async def test_probe_bots_default_to_unlisted() -> None:
    users = FakeUsersCollection()
    service = UserService(users)

    user, _token = await service.create_bot(
        type(
            "Payload",
            (),
            {
                "username": "probebot",
                "display_name": "Probe Bot",
                "owner_email": "owner@example.com",
                "description": "Probe runner",
                "listed": None,
            },
        )()
    )

    assert user.bot_profile is not None
    assert user.bot_profile.listed is False


@pytest.mark.asyncio
async def test_random_any_defaults_to_berkeley_any_only() -> None:
    users = FakeUsersCollection()
    service = UserService(users)

    user, _token = await service.create_bot(
        type(
            "Payload",
            (),
            {
                "username": "randobotany",
                "display_name": "Random Any Bot",
                "owner_email": "owner@example.com",
                "description": "Asks any first",
                "listed": True,
                "supported_rule_variants": None,
            },
        )()
    )

    assert user.bot_profile is not None
    assert user.bot_profile.supported_rule_variants == ["berkeley_any"]


@pytest.mark.asyncio
async def test_bot_service_get_bot_by_id_rejects_invalid_and_filters_inactive() -> None:
    users = FakeUsersCollection()
    active_id = ObjectId()
    inactive_id = ObjectId()
    users.docs.extend(
        [
            {
                "_id": active_id,
                "username": "randobot",
                "username_display": "Random Bot",
                "role": "bot",
                "status": "active",
                "bot_profile": {"display_name": "Random Bot", "description": "ready"},
            },
            {
                "_id": inactive_id,
                "username": "sleepybot",
                "username_display": "Sleepy Bot",
                "role": "bot",
                "status": "inactive",
                "bot_profile": {"display_name": "Sleepy Bot", "description": "offline"},
            },
        ]
    )
    service = BotService(users)

    assert await service.get_bot_by_id("not-an-object-id") is None
    assert await service.get_bot_by_id(str(inactive_id)) is None
    assert (await service.get_bot_by_id(str(active_id)))["username"] == "randobot"


def test_bot_service_supported_rule_variants_fallbacks_cover_randobotany() -> None:
    assert BotService._supported_rule_variants({"username": "randobotany", "bot_profile": {}}) == ["berkeley_any"]
    assert BotService._supported_rule_variants({"username": "randobot", "bot_profile": {}}) == [
        "berkeley",
        "berkeley_any",
        "cincinnati",
        "wild16",
        "rand",
        "english",
        "crazykrieg",
    ]


def test_bot_router_uses_db_users_and_lists_bots() -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    users = FakeUsersCollection()
    users.docs.append(
        {
            "_id": ObjectId(),
            "username": "randobot",
            "username_display": "Random Bot",
            "role": "bot",
            "status": "active",
            "bot_profile": {
                "display_name": "Random Bot",
                "description": "ready",
                "listed": True,
                "supported_rule_variants": ["berkeley", "berkeley_any"],
            },
        }
    )

    from app.routers import bot as bot_router_module

    bot_router_module.get_db = lambda: type("Db", (), {"users": users})()
    service = bot_router_module.get_bot_service()
    assert service._users is users  # noqa: SLF001

    app.dependency_overrides[bot_router_module.get_current_user] = lambda: type("User", (), {"id": "u1"})()
    with TestClient(app) as client:
        response = client.get("/api/bots")

    assert response.status_code == 200
    assert response.json()["bots"][0]["username"] == "randobot"
