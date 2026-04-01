from __future__ import annotations

from datetime import UTC, datetime

import pytest
from bson import ObjectId
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.models.game import CreateGameRequest
from app.services.bot_service import BotService
from app.services.game_service import GameConflictError, GameService
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
            },
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
    assert authenticated.bot_profile.owner_email is None


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
