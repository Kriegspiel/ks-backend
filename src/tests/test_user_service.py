from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import pytest
from bson import ObjectId

from app.models.auth import BotRegisterRequest, RegisterRequest
from app.services.user_service import UserConflictError, UserService


@dataclass
class InsertResult:
    inserted_id: ObjectId


class FakeCursor:
    def __init__(self, docs: list[dict]):
        self._docs = list(docs)

    def sort(self, fields, direction: int | None = None):
        if isinstance(fields, str):
            specs = [(fields, direction if direction is not None else 1)]
        else:
            specs = fields

        for key, order in reversed(specs):
            self._docs.sort(key=lambda item: self._resolve(item, key), reverse=order < 0)
        return self

    def skip(self, count: int):
        self._docs = self._docs[count:]
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

    @staticmethod
    def _resolve(doc: dict, key: str):
        value = doc
        for part in key.split("."):
            if not isinstance(value, dict):
                return None
            value = value.get(part)
        return value


class FakeUsersCollection:
    def __init__(self) -> None:
        self.docs: list[dict] = []

    async def find_one(self, query: dict):
        for doc in self.docs:
            if self._matches(doc, query):
                return dict(doc)
        return None

    async def insert_one(self, payload: dict):
        doc = dict(payload)
        doc["_id"] = ObjectId()
        self.docs.append(doc)
        return InsertResult(inserted_id=doc["_id"])

    async def find_one_and_update(self, query: dict, update: dict, return_document=None):
        for idx, doc in enumerate(self.docs):
            if self._matches(doc, query):
                for key, value in update.get("$set", {}).items():
                    self._set_nested(doc, key, value)
                for key in update.get("$unset", {}):
                    self._unset_nested(doc, key)
                self.docs[idx] = doc
                return dict(doc)
        return None

    async def count_documents(self, query: dict):
        return len([d for d in self.docs if self._matches(d, query)])

    def find(self, query: dict):
        return FakeCursor([d for d in self.docs if self._matches(d, query)])

    def _matches(self, doc: dict, query: dict) -> bool:
        for key, expected in query.items():
            if key == "$or":
                if any(self._matches(doc, cond) for cond in expected):
                    continue
                return False
            value = self._resolve(doc, key)
            if isinstance(expected, dict) and "$gte" in expected:
                if value is None or value < expected["$gte"]:
                    return False
            elif isinstance(expected, dict) and "$ne" in expected:
                if value == expected["$ne"]:
                    return False
            elif value != expected:
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

    @staticmethod
    def _set_nested(doc: dict, key: str, value):
        parts = key.split(".")
        cur = doc
        for part in parts[:-1]:
            cur = cur.setdefault(part, {})
        cur[parts[-1]] = value

    @staticmethod
    def _unset_nested(doc: dict, key: str):
        parts = key.split(".")
        cur = doc
        for part in parts[:-1]:
            cur = cur.get(part)
            if not isinstance(cur, dict):
                return
        if isinstance(cur, dict):
            cur.pop(parts[-1], None)


class FakeDB:
    def __init__(self, users: FakeUsersCollection, game_archives: FakeUsersCollection):
        self.users = users
        self.game_archives = game_archives


@pytest.mark.asyncio
async def test_create_user_stores_canonical_username_display_and_hashed_password() -> None:
    users = FakeUsersCollection()
    service = UserService(users)

    created = await service.create_user(RegisterRequest(username="PlayerOne", email="Player@One.Example", password="abc12345"))

    stored = users.docs[0]
    assert stored["username"] == "playerone"
    assert stored["username_display"] == "PlayerOne"
    assert stored["email"] == "player@one.example"
    assert stored["email_verified"] is False
    assert stored["password_hash"] != "abc12345"
    assert service.verify_password("abc12345", stored["password_hash"])
    assert not service.verify_password("wrong-pass", stored["password_hash"])
    assert created.username == "playerone"


@pytest.mark.asyncio
async def test_create_user_rejects_duplicate_username() -> None:
    users = FakeUsersCollection()
    service = UserService(users)

    await service.create_user(RegisterRequest(username="PlayerOne", email="one@example.com", password="abc12345"))

    with pytest.raises(UserConflictError) as exc:
        await service.create_user(RegisterRequest(username="playerone", email="two@example.com", password="abc12345"))

    assert exc.value.code == "USERNAME_TAKEN"
    assert exc.value.field == "username"


@pytest.mark.asyncio
async def test_create_user_rejects_duplicate_email() -> None:
    users = FakeUsersCollection()
    service = UserService(users)

    await service.create_user(RegisterRequest(username="PlayerOne", email="one@example.com", password="abc12345"))

    with pytest.raises(UserConflictError) as exc:
        await service.create_user(RegisterRequest(username="PlayerTwo", email="One@Example.com", password="abc12345"))

    assert exc.value.code == "EMAIL_TAKEN"
    assert exc.value.field == "email"


@pytest.mark.asyncio
async def test_authenticate_returns_user_for_valid_credentials_else_none() -> None:
    users = FakeUsersCollection()
    service = UserService(users)

    created = await service.create_user(RegisterRequest(username="PlayerOne", email="one@example.com", password="abc12345"))

    valid = await service.authenticate("PLAYERONE", "abc12345")
    invalid_password = await service.authenticate("playerone", "badpass123")
    missing_user = await service.authenticate("missing", "abc12345")

    assert valid is not None
    assert valid.id == created.id
    assert invalid_password is None
    assert missing_user is None


@pytest.mark.asyncio
async def test_create_bot_stores_hmac_digest_and_authenticates_without_bcrypt_hash() -> None:
    users = FakeUsersCollection()
    service = UserService(users)
    UserService.clear_bot_token_cache()

    bot, token = await service.create_bot(
        BotRegisterRequest(
            username="digestbot",
            display_name="Digest Bot",
            owner_email="digestbot@example.com",
            description="Digest-backed bot auth",
            supported_rule_variants=["berkeley", "berkeley_any"],
        )
    )

    stored = users.docs[0]
    assert stored["bot_profile"]["api_token_hash"] is None
    assert stored["bot_profile"]["api_token_digest"]

    authenticated = await service.authenticate_bot_token(token)

    assert authenticated is not None
    assert authenticated.id == bot.id


@pytest.mark.asyncio
async def test_authenticate_bot_token_migrates_legacy_bcrypt_hash_to_digest() -> None:
    users = FakeUsersCollection()
    service = UserService(users)
    UserService.clear_bot_token_cache()
    token_id = "abc123"
    token_secret = "legacy-secret"
    users.docs.append(
        {
            "_id": ObjectId(),
            "username": "legacybot",
            "username_display": "Legacy Bot",
            "email": "legacybot@bots.kriegspiel.local",
            "email_verified": True,
            "email_verification_sent_at": None,
            "email_verified_at": datetime(2026, 4, 3, tzinfo=UTC),
            "password_hash": service.hash_password("irrelevant123"),
            "auth_providers": ["bot_token"],
            "profile": {"bio": "", "avatar_url": None, "country": None},
            "bot_profile": {
                "display_name": "Legacy Bot",
                "owner_email": "legacy@example.com",
                "description": "",
                "listed": True,
                "api_token_id": token_id,
                "api_token_hash": service.hash_password(token_secret),
                "registered_at": datetime(2026, 4, 3, tzinfo=UTC),
                "supported_rule_variants": ["berkeley", "berkeley_any"],
            },
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
                "sound_enabled": False,
                "auto_ask_any": False,
            },
            "role": "bot",
            "status": "active",
            "last_active_at": datetime(2026, 4, 3, tzinfo=UTC),
            "created_at": datetime(2026, 4, 3, tzinfo=UTC),
            "updated_at": datetime(2026, 4, 3, tzinfo=UTC),
        }
    )

    authenticated = await service.authenticate_bot_token(f"ksbot_{token_id}.{token_secret}")

    assert authenticated is not None
    assert users.docs[0]["bot_profile"].get("api_token_hash") is None
    assert users.docs[0]["bot_profile"].get("api_token_digest") == service.bot_token_digest(token_secret)


@pytest.mark.asyncio
async def test_get_public_profile_and_missing_user() -> None:
    users = FakeUsersCollection()
    user_id = ObjectId()
    users.docs.append(
        {
            "_id": user_id,
            "username": "playerone",
            "profile": {"bio": "Kriegspiel enthusiast", "avatar_url": None, "country": "US"},
            "stats": {
                "games_played": 7,
                "games_won": 4,
                "games_lost": 2,
                "games_drawn": 1,
                "elo": 1337,
                "elo_peak": 1337,
            },
            "created_at": datetime(2025, 1, 15, tzinfo=UTC),
        }
    )
    db = FakeDB(users=users, game_archives=FakeUsersCollection())
    service = UserService(users)

    profile = await service.get_public_profile(db, "PlayerOne")
    missing = await service.get_public_profile(db, "missing")

    assert profile is not None
    assert profile["username"] == "playerone"
    assert profile["stats"]["elo"] == 1337
    assert missing is None


@pytest.mark.asyncio
async def test_get_game_history_paginates_newest_first_and_out_of_range_empty() -> None:
    users = FakeUsersCollection()
    archives = FakeUsersCollection()
    user_id = ObjectId()
    other_id = ObjectId()
    archives.docs.extend(
        [
            {
                "_id": ObjectId(),
                "white": {"user_id": str(user_id), "username": "playerone"},
                "black": {"user_id": str(other_id), "username": "rival-a"},
                "result": {"winner": "white", "reason": "checkmate"},
                "rating_snapshot": {"white_before": 1200, "white_after": 1216, "white_delta": 16, "black_before": 1200, "black_after": 1184, "black_delta": -16},
                "moves": [1, 2, 3],
                "created_at": datetime(2026, 3, 10, tzinfo=UTC),
                "updated_at": datetime(2026, 3, 10, tzinfo=UTC),
            },
            {
                "_id": ObjectId(),
                "white": {"user_id": str(other_id), "username": "rival-b"},
                "black": {"user_id": str(user_id), "username": "playerone"},
                "result": {"winner": None, "reason": "stalemate"},
                "moves": [1, 2],
                "created_at": datetime(2026, 3, 9, tzinfo=UTC),
                "updated_at": datetime(2026, 3, 9, tzinfo=UTC),
            },
        ]
    )
    db = FakeDB(users=users, game_archives=archives)
    service = UserService(users)

    page_1, total = await service.get_game_history(db, str(user_id), page=1, per_page=1)
    out_of_range, total_2 = await service.get_game_history(db, str(user_id), page=4, per_page=1)

    assert total == 2
    assert total_2 == 2
    assert page_1[0]["opponent"] == "rival-a"
    assert page_1[0]["elo_before"] == 1200
    assert page_1[0]["elo_after"] == 1216
    assert page_1[0]["elo_delta"] == 16
    assert out_of_range == []


@pytest.mark.asyncio
async def test_update_settings_persists_and_returns_payload() -> None:
    users = FakeUsersCollection()
    user_id = ObjectId()
    users.docs.append(
        {
            "_id": user_id,
            "username": "playerone",
            "settings": {
                "board_theme": "default",
                "piece_set": "cburnett",
                "sound_enabled": True,
                "auto_ask_any": False,
            },
        }
    )
    db = FakeDB(users=users, game_archives=FakeUsersCollection())
    service = UserService(users)

    updated = await service.update_settings(db, str(user_id), {"board_theme": "dark", "sound_enabled": False})

    assert updated["board_theme"] == "dark"
    assert updated["sound_enabled"] is False


@pytest.mark.asyncio
async def test_get_leaderboard_filters_ranks_and_tiebreaks_by_username() -> None:
    users = FakeUsersCollection()
    users.docs.extend(
        [
            {
                "_id": ObjectId(),
                "username": "zeta",
                "status": "active",
                "stats": {"elo": 1500, "games_played": 12, "games_won": 7},
            },
            {
                "_id": ObjectId(),
                "username": "alpha",
                "status": "active",
                "stats": {"elo": 1500, "games_played": 6, "games_won": 4},
            },
            {
                "_id": ObjectId(),
                "username": "inactive",
                "status": "disabled",
                "stats": {"elo": 1700, "games_played": 60, "games_won": 40},
            },
            {
                "_id": ObjectId(),
                "username": "newbie",
                "status": "active",
                "stats": {"elo": 2000, "games_played": 2, "games_won": 2},
            },
        ]
    )
    db = FakeDB(users=users, game_archives=FakeUsersCollection())
    service = UserService(users)

    players, total = await service.get_leaderboard(db, page=1, per_page=20)

    assert total == 2
    assert [p["username"] for p in players] == ["alpha", "zeta"]
    assert players[0]["rank"] == 1
    assert players[1]["rank"] == 2
