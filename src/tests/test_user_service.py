from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import bcrypt
import pytest
from bson import ObjectId
from pymongo.errors import DuplicateKeyError

from app.models.auth import BotRegisterRequest, ConvertGuestRequest, RegisterRequest
from app.models.user import UserModel, default_user_stats_payload
from app.services import user_service as user_service_module
from app.services.guest_names import GUEST_FIRST_NAMES, GUEST_LAST_NAMES
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
        self.find_calls: list[tuple[dict, dict | None]] = []

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

    async def update_many(self, query: dict, update: dict):
        matched_count = 0
        for idx, doc in enumerate(self.docs):
            if self._matches(doc, query):
                matched_count += 1
                for key, value in update.get("$set", {}).items():
                    self._set_nested(doc, key, value)
                for key in update.get("$unset", {}):
                    self._unset_nested(doc, key)
                self.docs[idx] = doc
        return type("UpdateResult", (), {"matched_count": matched_count, "modified_count": matched_count})()

    async def count_documents(self, query: dict):
        return len([d for d in self.docs if self._matches(d, query)])

    def find(self, query: dict, projection: dict | None = None):
        self.find_calls.append((query, projection))
        matches = [d for d in self.docs if self._matches(d, query)]
        if projection:
            matches = [self._project(doc, projection) for doc in matches]
        return FakeCursor(matches)

    def _matches(self, doc: dict, query: dict) -> bool:
        for key, expected in query.items():
            if key == "$or":
                if any(self._matches(doc, cond) for cond in expected):
                    continue
                return False
            value = self._resolve(doc, key)
            if isinstance(expected, dict):
                if "$gte" in expected and (value is None or value < expected["$gte"]):
                    return False
                if "$gt" in expected and (value is None or value <= expected["$gt"]):
                    return False
                if "$lt" in expected and (value is None or value >= expected["$lt"]):
                    return False
                if "$lte" in expected and (value is None or value > expected["$lte"]):
                    return False
                if "$in" in expected and value not in expected["$in"]:
                    return False
                if "$nin" in expected and value in expected["$nin"]:
                    return False
                if "$ne" in expected and value == expected["$ne"]:
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

    @classmethod
    def _project(cls, doc: dict, projection: dict):
        result = {}
        for key, include in projection.items():
            if not include:
                continue
            value = cls._resolve(doc, key)
            if value is not None:
                cls._set_nested(result, key, value)
        return result


class FakeAggregateCollection(FakeUsersCollection):
    def __init__(self, aggregate_result: dict) -> None:
        super().__init__()
        self.aggregate_calls: list[list[dict]] = []
        self.aggregate_result = aggregate_result

    def aggregate(self, pipeline: list[dict]):
        self.aggregate_calls.append(pipeline)
        return FakeCursor([self.aggregate_result])


class FakeDB:
    def __init__(
        self,
        users: FakeUsersCollection,
        game_archives: FakeUsersCollection,
        games: FakeUsersCollection | None = None,
    ):
        self.users = users
        self.game_archives = game_archives
        self.games = games


def test_find_uses_single_argument_call_when_projection_is_omitted() -> None:
    class TrackingCollection:
        def __init__(self) -> None:
            self.calls: list[tuple] = []

        def find(self, *args):
            self.calls.append(args)
            return "cursor"

    collection = TrackingCollection()

    assert UserService._find(collection, {"role": "bot"}) == "cursor"
    assert collection.calls == [({"role": "bot"},)]


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


def test_guest_name_pools_have_expected_size_and_safe_shape() -> None:
    assert len(GUEST_FIRST_NAMES) == 222
    assert len(GUEST_LAST_NAMES) == 232
    assert UserService.guest_name_pool_size() == 51_504
    assert len(set(GUEST_FIRST_NAMES)) == 222
    assert len(set(GUEST_LAST_NAMES)) == 232
    assert "magnus" in GUEST_FIRST_NAMES
    assert "paolo" in GUEST_FIRST_NAMES
    assert "pien" in GUEST_FIRST_NAMES
    assert "ciancarini" in GUEST_LAST_NAMES
    assert "tencate" in GUEST_LAST_NAMES
    assert "loustau" in GUEST_LAST_NAMES
    assert all(len(name) > 1 for name in GUEST_FIRST_NAMES)
    assert all(name.isascii() and name.isalnum() and name == name.lower() for name in GUEST_FIRST_NAMES)
    assert all(name.isascii() and name.isalnum() and name == name.lower() for name in GUEST_LAST_NAMES)
    assert all(len(UserService._guest_username_for_index(index)) <= 33 for index in range(UserService.guest_name_pool_size()))


@pytest.mark.asyncio
async def test_create_guest_user_stores_session_safe_guest_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(user_service_module.secrets, "randbelow", lambda _upper: 0)
    users = FakeUsersCollection()
    service = UserService(users)

    guest = await service.create_guest_user()

    stored = users.docs[0]
    assert guest.username == "guest_adolf_adams"
    assert stored["username"] == "guest_adolf_adams"
    assert stored["username_display"] == "guest_adolf_adams"
    assert stored["email"] == "guest_adolf_adams@guests.kriegspiel.local"
    assert stored["email_verified"] is True
    assert stored["auth_providers"] == ["guest"]
    assert stored["role"] == "guest"
    assert stored["status"] == "active"
    assert stored["password_hash"] != ""


@pytest.mark.asyncio
async def test_create_guest_user_skips_existing_guest_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(user_service_module.secrets, "randbelow", lambda _upper: 0)
    users = FakeUsersCollection()
    service = UserService(users)

    first = await service.create_guest_user()
    second = await service.create_guest_user()

    assert first.username == "guest_adolf_adams"
    assert second.username == "guest_akiba_adams"
    assert len({doc["username"] for doc in users.docs}) == 2


@pytest.mark.asyncio
async def test_create_guest_user_raises_when_small_name_pool_is_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    users = FakeUsersCollection()
    users.docs.append({"_id": ObjectId(), "username": "guest_taken"})
    monkeypatch.setattr(UserService, "guest_name_pool_size", classmethod(lambda cls: 1))
    monkeypatch.setattr(UserService, "_guest_username_for_index", classmethod(lambda cls, index: "guest_taken"))
    monkeypatch.setattr(user_service_module.secrets, "randbelow", lambda _upper: 0)

    with pytest.raises(UserConflictError) as exc:
        await UserService(users).create_guest_user()

    assert exc.value.code == "GUEST_NAME_POOL_EXHAUSTED"


@pytest.mark.asyncio
async def test_convert_guest_to_user_claims_account_and_updates_player_refs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(user_service_module.secrets, "randbelow", lambda _upper: 0)
    users = FakeUsersCollection()
    games = FakeUsersCollection()
    game_archives = FakeUsersCollection()
    service = UserService(users)
    guest = await service.create_guest_user()
    user_id = guest.id
    games.docs.append(
        {
            "white": {"user_id": user_id, "username": guest.username, "role": "guest"},
            "black": {"user_id": "other", "username": "other", "role": "user"},
        }
    )
    game_archives.docs.append(
        {
            "white": {"user_id": "other", "username": "other", "role": "user"},
            "black": {"user_id": user_id, "username": guest.username, "role": "guest"},
        }
    )

    converted = await service.convert_guest_to_user(
        FakeDB(users=users, games=games, game_archives=game_archives),
        guest,
        ConvertGuestRequest(email="Player@One.Example", password="abc12345"),
    )

    stored = users.docs[0]
    assert converted.username == "adolf_adams"
    assert stored["username"] == "adolf_adams"
    assert stored["username_display"] == "adolf_adams"
    assert stored["email"] == "player@one.example"
    assert stored["email_verified"] is False
    assert stored["auth_providers"] == ["local"]
    assert stored["role"] == "user"
    assert stored["profile"]["bio"] == ""
    assert service.verify_password("abc12345", stored["password_hash"])
    assert games.docs[0]["white"]["username"] == "adolf_adams"
    assert games.docs[0]["white"]["role"] == "user"
    assert game_archives.docs[0]["black"]["username"] == "adolf_adams"
    assert game_archives.docs[0]["black"]["role"] == "user"


@pytest.mark.asyncio
async def test_convert_guest_to_user_rejects_duplicate_regular_username(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(user_service_module.secrets, "randbelow", lambda _upper: 0)
    users = FakeUsersCollection()
    service = UserService(users)
    guest = await service.create_guest_user()
    await service.create_user(RegisterRequest(username="adolf_adams", email="existing@example.com", password="abc12345"))

    with pytest.raises(UserConflictError) as exc:
        await service.convert_guest_to_user(
            FakeDB(users=users, games=FakeUsersCollection(), game_archives=FakeUsersCollection()),
            guest,
            ConvertGuestRequest(email="new@example.com", password="abc12345"),
        )

    assert exc.value.code == "USERNAME_TAKEN"


@pytest.mark.asyncio
async def test_convert_guest_to_user_rejects_duplicate_email(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(user_service_module.secrets, "randbelow", lambda _upper: 0)
    users = FakeUsersCollection()
    service = UserService(users)
    guest = await service.create_guest_user()
    await service.create_user(RegisterRequest(username="playerone", email="taken@example.com", password="abc12345"))

    with pytest.raises(UserConflictError) as exc:
        await service.convert_guest_to_user(
            FakeDB(users=users, games=FakeUsersCollection(), game_archives=FakeUsersCollection()),
            guest,
            ConvertGuestRequest(email="Taken@Example.com", password="abc12345"),
        )

    assert exc.value.code == "EMAIL_TAKEN"


@pytest.mark.asyncio
async def test_create_user_and_guest_user_attach_acquisition_and_guest_retries_duplicate_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    users = FakeUsersCollection()
    service = UserService(users)

    created = await service.create_user(
        RegisterRequest(username="Acquired", email="acquired@example.com", password="abc12345"),
        acquisition={"utm": {"source": "reddit"}},
    )

    assert created.username == "acquired"
    assert users.docs[0]["acquisition"]["utm"] == {"source": "reddit"}
    assert isinstance(users.docs[0]["acquisition"]["acquired_at"], datetime)

    class DuplicateOnceGuests(FakeUsersCollection):
        def __init__(self) -> None:
            super().__init__()
            self.insert_attempts = 0

        async def insert_one(self, payload: dict):
            self.insert_attempts += 1
            if self.insert_attempts == 1:
                raise DuplicateKeyError("guest username race")
            return await super().insert_one(payload)

    monkeypatch.setattr(user_service_module.secrets, "randbelow", lambda _upper: 0)
    guest_users = DuplicateOnceGuests()
    guest = await UserService(guest_users).create_guest_user(acquisition={"landing_path": "/play"})

    assert guest.username == UserService._guest_username_for_index(1)
    assert guest_users.insert_attempts == 2
    assert guest_users.docs[0]["acquisition"]["landing_path"] == "/play"


def _guest_user_model(
    *,
    username: str = "guest_mikhail_tal",
    role: str = "guest",
    user_id: ObjectId | None = None,
) -> UserModel:
    now = datetime(2026, 5, 9, tzinfo=UTC)
    return UserModel.from_mongo(
        {
            "_id": user_id or ObjectId(),
            "username": username,
            "username_display": username,
            "email": f"{username}@guests.kriegspiel.local",
            "email_verified": True,
            "password_hash": "hash",
            "auth_providers": ["guest"],
            "profile": {"bio": "Guest player", "avatar_url": None, "country": None},
            "bot_profile": None,
            "stats": default_user_stats_payload(),
            "settings": {"board_theme": "default", "piece_set": "cburnett", "sound_enabled": True, "auto_ask_any": False},
            "role": role,
            "status": "active",
            "last_active_at": now,
            "created_at": now,
            "updated_at": now,
        }
    )


@pytest.mark.asyncio
async def test_convert_guest_to_user_rejects_non_guest_invalid_guest_duplicate_key_and_missing_update() -> None:
    payload = ConvertGuestRequest(email="converted@example.com", password="abc12345")

    with pytest.raises(ValueError, match="Only guest accounts can be converted"):
        await UserService(FakeUsersCollection()).convert_guest_to_user(
            FakeDB(users=FakeUsersCollection(), games=FakeUsersCollection(), game_archives=FakeUsersCollection()),
            _guest_user_model(role="user"),
            payload,
        )

    with pytest.raises(ValueError, match="Guest username cannot be converted"):
        await UserService(FakeUsersCollection()).convert_guest_to_user(
            FakeDB(users=FakeUsersCollection(), games=FakeUsersCollection(), game_archives=FakeUsersCollection()),
            _guest_user_model(username="guest_"),
            payload,
        )

    class DuplicateConvertUsers(FakeUsersCollection):
        def __init__(self, message: str) -> None:
            super().__init__()
            self.message = message

        async def find_one_and_update(self, query: dict, update: dict, return_document=None):  # noqa: ARG002
            raise DuplicateKeyError(self.message)

    with pytest.raises(UserConflictError) as username_exc:
        duplicate_username_users = DuplicateConvertUsers("duplicate username index")
        await UserService(duplicate_username_users).convert_guest_to_user(
            FakeDB(users=duplicate_username_users, games=FakeUsersCollection(), game_archives=FakeUsersCollection()),
            _guest_user_model(),
            payload,
        )
    assert username_exc.value.field == "username"

    with pytest.raises(UserConflictError) as email_exc:
        duplicate_email_users = DuplicateConvertUsers("duplicate email index")
        await UserService(duplicate_email_users).convert_guest_to_user(
            FakeDB(users=duplicate_email_users, games=FakeUsersCollection(), game_archives=FakeUsersCollection()),
            _guest_user_model(),
            payload,
        )
    assert email_exc.value.field == "email"

    missing_users = FakeUsersCollection()
    with pytest.raises(ValueError, match="Only guest accounts can be converted"):
        await UserService(missing_users).convert_guest_to_user(
            FakeDB(users=missing_users, games=FakeUsersCollection(), game_archives=FakeUsersCollection()),
            _guest_user_model(),
            payload,
        )

    games = FakeUsersCollection()
    await UserService(FakeUsersCollection())._update_guest_player_references(
        type("PartialDB", (), {"games": games, "game_archives": None})(),
        user_id="guest-id",
        username="converted",
    )


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
async def test_authenticate_rehashes_legacy_bcrypt_password_hash() -> None:
    users = FakeUsersCollection()
    legacy_hash = bcrypt.hashpw("abc12345".encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    users.docs.append(
        {
            "_id": ObjectId(),
            "username": "playerone",
            "username_display": "PlayerOne",
            "email": "one@example.com",
            "password_hash": legacy_hash,
            "auth_providers": ["local"],
            "profile": {"bio": "", "avatar_url": None, "country": None},
            "bot_profile": None,
            "stats": default_user_stats_payload(),
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
    service = UserService(users)

    authenticated = await service.authenticate("PLAYERONE", "abc12345")

    assert authenticated is not None
    assert users.docs[0]["password_hash"] != legacy_hash
    assert UserService.needs_password_rehash(users.docs[0]["password_hash"]) is False
    assert service.verify_password("abc12345", users.docs[0]["password_hash"]) is True


@pytest.mark.asyncio
async def test_authenticate_tolerates_failed_rehash_update() -> None:
    class NoUpdateUsers(FakeUsersCollection):
        async def find_one_and_update(self, query: dict, update: dict, return_document=None):  # noqa: ARG002
            return None

    users = NoUpdateUsers()
    legacy_hash = bcrypt.hashpw("abc12345".encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    users.docs.append(
        {
            "_id": ObjectId(),
            "username": "playerone",
            "username_display": "PlayerOne",
            "email": "one@example.com",
            "password_hash": legacy_hash,
            "auth_providers": ["local"],
            "profile": {"bio": "", "avatar_url": None, "country": None},
            "bot_profile": None,
            "stats": default_user_stats_payload(),
            "settings": {"board_theme": "default", "piece_set": "cburnett", "sound_enabled": True, "auto_ask_any": False},
            "role": "user",
            "status": "active",
            "last_active_at": datetime.now(UTC),
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        }
    )

    authenticated = await UserService(users).authenticate("PLAYERONE", "abc12345")

    assert authenticated is not None
    assert users.docs[0]["password_hash"] == legacy_hash


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
async def test_authenticate_bot_token_rejects_legacy_bcrypt_hash_only_bot() -> None:
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
                "listed": False,
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

    assert authenticated is None


def test_bot_token_cache_ttl_uses_one_hour_default() -> None:
    assert UserService._bot_token_cache_ttl_seconds == 3600.0


@pytest.mark.asyncio
async def test_get_public_profile_and_missing_user() -> None:
    users = FakeUsersCollection()
    user_id = ObjectId()
    users.docs.append(
        {
            "_id": user_id,
            "username": "playerone",
            "llm_bot_tier": "tier2",
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
    users.docs.append(
        {
            "_id": ObjectId(),
            "username": "randobotany",
            "role": "bot",
            "bot_profile": {
                "display_name": "Random Any Bot",
                "owner_email": "bot-random-any@kriegspiel.org",
            },
            "profile": {"bio": "Bot", "avatar_url": None, "country": None},
            "stats": default_user_stats_payload(),
            "created_at": datetime(2025, 1, 16, tzinfo=UTC),
        }
    )
    db = FakeDB(users=users, game_archives=FakeUsersCollection())
    service = UserService(users)

    profile = await service.get_public_profile(db, "PlayerOne")
    bot_profile = await service.get_public_profile(db, "randobotany")
    missing = await service.get_public_profile(db, "missing")

    assert profile is not None
    assert profile["username"] == "playerone"
    assert profile["llm_bot_tier"] == "tier2"
    assert profile["stats"]["elo"] == 1337
    assert profile["stats"]["ratings"]["overall"]["elo"] == 1337
    assert profile["user_metrics"]["completed_games"] == 0
    assert "bot_metrics" not in profile
    assert bot_profile is not None
    assert bot_profile["llm_bot_tier"] is None
    assert bot_profile["owner_email"] == "bot-random-any@kriegspiel.org"
    assert bot_profile["user_metrics"]["completed_games"] == 0
    assert bot_profile["bot_metrics"]["completed_games"] == 0
    assert missing is None


@pytest.mark.asyncio
async def test_get_public_profile_includes_user_metrics_for_regular_users() -> None:
    users = FakeUsersCollection()
    archives = FakeUsersCollection()
    user_id = ObjectId()
    users.docs.append(
        {
            "_id": user_id,
            "username": "fil",
            "username_display": "fil",
            "email": "fil@example.com",
            "email_verified": True,
            "password_hash": "hash",
            "auth_providers": ["local"],
            "profile": {"bio": "", "avatar_url": None, "country": None},
            "stats": default_user_stats_payload(),
            "settings": {},
            "role": "user",
            "status": "active",
            "created_at": datetime(2026, 5, 18, tzinfo=UTC),
            "updated_at": datetime(2026, 5, 18, tzinfo=UTC),
        }
    )
    archives.docs.extend(
        [
            {
                "_id": ObjectId(),
                "game_code": "USR001",
                "white": {"user_id": str(user_id), "username": "fil", "role": "user"},
                "black": {"user_id": "bot-1", "username": "randobot", "role": "bot"},
                "rule_variant": "berkeley",
                "turn_count": 5,
                "result": {"winner": "black"},
                "created_at": datetime(2026, 5, 18, 10, 0, tzinfo=UTC),
                "updated_at": datetime(2026, 5, 18, 10, 5, tzinfo=UTC),
            },
            {
                "_id": ObjectId(),
                "game_code": "USR002",
                "white": {"user_id": "human-1", "username": "amy", "role": "user"},
                "black": {"user_id": str(user_id), "username": "fil", "role": "user"},
                "rule_variant": "english",
                "move_count": 7,
                "result": {"winner": "black"},
                "created_at": datetime(2026, 5, 18, 11, 0, tzinfo=UTC),
                "updated_at": datetime(2026, 5, 18, 11, 10, tzinfo=UTC),
            },
        ]
    )
    db = FakeDB(users=users, game_archives=archives)
    service = UserService(users)

    profile = await service.get_public_profile(db, "fil")

    assert profile is not None
    assert "bot_metrics" not in profile
    metrics = profile["user_metrics"]
    assert metrics["completed_games"] == 2
    assert metrics["average_turn_count"] == 6.0
    assert metrics["vs_bots"] == {"total_games": 1, "wins": 0, "losses": 1, "draws": 0, "win_rate": 0.0}
    assert metrics["vs_humans"] == {"total_games": 1, "wins": 1, "losses": 0, "draws": 0, "win_rate": 1.0}
    assert metrics["as_white"] == {"total_games": 1, "wins": 0, "losses": 1, "draws": 0, "win_rate": 0.0}
    assert metrics["as_black"] == {"total_games": 1, "wins": 1, "losses": 0, "draws": 0, "win_rate": 1.0}
    assert metrics["opponents"][0]["username"] == "amy"
    assert metrics["rulesets"][0]["rule_variant"] == "berkeley"


@pytest.mark.asyncio
async def test_get_public_bot_profile_includes_generic_profile_metrics() -> None:
    users = FakeUsersCollection()
    archives = FakeUsersCollection()
    bot_id = ObjectId()
    users.docs.append(
        {
            "_id": bot_id,
            "username": "darkboardmcts",
            "username_display": "darkboardmcts",
            "email": "bot-darkboard-mcts@kriegspiel.org",
            "email_verified": True,
            "password_hash": "hash",
            "auth_providers": ["local"],
            "profile": {"bio": "", "avatar_url": None, "country": None},
            "bot_profile": {"owner_email": "bot-darkboard-mcts@kriegspiel.org"},
            "stats": default_user_stats_payload(),
            "settings": {},
            "role": "bot",
            "status": "active",
            "last_active_at": datetime(2026, 5, 18, tzinfo=UTC),
            "created_at": datetime(2026, 5, 18, tzinfo=UTC),
            "updated_at": datetime(2026, 5, 18, tzinfo=UTC),
        }
    )
    archives.docs.extend(
        [
            {
                "_id": ObjectId(),
                "game_code": "BOT001",
                "white": {"user_id": str(bot_id), "username": "darkboardmcts", "role": "bot"},
                "black": {"user_id": "bot-1", "username": "randobot", "role": "bot"},
                "rule_variant": "wild16",
                "turn_count": 12,
                "result": {"winner": "white"},
                "created_at": datetime(2026, 5, 18, 10, 0, tzinfo=UTC),
                "updated_at": datetime(2026, 5, 18, 10, 5, tzinfo=UTC),
            },
            {
                "_id": ObjectId(),
                "game_code": "BOT002",
                "white": {"user_id": "human-1", "username": "fil", "role": "user"},
                "black": {"user_id": str(bot_id), "username": "darkboardmcts", "role": "bot"},
                "rule_variant": "berkeley_any",
                "move_count": 8,
                "result": {"winner": "white"},
                "created_at": datetime(2026, 5, 18, 11, 0, tzinfo=UTC),
                "updated_at": datetime(2026, 5, 18, 11, 10, tzinfo=UTC),
            },
            {
                "_id": ObjectId(),
                "game_code": "BOT003",
                "white": {"user_id": str(bot_id), "username": "darkboardmcts", "role": "bot"},
                "black": {"user_id": "bot-1", "username": "randobot", "role": "bot"},
                "rule_variant": "wild16",
                "turn_count": 10,
                "result": {"winner": None},
                "created_at": datetime(2026, 5, 18, 12, 0, tzinfo=UTC),
                "updated_at": datetime(2026, 5, 18, 12, 15, tzinfo=UTC),
            },
            {
                "_id": ObjectId(),
                "game_code": "BOT004",
                "state": "active",
                "white": {"user_id": str(bot_id), "username": "darkboardmcts", "role": "bot"},
                "black": {"user_id": "human-2", "username": "alex", "role": "user"},
                "rule_variant": "wild16",
                "turn_count": 6,
                "result": {"winner": "white"},
                "created_at": datetime(2026, 5, 18, 13, 0, tzinfo=UTC),
                "updated_at": datetime(2026, 5, 18, 13, 5, tzinfo=UTC),
            },
        ]
    )
    db = FakeDB(users=users, game_archives=archives)
    service = UserService(users)

    profile = await service.get_public_profile(db, "darkboardmcts")

    assert profile is not None
    metrics = profile["user_metrics"]
    assert profile["bot_metrics"] == metrics
    assert metrics["completed_games"] == 3
    assert metrics["average_duration_seconds"] == 600
    assert metrics["average_turn_count"] == 10.0
    assert metrics["last_completed_at"].isoformat() == "2026-05-18T12:15:00+00:00"
    assert metrics["overall"] == {"total_games": 3, "wins": 1, "losses": 1, "draws": 1, "win_rate": 0.3333}
    assert metrics["vs_bots"] == {"total_games": 2, "wins": 1, "losses": 0, "draws": 1, "win_rate": 0.5}
    assert metrics["vs_humans"] == {"total_games": 1, "wins": 0, "losses": 1, "draws": 0, "win_rate": 0.0}
    assert metrics["as_white"] == {"total_games": 2, "wins": 1, "losses": 0, "draws": 1, "win_rate": 0.5}
    assert metrics["as_black"] == {"total_games": 1, "wins": 0, "losses": 1, "draws": 0, "win_rate": 0.0}
    assert metrics["opponents"][0] == {
        "username": "randobot",
        "role": "bot",
        "total_games": 2,
        "wins": 1,
        "losses": 0,
        "draws": 1,
        "win_rate": 0.5,
    }
    assert metrics["rulesets"][0] == {
        "rule_variant": "wild16",
        "total_games": 2,
        "wins": 1,
        "losses": 0,
        "draws": 1,
        "win_rate": 0.5,
    }
    assert archives.find_calls[0][0] == {"$or": [{"white.user_id": str(bot_id)}, {"black.user_id": str(bot_id)}]}
    assert "moves" not in archives.find_calls[0][1]


@pytest.mark.asyncio
async def test_get_public_profile_backfills_track_results() -> None:
    users = FakeUsersCollection()
    archives = FakeUsersCollection()
    user_id = ObjectId()
    users.docs.append(
        {
            "_id": user_id,
            "username": "fil",
            "username_display": "fil",
            "email": "fil@example.com",
            "email_verified": True,
            "password_hash": "hash",
            "auth_providers": ["local"],
            "profile": {"bio": "", "avatar_url": None, "country": None},
            "bot_profile": None,
            "stats": default_user_stats_payload(),
            "settings": {},
            "role": "user",
            "status": "active",
            "last_active_at": datetime(2026, 4, 6, tzinfo=UTC),
            "created_at": datetime(2026, 4, 6, tzinfo=UTC),
            "updated_at": datetime(2026, 4, 6, tzinfo=UTC),
        }
    )
    archives.docs.extend(
        [
            {
                "_id": ObjectId(),
                "white": {"user_id": str(user_id), "role": "user"},
                "black": {"user_id": "bot-1", "role": "bot"},
                "result": {"winner": "white"},
                "created_at": datetime(2026, 4, 6, 1, tzinfo=UTC),
            },
            {
                "_id": ObjectId(),
                "white": {"user_id": "user-2", "role": "user"},
                "black": {"user_id": str(user_id), "role": "user"},
                "result": {"winner": "white"},
                "created_at": datetime(2026, 4, 6, 2, tzinfo=UTC),
            },
        ]
    )
    db = FakeDB(users=users, game_archives=archives)
    service = UserService(users)

    profile = await service.get_public_profile(db, "fil")

    assert profile is not None
    assert profile["stats"]["results"]["overall"]["games_played"] == 2
    assert profile["stats"]["results"]["vs_bots"]["games_won"] == 1
    assert profile["stats"]["results"]["vs_humans"]["games_lost"] == 1


@pytest.mark.asyncio
async def test_get_public_profile_keeps_consistent_unsynced_result_totals() -> None:
    users = FakeUsersCollection()
    archives = FakeUsersCollection()
    user_id = ObjectId()
    users.docs.append(
        {
            "_id": user_id,
            "username": "darkboardmcts",
            "username_display": "Darkboard MCTS",
            "email": "bot@example.com",
            "email_verified": True,
            "password_hash": "hash",
            "auth_providers": ["local"],
            "profile": {"bio": "", "avatar_url": None, "country": None},
            "bot_profile": {"owner_email": "bot@example.com"},
            "stats": {
                **default_user_stats_payload(),
                "games_played": 2,
                "games_lost": 2,
                "results": {
                    "overall": {"games_played": 2, "games_won": 0, "games_lost": 2, "games_drawn": 0},
                    "vs_humans": {"games_played": 1, "games_won": 0, "games_lost": 1, "games_drawn": 0},
                    "vs_bots": {"games_played": 1, "games_won": 0, "games_lost": 1, "games_drawn": 0},
                },
            },
            "settings": {},
            "role": "bot",
            "status": "active",
            "last_active_at": datetime(2026, 5, 18, tzinfo=UTC),
            "created_at": datetime(2026, 5, 18, tzinfo=UTC),
            "updated_at": datetime(2026, 5, 18, tzinfo=UTC),
        }
    )
    db = FakeDB(users=users, game_archives=archives)
    service = UserService(users)

    profile = await service.get_public_profile(db, "darkboardmcts")

    assert profile is not None
    assert profile["stats"]["games_played"] == 2
    assert profile["stats"]["results"]["vs_bots"]["games_played"] == 1
    assert len(archives.find_calls) == 1


@pytest.mark.asyncio
async def test_get_public_profile_recomputes_partial_unsynced_track_results() -> None:
    users = FakeUsersCollection()
    archives = FakeUsersCollection()
    user_id = ObjectId()
    users.docs.append(
        {
            "_id": user_id,
            "username": "randobotany",
            "username_display": "randobotany",
            "email": "bot-random-any@kriegspiel.org",
            "email_verified": True,
            "password_hash": "hash",
            "auth_providers": ["local"],
            "profile": {"bio": "", "avatar_url": None, "country": None},
            "bot_profile": {"owner_email": "bot-random-any@kriegspiel.org"},
            "stats": {
                **default_user_stats_payload(),
                "games_played": 3,
                "games_won": 1,
                "games_lost": 1,
                "games_drawn": 1,
                "results": {
                    "overall": {"games_played": 3, "games_won": 1, "games_lost": 1, "games_drawn": 1},
                    "vs_humans": {"games_played": 0, "games_won": 0, "games_lost": 0, "games_drawn": 0},
                    "vs_bots": {"games_played": 1, "games_won": 1, "games_lost": 0, "games_drawn": 0},
                },
            },
            "settings": {},
            "role": "bot",
            "status": "active",
            "last_active_at": datetime(2026, 4, 6, tzinfo=UTC),
            "created_at": datetime(2026, 4, 6, tzinfo=UTC),
            "updated_at": datetime(2026, 4, 6, tzinfo=UTC),
        }
    )
    archives.docs.extend(
        [
            {
                "_id": ObjectId(),
                "white": {"user_id": str(user_id), "role": "bot"},
                "black": {"user_id": "bot-1", "role": "bot"},
                "result": {"winner": "white"},
                "created_at": datetime(2026, 4, 6, 1, tzinfo=UTC),
            },
            {
                "_id": ObjectId(),
                "white": {"user_id": str(user_id), "role": "bot"},
                "black": {"user_id": "bot-2", "role": "bot"},
                "result": {"winner": "black"},
                "created_at": datetime(2026, 4, 6, 2, tzinfo=UTC),
            },
            {
                "_id": ObjectId(),
                "white": {"user_id": str(user_id), "role": "bot"},
                "black": {"user_id": "bot-3", "role": "bot"},
                "result": {"winner": None},
                "created_at": datetime(2026, 4, 6, 3, tzinfo=UTC),
            },
        ]
    )
    db = FakeDB(users=users, game_archives=archives)
    service = UserService(users)

    profile = await service.get_public_profile(db, "randobotany")

    assert profile is not None
    assert profile["stats"]["games_played"] == 3
    assert profile["stats"]["games_won"] == 1
    assert profile["stats"]["games_lost"] == 1
    assert profile["stats"]["games_drawn"] == 1
    assert profile["stats"]["results"]["overall"]["games_played"] == 3
    assert profile["stats"]["results"]["vs_bots"]["games_played"] == 3
    assert profile["stats"]["results"]["vs_bots"]["games_won"] == 1
    assert profile["stats"]["results"]["vs_bots"]["games_lost"] == 1
    assert profile["stats"]["results"]["vs_bots"]["games_drawn"] == 1
    stored_user = users.docs[0]
    assert stored_user["stats"].get("results_synced_at") is not None


@pytest.mark.asyncio
async def test_get_public_profile_repairs_inconsistent_synced_result_totals() -> None:
    users = FakeUsersCollection()
    archives = FakeUsersCollection()
    user_id = ObjectId()
    users.docs.append(
        {
            "_id": user_id,
            "username": "darkboardmcts",
            "username_display": "Darkboard MCTS",
            "email": "bot@example.com",
            "email_verified": True,
            "password_hash": "hash",
            "auth_providers": ["local"],
            "profile": {"bio": "", "avatar_url": None, "country": None},
            "bot_profile": {"owner_email": "bot@example.com"},
            "stats": {
                **default_user_stats_payload(),
                "games_played": 2,
                "games_lost": 2,
                "results": {
                    "overall": {"games_played": 2, "games_won": 0, "games_lost": 2, "games_drawn": 0},
                    "vs_humans": {"games_played": 1, "games_won": 0, "games_lost": 1, "games_drawn": 0},
                    "vs_bots": {"games_played": 0, "games_won": 0, "games_lost": 0, "games_drawn": 0},
                },
                "results_synced_at": datetime(2026, 5, 18, tzinfo=UTC),
            },
            "settings": {},
            "role": "bot",
            "status": "active",
            "last_active_at": datetime(2026, 5, 18, tzinfo=UTC),
            "created_at": datetime(2026, 5, 18, tzinfo=UTC),
            "updated_at": datetime(2026, 5, 18, tzinfo=UTC),
        }
    )
    archives.docs.append(
        {
            "_id": ObjectId(),
            "white": {"user_id": "human-1", "role": "user"},
            "black": {"user_id": str(user_id), "role": "bot"},
            "result": {"winner": "white"},
            "created_at": datetime(2026, 5, 18, 17, tzinfo=UTC),
        }
    )
    db = FakeDB(users=users, game_archives=archives)
    service = UserService(users)

    profile = await service.get_public_profile(db, "darkboardmcts")

    assert profile is not None
    assert profile["stats"]["games_played"] == 1
    assert profile["stats"]["games_lost"] == 1
    assert profile["stats"]["results"]["overall"]["games_played"] == 1
    assert profile["stats"]["results"]["overall"]["games_lost"] == 1
    assert profile["stats"]["results"]["vs_humans"]["games_played"] == 1
    assert profile["stats"]["results"]["vs_humans"]["games_lost"] == 1
    assert profile["stats"]["results"]["vs_bots"]["games_played"] == 0
    assert users.docs[0]["stats"]["games_played"] == 1
    assert users.docs[0]["stats"].get("results_synced_at") is not None


@pytest.mark.asyncio
async def test_get_rating_history_returns_series_for_selected_track() -> None:
    users = FakeUsersCollection()
    archives = FakeUsersCollection()
    user_id = str(ObjectId())
    archives.docs.extend(
        [
            {
                "_id": ObjectId(),
                "white": {"user_id": user_id, "role": "bot"},
                "black": {"user_id": "opponent-1", "role": "bot"},
                "result": {"winner": "white"},
                "created_at": datetime(2026, 4, 5, 12, tzinfo=UTC),
                "updated_at": datetime(2026, 4, 5, 12, 10, tzinfo=UTC),
                "rating_snapshot": {
                    "overall": {"white_after": 1216, "white_delta": 16},
                    "specific": {"white_after": 1216, "white_delta": 16},
                    "white_track": "vs_bots",
                    "black_track": "vs_bots",
                },
            },
            {
                "_id": ObjectId(),
                "white": {"user_id": user_id, "role": "bot"},
                "black": {"user_id": "opponent-2", "role": "bot"},
                "result": {"winner": None},
                "created_at": datetime(2026, 4, 6, 12, tzinfo=UTC),
                "updated_at": datetime(2026, 4, 6, 12, 10, tzinfo=UTC),
                "rating_snapshot": {
                    "overall": {"white_after": 1220, "white_delta": 4},
                    "specific": {"white_after": 1220, "white_delta": 4},
                    "white_track": "vs_bots",
                    "black_track": "vs_bots",
                },
            },
        ]
    )
    service = UserService(users)

    history = await service.get_rating_history(FakeDB(users, archives), user_id, track="vs_bots", limit=100)

    assert history["track"] == "vs_bots"
    assert len(history["series"]["game"]) == 2
    assert history["series"]["game"][0]["label"] == "Game 1"
    assert history["series"]["date"][1]["label"] == "2026-04-06"


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
                "game_code": "A7K2M9",
                "white": {"user_id": str(user_id), "username": "playerone"},
                "black": {"user_id": str(other_id), "username": "rival-a", "role": "bot"},
                "result": {"winner": "white", "reason": "checkmate"},
                "rating_snapshot": {
                    "overall": {
                        "white_before": 1200,
                        "white_after": 1216,
                        "white_delta": 16,
                        "black_before": 1200,
                        "black_after": 1184,
                        "black_delta": -16,
                    },
                    "specific": {
                        "white_before": 1200,
                        "white_after": 1216,
                        "white_delta": 16,
                        "black_before": 1200,
                        "black_after": 1184,
                        "black_delta": -16,
                    },
                    "white_track": "vs_bots",
                    "black_track": "vs_humans",
                },
                "move_count": 3,
                "turn_count": 1,
                "moves": [{"move_done": True}, {"move_done": False}, {"move_done": True}],
                "created_at": datetime(2026, 3, 10, tzinfo=UTC),
                "updated_at": datetime(2026, 3, 10, tzinfo=UTC),
            },
            {
                "_id": ObjectId(),
                "game_code": "B7K2M9",
                "white": {"user_id": str(other_id), "username": "rival-b"},
                "black": {"user_id": str(user_id), "username": "playerone"},
                "result": {"winner": None, "reason": "stalemate"},
                "move_count": 2,
                "turn_count": 1,
                "moves": [{"move_done": True}, {"move_done": True}],
                "created_at": datetime(2026, 3, 9, tzinfo=UTC),
                "updated_at": datetime(2026, 3, 9, tzinfo=UTC),
            },
        ]
    )
    db = FakeDB(users=users, game_archives=archives)
    service = UserService(users)

    page_1, total, filter_options = await service.get_game_history(db, str(user_id), page=1, per_page=1)
    out_of_range, total_2, _ = await service.get_game_history(db, str(user_id), page=4, per_page=1)

    assert total == 2
    assert total_2 == 2
    assert {"value": "rival-a", "group": "Bots", "count": 1} in filter_options["opponent"]
    assert {"value": "rival-b", "group": "Humans", "count": 1} in filter_options["opponent"]
    assert page_1[0]["game_code"] == "A7K2M9"
    assert page_1[0]["rule_variant"] is None
    assert page_1[0]["opponent"] == "rival-a"
    assert page_1[0]["opponent_role"] == "bot"
    assert page_1[0]["turn_count"] == 1
    assert page_1[0]["elo_before"] == 1200
    assert page_1[0]["elo_after"] == 1216
    assert page_1[0]["elo_delta"] == 16
    assert out_of_range == []


@pytest.mark.asyncio
async def test_get_game_history_uses_indexed_side_queries_without_filter_options() -> None:
    users = FakeUsersCollection()
    archives = FakeUsersCollection()
    user_id = ObjectId()
    archives.docs.extend(
        [
            {
                "_id": ObjectId(),
                "game_code": "OLD001",
                "white": {"user_id": str(user_id), "username": "playerone"},
                "black": {"user_id": "opponent-a", "username": "opponent-a"},
                "result": {"winner": "white", "reason": "checkmate"},
                "move_count": 4,
                "turn_count": 2,
                "created_at": datetime(2026, 7, 1, tzinfo=UTC),
                "updated_at": datetime(2026, 7, 1, tzinfo=UTC),
            },
            {
                "_id": ObjectId(),
                "game_code": "NEW001",
                "white": {"user_id": "opponent-b", "username": "opponent-b"},
                "black": {"user_id": str(user_id), "username": "playerone"},
                "result": {"winner": "black", "reason": "timeout"},
                "move_count": 8,
                "turn_count": 4,
                "created_at": datetime(2026, 7, 3, tzinfo=UTC),
                "updated_at": datetime(2026, 7, 3, tzinfo=UTC),
            },
            {
                "_id": ObjectId(),
                "game_code": "MID001",
                "white": {"user_id": str(user_id), "username": "playerone"},
                "black": {"user_id": "opponent-c", "username": "opponent-c"},
                "result": {"winner": None, "reason": "stalemate"},
                "move_count": 6,
                "turn_count": 3,
                "created_at": datetime(2026, 7, 2, tzinfo=UTC),
                "updated_at": datetime(2026, 7, 2, tzinfo=UTC),
            },
        ]
    )
    db = FakeDB(users=users, game_archives=archives)
    service = UserService(users)

    page, total, filter_options = await service.get_game_history(
        db,
        str(user_id),
        page=1,
        per_page=2,
        sort_key="played_at",
        sort_direction="desc",
        include_filter_options=False,
    )

    assert total == 3
    assert filter_options == {}
    assert [game["game_code"] for game in page] == ["NEW001", "MID001"]
    assert archives.find_calls == [
        ({"white.user_id": str(user_id)}, user_service_module.USER_GAME_HISTORY_PROJECTION),
        ({"black.user_id": str(user_id)}, user_service_module.USER_GAME_HISTORY_PROJECTION),
    ]


@pytest.mark.asyncio
async def test_get_game_history_clamps_direct_service_calls_to_10000_per_page() -> None:
    user_id = ObjectId()
    users = FakeUsersCollection()
    archives = FakeUsersCollection()
    archives.docs.extend(
        {
            "_id": ObjectId(),
            "game_code": f"G{idx:05d}",
            "white": {"user_id": str(user_id), "username": "playerone", "role": "user"},
            "black": {"user_id": str(ObjectId()), "username": f"rival-{idx}", "role": "bot"},
            "created_at": datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=idx),
            "updated_at": datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=idx),
            "result": {"winner": "white", "reason": "checkmate"},
            "move_count": 0,
            "turn_count": 0,
        }
        for idx in range(10005)
    )
    db = FakeDB(users=users, game_archives=archives)
    service = UserService(users)

    page, total, _ = await service.get_game_history(db, str(user_id), page=1, per_page=10001)

    assert total == 10005
    assert len(page) == 10000


@pytest.mark.asyncio
async def test_get_game_history_filters_and_sorts_before_paginating() -> None:
    users = FakeUsersCollection()
    archives = FakeUsersCollection()
    user_id = ObjectId()
    archives.docs.extend(
        [
            {
                "_id": ObjectId(),
                "game_code": "BOT001",
                "white": {"user_id": str(user_id), "username": "randobotany", "role": "bot"},
                "black": {"user_id": "bot-gemini", "username": "bot_gemini31_lite", "role": "bot"},
                "rule_variant": "berkeley_any",
                "result": {"winner": "white", "reason": "resignation"},
                "move_count": 8,
                "turn_count": 4,
                "created_at": datetime(2026, 7, 1, tzinfo=UTC),
                "updated_at": datetime(2026, 7, 1, tzinfo=UTC),
            },
            {
                "_id": ObjectId(),
                "game_code": "BOT002",
                "white": {"user_id": str(user_id), "username": "randobotany", "role": "bot"},
                "black": {"user_id": "bot-random", "username": "randobot", "role": "bot"},
                "rule_variant": "berkeley_any",
                "result": {"winner": "black", "reason": "timeout"},
                "move_count": 16,
                "turn_count": 8,
                "created_at": datetime(2026, 7, 2, tzinfo=UTC),
                "updated_at": datetime(2026, 7, 2, tzinfo=UTC),
            },
        ]
    )
    db = FakeDB(users=users, game_archives=archives)
    service = UserService(users)

    page, total, filter_options = await service.get_game_history(
        db,
        str(user_id),
        page=1,
        per_page=100,
        filters={"opponent": ["bot_gemini31_lite"]},
        sort_key="turns",
        sort_direction="desc",
    )

    assert total == 1
    assert [game["game_code"] for game in page] == ["BOT001"]
    assert {option["value"] for option in filter_options["opponent"]} == {
        "bot_gemini31_lite",
        "randobot",
    }

    no_sort_page, _, _ = await service.get_game_history(
        db,
        str(user_id),
        page=1,
        per_page=100,
        sort_key="none",
        sort_direction="asc",
    )

    assert [game["game_code"] for game in no_sort_page] == ["BOT002", "BOT001"]


@pytest.mark.asyncio
async def test_get_game_history_filters_by_opponent_group_tokens() -> None:
    users = FakeUsersCollection()
    archives = FakeUsersCollection()
    user_id = ObjectId()
    archives.docs.extend(
        [
            {
                "_id": ObjectId(),
                "game_code": "BOT001",
                "white": {"user_id": str(user_id), "username": "playerone", "role": "user"},
                "black": {"user_id": "bot-random", "username": "randobot", "role": "bot"},
                "result": {"winner": "white", "reason": "checkmate"},
                "move_count": 8,
                "turn_count": 4,
                "created_at": datetime(2026, 7, 1, tzinfo=UTC),
                "updated_at": datetime(2026, 7, 1, tzinfo=UTC),
            },
            {
                "_id": ObjectId(),
                "game_code": "HUM001",
                "white": {"user_id": str(user_id), "username": "playerone", "role": "user"},
                "black": {"user_id": "human-rival", "username": "lgyanf", "role": "user"},
                "result": {"winner": "black", "reason": "timeout"},
                "move_count": 12,
                "turn_count": 6,
                "created_at": datetime(2026, 7, 2, tzinfo=UTC),
                "updated_at": datetime(2026, 7, 2, tzinfo=UTC),
            },
        ]
    )
    db = FakeDB(users=users, game_archives=archives)
    service = UserService(users)

    bot_page, bot_total, _ = await service.get_game_history(
        db,
        str(user_id),
        page=1,
        per_page=100,
        filters={"opponent": ["bot:*"]},
    )
    human_page, human_total, _ = await service.get_game_history(
        db,
        str(user_id),
        page=1,
        per_page=100,
        filters={"opponent": ["human:*"]},
    )

    assert bot_total == 1
    assert [game["game_code"] for game in bot_page] == ["BOT001"]
    assert human_total == 1
    assert [game["game_code"] for game in human_page] == ["HUM001"]


@pytest.mark.asyncio
async def test_get_game_history_uses_aggregation_for_filtered_rows_without_facets() -> None:
    users = FakeUsersCollection()
    user_id = ObjectId()
    row = {
        "_id": ObjectId(),
        "game_code": "BOT001",
        "white": {"user_id": str(user_id), "username": "randobotany", "role": "bot"},
        "black": {"user_id": "bot-random", "username": "randobot", "role": "bot"},
        "rule_variant": "berkeley_any",
        "result": {"winner": "white", "reason": "resignation"},
        "move_count": 8,
        "turn_count": 4,
        "created_at": datetime(2026, 7, 1, tzinfo=UTC),
        "updated_at": datetime(2026, 7, 1, tzinfo=UTC),
    }
    archives = FakeAggregateCollection({"rows": [row], "total": [{"count": 1}]})
    db = FakeDB(users=users, game_archives=archives)
    service = UserService(users)

    page, total, filter_options = await service.get_game_history(
        db,
        str(user_id),
        page=1,
        per_page=100,
        filters={"opponent": ["randobot"]},
        sort_key="turns",
        sort_direction="desc",
        include_filter_options=False,
    )

    assert total == 1
    assert filter_options == {}
    assert [game["game_code"] for game in page] == ["BOT001"]
    assert page[0]["opponent"] == "randobot"
    assert archives.find_calls == []
    assert len(archives.aggregate_calls) == 1
    pipeline = archives.aggregate_calls[0]
    assert {
        "$match": {"history_filter_opponent": {"$in": ["randobot", "human:randobot", "bot:randobot"]}}
    } in pipeline
    assert pipeline[-1]["$facet"]["rows"][0]["$sort"]["history_turns"] == -1


@pytest.mark.asyncio
async def test_get_game_history_aggregation_filters_by_opponent_group_tokens() -> None:
    users = FakeUsersCollection()
    user_id = ObjectId()
    row = {
        "_id": ObjectId(),
        "game_code": "BOT001",
        "white": {"user_id": str(user_id), "username": "playerone", "role": "user"},
        "black": {"user_id": "bot-random", "username": "randobot", "role": "bot"},
        "result": {"winner": "white", "reason": "checkmate"},
        "move_count": 8,
        "turn_count": 4,
        "created_at": datetime(2026, 7, 1, tzinfo=UTC),
        "updated_at": datetime(2026, 7, 1, tzinfo=UTC),
    }
    archives = FakeAggregateCollection({"rows": [row], "total": [{"count": 1}]})
    db = FakeDB(users=users, game_archives=archives)
    service = UserService(users)

    page, total, filter_options = await service.get_game_history(
        db,
        str(user_id),
        page=1,
        per_page=100,
        filters={"opponent": ["bot:*"]},
        include_filter_options=False,
    )

    assert total == 1
    assert filter_options == {}
    assert [game["game_code"] for game in page] == ["BOT001"]
    assert archives.find_calls == []
    assert len(archives.aggregate_calls) == 1
    assert {"$match": {"history_opponent_group": {"$in": ["bot"]}}} in archives.aggregate_calls[0]


@pytest.mark.asyncio
async def test_get_game_history_aggregation_filters_by_result_after_materializing_result() -> None:
    users = FakeUsersCollection()
    user_id = ObjectId()
    row = {
        "_id": ObjectId(),
        "game_code": "WIN001",
        "white": {"user_id": str(user_id), "username": "randobotany", "role": "bot"},
        "black": {"user_id": "human-rival", "username": "notifil", "role": "user"},
        "result": {"winner": "white", "reason": "checkmate"},
        "move_count": 8,
        "turn_count": 4,
        "created_at": datetime(2026, 7, 1, tzinfo=UTC),
        "updated_at": datetime(2026, 7, 1, tzinfo=UTC),
    }
    archives = FakeAggregateCollection({"rows": [row], "total": [{"count": 1}]})
    db = FakeDB(users=users, game_archives=archives)
    service = UserService(users)

    page, total, filter_options = await service.get_game_history(
        db,
        str(user_id),
        page=1,
        per_page=100,
        filters={"opponent": ["notifil"], "result": ["win"]},
        include_filter_options=False,
    )

    assert total == 1
    assert filter_options == {}
    assert [game["game_code"] for game in page] == ["WIN001"]
    pipeline = archives.aggregate_calls[0]
    history_result_stage = next(
        idx for idx, stage in enumerate(pipeline) if "history_result" in stage.get("$addFields", {})
    )
    history_filter_result_stage = next(
        idx for idx, stage in enumerate(pipeline) if "history_filter_result" in stage.get("$addFields", {})
    )
    assert history_filter_result_stage > history_result_stage
    assert {
        "$match": {
            "$and": [
                {"history_filter_result": {"$in": ["win"]}},
                {"history_filter_opponent": {"$in": ["notifil", "human:notifil", "bot:notifil"]}},
            ]
        }
    } in pipeline


@pytest.mark.asyncio
async def test_get_game_history_handles_null_result_documents() -> None:
    users = FakeUsersCollection()
    archives = FakeUsersCollection()
    user_id = ObjectId()
    other_id = ObjectId()
    archives.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "C7K2M9",
            "white": {"user_id": str(user_id), "username": "playerone"},
            "black": {"user_id": str(other_id), "username": "rival-a"},
            "result": None,
            "move_count": 3,
            "turn_count": 2,
            "moves": [{"move_done": True}, {"move_done": True}, {"move_done": True}],
            "created_at": datetime(2026, 3, 10, tzinfo=UTC),
            "updated_at": datetime(2026, 3, 10, tzinfo=UTC),
        }
    )
    db = FakeDB(users=users, game_archives=archives)
    service = UserService(users)

    page, total, _ = await service.get_game_history(db, str(user_id), page=1, per_page=10)

    assert total == 1
    assert page[0]["result"] == "draw"
    assert page[0]["reason"] is None
    assert page[0]["turn_count"] == 2


@pytest.mark.asyncio
async def test_get_game_history_exposes_named_track_snapshots_for_selected_track() -> None:
    users = FakeUsersCollection()
    archives = FakeUsersCollection()
    user_id = ObjectId()
    other_id = ObjectId()
    archives.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "D7K2M9",
            "white": {"user_id": str(user_id), "username": "llm_gptnano", "role": "bot"},
            "black": {"user_id": str(other_id), "username": "randobot", "role": "bot"},
            "result": {"winner": "black", "reason": "checkmate"},
            "rating_snapshot": {
                "overall": {
                    "white_before": 1333,
                    "white_after": 1312,
                    "white_delta": -21,
                    "black_before": 1226,
                    "black_after": 1247,
                    "black_delta": 21,
                },
                "specific": {
                    "white_before": 1294,
                    "white_after": 1273,
                    "white_delta": -21,
                    "black_before": 1190,
                    "black_after": 1211,
                    "black_delta": 21,
                },
                "white_track": "vs_bots",
                "black_track": "vs_bots",
            },
            "move_count": 1,
            "turn_count": 1,
            "moves": [{"move_done": True}],
            "created_at": datetime(2026, 4, 6, tzinfo=UTC),
            "updated_at": datetime(2026, 4, 6, tzinfo=UTC),
        }
    )
    db = FakeDB(users=users, game_archives=archives)
    service = UserService(users)

    page, total, _ = await service.get_game_history(db, str(user_id), page=1, per_page=10)

    assert total == 1
    assert page[0]["elo_after"] == 1312
    assert page[0]["rating_snapshot"]["overall"]["elo_after"] == 1312
    assert page[0]["rating_snapshot"]["vs_bots"]["elo_after"] == 1273
    assert page[0]["rating_snapshot"]["vs_bots"]["elo_delta"] == -21
    assert page[0]["rating_snapshot"]["vs_humans"]["elo_after"] is None


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
    assert players[0]["ratings"]["overall"]["elo"] == 1500


@pytest.mark.asyncio
async def test_get_leaderboard_sorts_filters_and_returns_filter_options() -> None:
    users = FakeUsersCollection()
    users.docs.extend(
        [
            {
                "_id": ObjectId(),
                "username": "alpha",
                "role": "user",
                "status": "active",
                "stats": {
                    "games_played": 10,
                    "games_won": 5,
                    "elo": 1500,
                    "ratings": {"vs_humans": {"elo": 1600}, "vs_bots": {"elo": 1300}},
                },
            },
            {
                "_id": ObjectId(),
                "username": "zeta",
                "role": "user",
                "status": "active",
                "stats": {
                    "games_played": 12,
                    "games_won": 8,
                    "elo": 1500,
                    "ratings": {"vs_humans": {"elo": 1510}, "vs_bots": {"elo": 1490}},
                },
            },
            {
                "_id": ObjectId(),
                "username": "randobot",
                "role": "bot",
                "status": "active",
                "bot_profile": {"display_name": "Random Bot", "listed": True},
                "stats": {
                    "games_played": 2,
                    "games_won": 2,
                    "elo": 1400,
                    "ratings": {"vs_humans": {"elo": 1200}, "vs_bots": {"elo": 1420}},
                },
            },
        ]
    )
    db = FakeDB(users=users, game_archives=FakeUsersCollection())
    service = UserService(users)

    players, total = await service.get_leaderboard(
        db,
        page=1,
        per_page=20,
        filters={"type": ["human"]},
        sort_key="games",
        sort_direction="desc",
    )
    filtered_bot_players, filtered_bot_total = await service.get_leaderboard(
        db,
        page=1,
        per_page=20,
        filters={"username": ["randobot"]},
        sort_key="win_rate",
        sort_direction="desc",
    )
    filter_options = await service.get_leaderboard_filter_options(db)

    assert total == 2
    assert [player["username"] for player in players] == ["zeta", "alpha"]
    assert [player["rank"] for player in players] == [2, 1]
    assert players[0]["ratings"]["vs_humans"]["elo"] == 1510

    assert filtered_bot_total == 1
    assert filtered_bot_players[0]["username"] == "randobot"
    assert filtered_bot_players[0]["rank"] == 3
    assert filtered_bot_players[0]["display_name"] == "Random Bot"

    assert {"value": "human", "label": "Human", "group": "", "count": 2} in filter_options["type"]
    assert {"value": "bot", "label": "Bot", "group": "", "count": 1} in filter_options["type"]
    assert {"value": "alpha", "label": "alpha", "group": "Humans", "count": 1} in filter_options["username"]
    assert {"value": "randobot", "label": "randobot", "group": "Bots", "count": 1} in filter_options["username"]


def test_helper_edges_cover_password_parsing_datetime_and_result_reasoning() -> None:
    assert UserService.verify_password("secret", "not-a-bcrypt-hash") is False
    assert UserService.parse_bot_token("not-a-token") is None
    assert UserService.parse_bot_token("ksbot_onlyprefix") is None
    assert UserService.parse_bot_token("ksbot_.secret") is None
    assert UserService.parse_bot_token("ksbot_token.") is None
    assert isinstance(UserService._safe_datetime("bad-value"), datetime)
    with pytest.raises(ValueError, match="Invalid user id"):
        UserService._to_object_id("bad-id")

    assert UserService._normalized_result_reason({"moves": [{"special_announcement": "DRAW_INSUFFICIENT"}]}) == "insufficient"
    assert UserService._normalized_result_reason({"moves": [{"special_announcement": "DRAW_STALEMATE"}]}) == "stalemate"
    assert (
        UserService._normalized_result_reason({"moves": [{"special_announcement": "DRAW_TOOMANYREVERSIBLEMOVES"}]})
        == "too_many_reversible_moves"
    )
    assert UserService._normalized_result_reason({"moves": [{"special_announcement": "STALEMATE_BLACK_WINS"}]}) == "stalemate"
    assert UserService._normalized_result_reason({"moves": [{"special_announcement": "CHECKMATE_BLACK_WINS"}]}) == "checkmate"
    assert UserService._history_opponent_filter_matches("human:notifil", ["notifil"]) is True
    assert UserService._history_opponent_filter_matches("human:notifil", ["bot:*"]) is False


def test_remaining_user_service_helper_edges(monkeypatch: pytest.MonkeyPatch) -> None:
    naive = datetime(2026, 5, 9, 12)
    generated_id = ObjectId()
    inconsistent_stats = default_user_stats_payload()
    inconsistent_stats["games_played"] = 99

    assert UserService._optional_datetime(naive) == naive.replace(tzinfo=UTC)
    assert UserService._created_day({"_id": generated_id}) == generated_id.generation_time.date().isoformat()
    assert UserService._result_tracks_are_consistent(inconsistent_stats) is False
    assert (
        UserService._profile_metric_play_as(
            {"white": None, "black": {"username": "MetricBot"}},
            user_id="missing",
            username="metricbot",
        )
        == "black"
    )
    assert UserService._profile_metric_play_as({"white": None, "black": None}, user_id="missing", username="metricbot") is None
    assert UserService._profile_metric_turn_count({"turn_count": "bad", "move_count": 5}) == 5
    assert UserService._profile_metric_turn_count({"turn_count": "bad", "move_count": "bad"}) == 0
    assert UserService._activity_game_has_completed_move({"moves": [{"move_done": True}]}) is True
    assert (
        UserService._activity_game_has_completed_move(
            {"move_number": "bad", "move_count": "bad", "turn_count": "bad"}
        )
        is False
    )
    assert UserService._game_clock_duration_seconds({"moves": []}) is None
    assert UserService._positive_float("bad", default=3.0) == 3.0
    assert UserService._positive_float(float("inf"), default=3.0) == 3.0
    assert UserService._positive_float(-1, default=3.0) == 3.0
    assert UserService._activity_player_key({"username": " PlayerOne "}) == "username:playerone"
    assert UserService._activity_player_key({"username": " "}) is None

    original_optional_datetime = UserService._optional_datetime
    flaky_timestamp_calls = iter([datetime(2026, 5, 9, tzinfo=UTC), None])

    def flaky_optional_datetime(value):  # noqa: ANN001
        if value == "flaky":
            return next(flaky_timestamp_calls)
        return original_optional_datetime(value)

    monkeypatch.setattr(UserService, "_optional_datetime", staticmethod(flaky_optional_datetime))
    assert UserService._game_clock_duration_seconds({"moves": [{"timestamp": "flaky", "color": "white"}]}) == 0


def test_game_clock_duration_handles_invalid_colors_and_pending_attempts() -> None:
    started_at = datetime(2026, 5, 9, 12, tzinfo=UTC)

    assert (
        UserService._game_clock_duration_seconds(
            {
                "state": "completed",
                "result": {"reason": "timeout"},
                "time_control": {"base": 20, "increment": 2},
                "moves": [
                    {"timestamp": started_at, "color": "white", "move_done": True},
                    {"timestamp": started_at + timedelta(seconds=5), "color": "green", "move_done": True},
                    {"timestamp": started_at + timedelta(seconds=9), "color": "black", "move_done": False},
                ],
            }
        )
        == 20
    )
    assert (
        UserService._game_clock_duration_seconds(
            {
                "moves": [
                    {"timestamp": started_at, "color": "green", "move_done": True},
                    {"timestamp": started_at + timedelta(seconds=5), "color": "white", "move_done": False},
                ],
            }
        )
        == 0
    )


@pytest.mark.asyncio
async def test_profile_metrics_skips_unmatched_games_and_uses_username_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bot_id = ObjectId()
    archive_docs = [
        {
            "_id": ObjectId(),
            "state": "active",
            "white": {"username": "metricbot", "role": "bot"},
            "black": {"username": "human", "role": "user"},
        },
        {
            "_id": ObjectId(),
            "state": "completed",
            "white": {"username": "someoneelse", "role": "bot"},
            "black": {"username": "human", "role": "user"},
        },
        {
            "_id": ObjectId(),
            "state": "completed",
            "rule_variant": "wild16",
            "white": {"username": " MetricBot ", "role": "bot"},
            "black": {"username": "human", "role": "user"},
            "result": {"winner": "white"},
            "move_count": "bad",
            "turn_count": 7,
            "created_at": datetime(2026, 5, 9, 12, tzinfo=UTC),
            "updated_at": datetime(2026, 5, 9, 12, 5, tzinfo=UTC),
        },
        {
            "_id": ObjectId(),
            "state": "completed",
            "rule_variant": "wild16",
            "white": {"username": " MetricBot ", "role": "bot"},
            "black": {"username": "human", "role": "user"},
            "result": {"winner": "black"},
            "turn_count": 1,
            "created_at": datetime(2026, 5, 8, 12, tzinfo=UTC),
            "updated_at": datetime(2026, 5, 8, 12, 1, tzinfo=UTC),
        },
    ]
    service = UserService(FakeUsersCollection())
    monkeypatch.setattr(service, "_find", lambda collection, query, projection=None: FakeCursor(archive_docs))  # noqa: ARG005

    metrics = await service._profile_metrics(
        FakeDB(users=FakeUsersCollection(), game_archives=FakeUsersCollection()),
        {"_id": bot_id, "username": "metricbot"},
    )

    assert metrics["completed_games"] == 2
    assert metrics["overall"]["wins"] == 1
    assert metrics["average_turn_count"] == 4.0
    assert metrics["opponents"][0]["username"] == "human"


def test_find_and_aggregate_series_cover_projection_fallbacks() -> None:
    class ProjectionlessCollection:
        def find(self, query: dict):  # noqa: ANN001
            return [query]

    result = UserService._find(ProjectionlessCollection(), {"role": "bot"}, {"username": 1})
    aggregated = UserService._aggregate_series(
        [
            {"label": "Game 1", "elo": 1200, "delta": 5, "played_at": "2026-04-01T00:00:00+00:00", "game_number": 1},
            {"label": "Game 2", "elo": 1210, "delta": 10, "played_at": "2026-04-02T00:00:00+00:00", "game_number": 2},
            {"label": "Game 3", "elo": 1225, "delta": 15, "played_at": "2026-04-03T00:00:00+00:00", "game_number": 3},
            {"label": "Game 4", "elo": 1230, "delta": 5, "played_at": "2026-04-04T00:00:00+00:00", "game_number": 4},
            {"label": "Game 5", "elo": 1240, "delta": 10, "played_at": "2026-04-05T00:00:00+00:00", "game_number": 5},
        ],
        limit=2,
        label_key="label",
    )

    assert result == [{"role": "bot"}]
    assert aggregated == [
        {
            "label": "Game 1 - Game 3",
            "elo": 1225,
            "delta": 15,
            "played_at": "2026-04-03T00:00:00+00:00",
            "game_number": 3,
        },
        {
            "label": "Game 4 - Game 5",
            "elo": 1240,
            "delta": 15,
            "played_at": "2026-04-05T00:00:00+00:00",
            "game_number": 5,
        },
    ]


def test_bot_token_cache_expires_stale_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    user = UserModel.from_mongo(
        {
            "_id": ObjectId(),
            "username": "cachebot",
            "username_display": "Cache Bot",
            "email": "cachebot@bots.kriegspiel.local",
            "password_hash": "hash",
            "auth_providers": ["bot_token"],
            "profile": {"bio": "", "avatar_url": None, "country": None},
            "bot_profile": {"display_name": "Cache Bot", "owner_email": "bots@kriegspiel.org"},
            "stats": default_user_stats_payload(),
            "settings": {"board_theme": "default", "piece_set": "cburnett", "sound_enabled": False, "auto_ask_any": False},
            "role": "bot",
            "status": "active",
            "last_active_at": datetime.now(UTC),
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        }
    )
    UserService.clear_bot_token_cache()
    monkeypatch.setattr("app.services.user_service.time.monotonic", lambda: 100.0)
    UserService._bot_token_cache["expired"] = (99.0, user)
    UserService._bot_token_cache["fresh"] = (101.0, user)

    assert UserService._get_cached_bot_user("expired") is None
    assert UserService._get_cached_bot_user("fresh") is user
    assert "expired" not in UserService._bot_token_cache
    UserService.clear_bot_token_cache()


@pytest.mark.asyncio
async def test_authenticate_bot_token_uses_cache_and_rejects_invalid_or_missing_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    users = FakeUsersCollection()
    service = UserService(users)
    cached_user = UserModel.from_mongo(
        {
            "_id": ObjectId(),
            "username": "cachedbot",
            "username_display": "Cached Bot",
            "email": "cachedbot@bots.kriegspiel.local",
            "password_hash": "hash",
            "auth_providers": ["bot_token"],
            "profile": {"bio": "", "avatar_url": None, "country": None},
            "bot_profile": {"display_name": "Cached Bot", "owner_email": "bots@kriegspiel.org"},
            "stats": default_user_stats_payload(),
            "settings": {"board_theme": "default", "piece_set": "cburnett", "sound_enabled": False, "auto_ask_any": False},
            "role": "bot",
            "status": "active",
            "last_active_at": datetime.now(UTC),
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        }
    )
    UserService.clear_bot_token_cache()
    monkeypatch.setattr("app.services.user_service.time.monotonic", lambda: 100.0)
    UserService._bot_token_cache["ksbot_cached.secret"] = (101.0, cached_user)

    assert await service.authenticate_bot_token("ksbot_cached.secret") is cached_user
    assert await service.authenticate_bot_token("bad-token") is None
    assert await service.authenticate_bot_token("ksbot_missing.secret") is None

    token_id = "digestbot"
    users.docs.append(
        {
            "_id": ObjectId(),
            "username": "digestbot",
            "username_display": "Digest Bot",
            "email": "digestbot@bots.kriegspiel.local",
            "email_verified": True,
            "password_hash": "hash",
            "auth_providers": ["bot_token"],
            "profile": {"bio": "", "avatar_url": None, "country": None},
            "bot_profile": {
                "display_name": "Digest Bot",
                "owner_email": "owner@example.com",
                "description": "",
                "listed": True,
                "api_token_id": token_id,
                "api_token_digest": UserService.bot_token_digest("actual-secret"),
                "supported_rule_variants": ["berkeley", "berkeley_any"],
            },
            "stats": default_user_stats_payload(),
            "settings": {"board_theme": "default", "piece_set": "cburnett", "sound_enabled": False, "auto_ask_any": False},
            "role": "bot",
            "status": "active",
            "last_active_at": datetime.now(UTC),
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        }
    )

    assert await service.authenticate_bot_token(f"ksbot_{token_id}.wrong-secret") is None
    UserService.clear_bot_token_cache()


@pytest.mark.asyncio
async def test_create_user_and_bot_surface_duplicate_key_errors() -> None:
    class DuplicateUsersCollection(FakeUsersCollection):
        def __init__(self, message: str) -> None:
            super().__init__()
            self.message = message

        async def insert_one(self, payload: dict):  # noqa: ARG002
            raise DuplicateKeyError(self.message)

    with pytest.raises(UserConflictError) as username_exc:
        await UserService(DuplicateUsersCollection("duplicate username index")).create_user(
            RegisterRequest(username="PlayerOne", email="one@example.com", password="abc12345")
        )
    assert username_exc.value.field == "username"

    with pytest.raises(UserConflictError) as email_exc:
        await UserService(DuplicateUsersCollection("duplicate email index")).create_user(
            RegisterRequest(username="PlayerTwo", email="two@example.com", password="abc12345")
        )
    assert email_exc.value.field == "email"

    existing_bot_users = FakeUsersCollection()
    existing_bot_users.docs.append({"_id": ObjectId(), "username": "takenbot"})
    with pytest.raises(UserConflictError) as existing_bot_exc:
        await UserService(existing_bot_users).create_bot(
            BotRegisterRequest(
                username="takenbot",
                display_name="Taken Bot",
                owner_email="owner@example.com",
                description="duplicate",
            )
        )
    assert existing_bot_exc.value.field == "username"

    with pytest.raises(UserConflictError) as duplicate_insert_exc:
        await UserService(DuplicateUsersCollection("duplicate key")).create_bot(
            BotRegisterRequest(
                username="newbot",
                display_name="New Bot",
                owner_email="owner@example.com",
                description="duplicate insert",
            )
        )
    assert duplicate_insert_exc.value.field == "username"


@pytest.mark.asyncio
async def test_get_public_profile_avoids_recomputing_synced_results(monkeypatch: pytest.MonkeyPatch) -> None:
    users = FakeUsersCollection()
    user_id = ObjectId()
    users.docs.append(
        {
            "_id": user_id,
            "username": "fil",
            "username_display": "fil",
            "email": "fil@example.com",
            "email_verified": True,
            "password_hash": "hash",
            "auth_providers": ["local"],
            "profile": {"bio": "", "avatar_url": None, "country": None},
            "bot_profile": None,
            "stats": {
                **default_user_stats_payload(),
                "games_played": 3,
                "games_won": 2,
                "games_lost": 1,
                "results": {
                    "overall": {"games_played": 3, "games_won": 2, "games_lost": 1, "games_drawn": 0},
                    "vs_humans": {"games_played": 1, "games_won": 0, "games_lost": 1, "games_drawn": 0},
                    "vs_bots": {"games_played": 2, "games_won": 2, "games_lost": 0, "games_drawn": 0},
                },
                "results_synced_at": datetime(2026, 4, 6, tzinfo=UTC),
            },
            "settings": {},
            "role": "user",
            "status": "active",
            "last_active_at": datetime(2026, 4, 6, tzinfo=UTC),
            "created_at": datetime(2026, 4, 6, tzinfo=UTC),
            "updated_at": datetime(2026, 4, 6, tzinfo=UTC),
        }
    )
    service = UserService(users)

    async def should_not_recompute(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("should not recompute")

    monkeypatch.setattr(service, "_compute_result_tracks", should_not_recompute)

    profile = await service.get_public_profile(FakeDB(users=users, game_archives=FakeUsersCollection()), "fil")

    assert profile is not None
    assert profile["stats"]["results"]["overall"]["games_played"] == 3


@pytest.mark.asyncio
async def test_get_rating_history_skips_other_tracks_and_missing_snapshots() -> None:
    archives = FakeUsersCollection()
    user_id = str(ObjectId())
    archives.docs.extend(
        [
            {
                "_id": ObjectId(),
                "white": {"user_id": user_id, "role": "user"},
                "black": {"user_id": "bot-1", "role": "bot"},
                "result": {"winner": "white"},
                "created_at": datetime(2026, 4, 5, 12, tzinfo=UTC),
                "updated_at": datetime(2026, 4, 5, 12, 10, tzinfo=UTC),
                "rating_snapshot": {
                    "overall": {"white_after": 1216, "white_delta": 16},
                    "specific": {"white_after": 1216, "white_delta": 16},
                    "white_track": "vs_bots",
                    "black_track": "vs_humans",
                },
            },
            {
                "_id": ObjectId(),
                "white": {"user_id": user_id, "role": "user"},
                "black": {"user_id": "human-1", "role": "user"},
                "result": {"winner": "white"},
                "created_at": datetime(2026, 4, 6, 12, tzinfo=UTC),
                "updated_at": datetime(2026, 4, 6, 12, 10, tzinfo=UTC),
                "rating_snapshot": {
                    "overall": {"white_after": 1220, "white_delta": 4},
                    "specific": {"white_after": 1188, "white_delta": -12},
                    "white_track": "vs_humans",
                    "black_track": "vs_humans",
                },
            },
            {
                "_id": ObjectId(),
                "white": {"user_id": user_id, "role": "user"},
                "black": {"user_id": "human-2", "role": "user"},
                "result": {"winner": None},
                "created_at": datetime(2026, 4, 7, 12, tzinfo=UTC),
                "updated_at": datetime(2026, 4, 7, 12, 10, tzinfo=UTC),
                "rating_snapshot": {
                    "overall": {"white_after": 1220, "white_delta": 0},
                    "specific": {"white_delta": 0},
                    "white_track": "vs_humans",
                    "black_track": "vs_humans",
                },
            },
        ]
    )

    history = await UserService(FakeUsersCollection()).get_rating_history(
        FakeDB(FakeUsersCollection(), archives),
        user_id,
        track="vs_humans",
        limit=100,
    )

    assert history["track"] == "vs_humans"
    assert [point["elo"] for point in history["series"]["game"]] == [1188]
    assert [point["label"] for point in history["series"]["date"]] == ["2026-04-06"]


@pytest.mark.asyncio
async def test_update_settings_raises_for_missing_user() -> None:
    with pytest.raises(ValueError, match="User not found"):
        await UserService(FakeUsersCollection()).update_settings(
            FakeDB(users=FakeUsersCollection(), game_archives=FakeUsersCollection()),
            str(ObjectId()),
            {"board_theme": "dark"},
        )


@pytest.mark.asyncio
async def test_get_listed_bot_daily_report_returns_empty_when_no_bots_are_listed() -> None:
    users = FakeUsersCollection()
    users.docs.extend(
        [
            {"_id": ObjectId(), "username": "hiddenbot", "role": "bot", "bot_profile": {"listed": False}},
            {"_id": ObjectId(), "username": "human", "role": "user"},
        ]
    )

    report = await UserService(users).get_listed_bot_daily_report(
        FakeDB(users=users, game_archives=FakeUsersCollection()),
        days=5,
    )

    assert report == {"timezone": "America/New_York", "bots": []}


@pytest.mark.asyncio
async def test_get_guest_report_lists_guests_with_archive_and_live_game_counts() -> None:
    guest_one_id = ObjectId()
    guest_two_id = ObjectId()
    human_id = ObjectId()
    guest_one_started = datetime(2026, 4, 1, 9, tzinfo=UTC)
    guest_two_started = datetime(2026, 4, 2, 9, tzinfo=UTC)
    archived_at = datetime(2026, 4, 3, 12, tzinfo=UTC)
    live_at = datetime(2026, 4, 4, 13, tzinfo=UTC)

    users = FakeUsersCollection()
    users.docs.extend(
        [
            {
                "_id": guest_one_id,
                "username": "guest_mikhail_tal",
                "username_display": "guest_mikhail_tal",
                "role": "guest",
                "created_at": guest_one_started,
            },
            {
                "_id": guest_two_id,
                "username": "guest_judit_polgar",
                "username_display": "guest_judit_polgar",
                "role": "guest",
                "created_at": guest_two_started,
            },
            {"_id": human_id, "username": "fil", "role": "user", "created_at": guest_two_started},
        ]
    )

    archives = FakeUsersCollection()
    archives.docs.extend(
        [
            {
                "_id": ObjectId(),
                "game_code": "DONE01",
                "white": {"user_id": str(guest_one_id), "username": "guest_mikhail_tal"},
                "black": {"user_id": str(human_id), "username": "fil"},
                "created_at": archived_at - timedelta(minutes=10),
                "updated_at": archived_at,
                "result": {"winner": "white", "reason": "checkmate"},
            },
            {
                "_id": ObjectId(),
                "game_code": "DONE02",
                "white": {"user_id": str(human_id), "username": "fil"},
                "black": {"user_id": str(guest_two_id), "username": "guest_judit_polgar"},
                "created_at": archived_at - timedelta(days=1),
                "updated_at": archived_at - timedelta(days=1),
                "result": {"winner": "white", "reason": "timeout"},
            },
        ]
    )
    games = FakeUsersCollection()
    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "LIVE01",
            "white": {"user_id": str(guest_one_id), "username": "guest_mikhail_tal"},
            "black": {"user_id": str(guest_two_id), "username": "guest_judit_polgar"},
            "created_at": live_at - timedelta(minutes=5),
            "updated_at": live_at,
        }
    )
    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "DONE01",
            "white": {"user_id": str(guest_one_id), "username": "guest_mikhail_tal"},
            "black": {"user_id": str(human_id), "username": "fil"},
            "created_at": archived_at - timedelta(hours=2),
            "updated_at": archived_at - timedelta(hours=1),
        }
    )

    report = await UserService(users).get_guest_report(FakeDB(users=users, game_archives=archives, games=games))

    assert report["total"] == 2
    assert report["available_guest_accounts"] == UserService.guest_name_pool_size() - 2
    rows = {guest["username"]: guest for guest in report["guests"]}
    assert rows["guest_mikhail_tal"] == {
        "name": "guest_mikhail_tal",
        "username": "guest_mikhail_tal",
        "day_started": "2026-04-01",
        "last_game": "2026-04-04T13:00:00+00:00",
        "number_of_games": 2,
        "non_timeout_games": 1,
        "total_time_played_seconds": 900,
    }
    assert rows["guest_judit_polgar"]["day_started"] == "2026-04-02"
    assert rows["guest_judit_polgar"]["last_game"] == "2026-04-04T13:00:00+00:00"
    assert rows["guest_judit_polgar"]["number_of_games"] == 2
    assert rows["guest_judit_polgar"]["non_timeout_games"] == 0
    assert rows["guest_judit_polgar"]["total_time_played_seconds"] == 300


@pytest.mark.asyncio
async def test_get_guest_report_uses_clock_time_for_delayed_timeout_archive() -> None:
    guest_id = ObjectId()
    bot_id = ObjectId()
    started_at = datetime(2026, 5, 13, 4, 34, 2, tzinfo=UTC)
    archived_at = started_at + timedelta(hours=8, minutes=46)

    users = FakeUsersCollection()
    users.docs.extend(
        [
            {
                "_id": guest_id,
                "username": "guest_soso_kupreichik",
                "username_display": "guest_soso_kupreichik",
                "role": "guest",
                "created_at": started_at,
            },
            {"_id": bot_id, "username": "randobot", "role": "bot", "created_at": started_at},
        ]
    )

    archives = FakeUsersCollection()
    archives.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "PE2S7Q",
            "state": "completed",
            "white": {"user_id": str(bot_id), "username": "randobot"},
            "black": {"user_id": str(guest_id), "username": "guest_soso_kupreichik"},
            "created_at": started_at,
            "updated_at": archived_at,
            "result": {"winner": "white", "reason": "timeout"},
            "time_control": {
                "base": 1500.0,
                "increment": 10.0,
                "white_remaining": 1535.0,
                "black_remaining": 0.0,
                "active_color": None,
                "last_updated_at": archived_at,
            },
            "moves": [
                {"color": "white", "move_done": True, "timestamp": started_at + timedelta(seconds=4)},
                {"color": "black", "move_done": True, "timestamp": started_at + timedelta(seconds=16)},
                {"color": "white", "move_done": True, "timestamp": started_at + timedelta(seconds=30)},
            ],
        }
    )

    report = await UserService(users).get_guest_report(FakeDB(users=users, game_archives=archives))

    row = report["guests"][0]
    assert row["username"] == "guest_soso_kupreichik"
    assert row["number_of_games"] == 1
    assert row["non_timeout_games"] == 0
    assert row["last_game"] == archived_at.isoformat()
    assert row["total_time_played_seconds"] == 1524


@pytest.mark.asyncio
async def test_get_guest_report_returns_empty_without_guest_accounts() -> None:
    users = FakeUsersCollection()
    users.docs.append({"_id": ObjectId(), "username": "human", "role": "user", "created_at": datetime(2026, 4, 1, tzinfo=UTC)})
    users.docs.append({"_id": None, "username": "", "role": "guest", "created_at": datetime(2026, 4, 1, tzinfo=UTC)})

    report = await UserService(users).get_guest_report(FakeDB(users=users, game_archives=FakeUsersCollection()))

    assert report == {"guests": [], "total": 0, "available_guest_accounts": UserService.guest_name_pool_size()}


@pytest.mark.asyncio
async def test_get_user_activity_report_counts_periods_and_user_games() -> None:
    human_id = ObjectId()
    guest_id = ObjectId()
    bot_id = ObjectId()
    other_bot_id = ObjectId()
    now = datetime(2026, 5, 1, 12, tzinfo=UTC)

    users = FakeUsersCollection()
    users.docs.extend(
        [
            {"_id": human_id, "username": "fil", "role": "user"},
            {"_id": guest_id, "username": "guest_judit_polgar", "role": "guest"},
            {"_id": bot_id, "username": "llm_gptnano", "role": "bot"},
            {"_id": other_bot_id, "username": "llm_haiku", "role": "bot"},
        ]
    )
    archives = FakeUsersCollection()
    archives.docs.extend(
        [
            {
                "_id": ObjectId(),
                "game_code": "USER01",
                "rule_variant": "crazykrieg",
                "state": "completed",
                "white": {"user_id": str(human_id), "username": "fil", "role": "user"},
                "black": {"user_id": str(bot_id), "username": "llm_gptnano"},
                "result": {"winner": "white", "reason": "checkmate"},
                "updated_at": datetime(2026, 5, 1, 10, tzinfo=UTC),
                "turn_count": 12,
            },
            {
                "_id": ObjectId(),
                "game_code": "BOTBOT",
                "white": {"user_id": str(bot_id), "username": "llm_gptnano", "role": "bot"},
                "black": {"user_id": str(other_bot_id), "username": "llm_haiku", "role": "bot"},
                "result": {"winner": "black"},
                "updated_at": datetime(2026, 4, 30, 15, tzinfo=UTC),
            },
        ]
    )
    games = FakeUsersCollection()
    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "LIVE02",
            "rule_variant": "wild16",
            "state": "active",
            "white": {"user_id": str(guest_id), "username": "guest_judit_polgar", "role": "guest"},
            "black": {"user_id": str(bot_id), "username": "llm_gptnano"},
            "move_number": 2,
            "created_at": datetime(2026, 5, 1, 10, 30, tzinfo=UTC),
            "updated_at": datetime(2026, 5, 1, 11, tzinfo=UTC),
        }
    )
    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "WAIT01",
            "rule_variant": "berkeley_any",
            "state": "waiting",
            "white": None,
            "black": {"user_id": str(bot_id), "username": "llm_gptnano", "role": "bot"},
            "created_at": datetime(2026, 5, 1, 11, 30, tzinfo=UTC),
            "updated_at": datetime(2026, 5, 1, 11, 30, tzinfo=UTC),
        }
    )
    games.docs.append(
        {
            "_id": ObjectId(),
            "game_code": "JOINED",
            "rule_variant": "berkeley_any",
            "state": "active",
            "white": {"user_id": str(human_id), "username": "fil", "role": "user"},
            "black": {"user_id": str(bot_id), "username": "llm_gptnano", "role": "bot"},
            "move_number": 1,
            "created_at": datetime(2026, 5, 1, 11, 40, tzinfo=UTC),
            "updated_at": datetime(2026, 5, 1, 11, 40, tzinfo=UTC),
        }
    )

    report = await UserService(users).get_user_activity_report(
        FakeDB(users=users, game_archives=archives, games=games),
        now=now,
    )

    assert report["timezone"] == "America/New_York"
    sections = {section["key"]: section for section in report["sections"]}
    assert set(sections) == {"dau", "wau", "mau"}
    current_day = sections["dau"]["rows"][-1]
    assert current_day["label"] == "2026-05-01"
    assert current_day["active_users"] == 2
    assert current_day["active_bots"] == 1
    assert current_day["total_games"] == 2
    previous_day = sections["dau"]["rows"][-2]
    assert previous_day["active_users"] == 0
    assert previous_day["active_bots"] == 2
    assert previous_day["total_games"] == 1
    current_week = sections["wau"]["rows"][-1]
    assert current_week["active_users"] == 2
    assert current_week["active_bots"] == 2
    assert current_week["total_games"] == 3

    assert [game["game_code"] for game in report["last_games"]] == ["LIVE02", "USER01"]
    assert "BOTBOT" not in [game["game_code"] for game in report["last_games"]]
    assert "WAIT01" not in [game["game_code"] for game in report["last_games"]]
    assert "JOINED" not in [game["game_code"] for game in report["last_games"]]
    assert report["last_games"][0]["white"] == {"username": "guest_judit_polgar", "role": "guest"}
    assert report["last_games"][0]["review_path"] == "/game/LIVE02/review"
    assert set(report["last_games"][0]) == {
        "game_id",
        "game_code",
        "rule_variant",
        "state",
        "white",
        "black",
        "result",
        "turn_count",
        "move_count",
        "played_at",
        "review_path",
    }
    expected_activity_query = {"updated_at": {"$gte": datetime(2025, 6, 1, 4, tzinfo=UTC)}}
    assert archives.find_calls[0][0] == expected_activity_query
    assert games.find_calls[0][0] == {**expected_activity_query, "state": "active"}
    assert "$or" not in archives.find_calls[0][0]
    assert games.find_calls[1][0] == {"state": "active"}


@pytest.mark.asyncio
async def test_get_user_activity_report_skips_edge_rows_and_caps_recent_games(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    users = object()
    archives = object()
    service = UserService(FakeUsersCollection())
    now = datetime(2026, 5, 1, 12, tzinfo=UTC)

    class TolerantCursor(FakeCursor):
        @staticmethod
        def _resolve(doc: dict, key: str):
            value = FakeCursor._resolve(doc, key)
            return value if value is not None else datetime.min.replace(tzinfo=UTC)

    activity_docs = [
        {"_id": ObjectId(), "game_code": "NODATE", "white": {}, "black": {}},
        {
            "_id": ObjectId(),
            "game_code": "DUPACT",
            "updated_at": datetime(2026, 5, 1, 10, tzinfo=UTC),
            "white": {"username": "newer"},
            "black": {},
        },
        {
            "_id": ObjectId(),
            "game_code": "DUPACT",
            "updated_at": datetime(2026, 5, 1, 9, tzinfo=UTC),
            "white": {"username": "older"},
            "black": {},
        },
        {
            "_id": ObjectId(),
            "game_code": "OLD",
            "updated_at": datetime(2024, 1, 1, tzinfo=UTC),
            "white": {"username": "old"},
            "black": {},
        },
        {
            "_id": ObjectId(),
            "game_code": "NOPLAYER",
            "updated_at": datetime(2026, 5, 1, 10, tzinfo=UTC),
            "white": {},
            "black": {},
        },
    ]
    recent_docs = [
        {
            "_id": ObjectId(),
            "game_code": f"USER{index:03d}",
            "updated_at": now - timedelta(minutes=index),
            "white": {"username": f"user{index}", "role": "user"},
            "black": {"username": "human-opponent", "role": "user"},
        }
        for index in range(101)
    ]
    recent_docs.extend(
        [
            {
                "_id": ObjectId(),
                "game_code": "DUPRECENT",
                "updated_at": now - timedelta(days=1),
                "white": {"username": "newer", "role": "user"},
                "black": {"username": "human-opponent", "role": "user"},
            },
            {
                "_id": ObjectId(),
                "game_code": "DUPRECENT",
                "updated_at": now - timedelta(days=2),
                "white": {"username": "older", "role": "user"},
                "black": {"username": "human-opponent", "role": "user"},
            },
        ]
    )
    recent_docs.append({"_id": ObjectId(), "game_code": "RECENT-NODATE", "white": {}, "black": {}})

    def fake_find(collection, query, projection=None):  # noqa: ANN001
        if collection is users:
            return TolerantCursor([])
        assert collection is archives
        if "updated_at" in query:
            return TolerantCursor(activity_docs)
        return TolerantCursor(recent_docs)

    monkeypatch.setattr(service, "_find", fake_find)

    report = await service.get_user_activity_report(
        FakeDB(users=users, game_archives=archives, games=None),
        now=now,
    )

    assert len(report["last_games"]) == 100
    assert report["last_games"][0]["game_code"] == "USER000"
    assert report["last_games"][-1]["game_code"] == "USER099"


@pytest.mark.asyncio
async def test_get_bot_matrix_report_aggregates_all_listed_bot_archives_for_period() -> None:
    users = FakeUsersCollection()
    users.docs.extend(
        [
            {
                "_id": "haiku-id",
                "username": "llm_haiku",
                "username_display": "LLM Haiku (bot)",
                "role": "bot",
                "bot_profile": {"listed": True, "display_name": "LLM Haiku (bot)"},
            },
            {
                "_id": "nano-id",
                "username": "llm_gptnano",
                "username_display": "LLM GPT-4.5 Nano (bot)",
                "role": "bot",
                "bot_profile": {"listed": True, "display_name": "LLM GPT-4.5 Nano (bot)"},
            },
            {
                "_id": "hidden-id",
                "username": "hiddenbot",
                "username_display": "Hidden Bot",
                "role": "bot",
                "bot_profile": {"listed": False, "display_name": "Hidden Bot"},
            },
            {
                "_id": "human-id",
                "username": "playerone",
                "username_display": "Player One",
                "role": "user",
            },
        ]
    )
    now = datetime(2026, 7, 5, 12, tzinfo=UTC)
    archives = FakeUsersCollection()
    archives.docs.extend(
        [
            {
                "state": "completed",
                "game_code": "OLD001",
                "updated_at": datetime(2026, 7, 4, 12, tzinfo=UTC),
                "white": {"user_id": "haiku-id", "username": "llm_haiku", "role": "bot"},
                "black": {"user_id": "nano-id", "username": "llm_gptnano", "role": "bot"},
                "result": {"winner": "white", "reason": "checkmate"},
                "move_count": 20,
            },
            {
                "state": "completed",
                "game_code": "TODAY1",
                "updated_at": datetime(2026, 7, 5, 9, tzinfo=UTC),
                "white": {"user_id": "nano-id", "username": "llm_gptnano", "role": "bot"},
                "black": {"user_id": "haiku-id", "username": "llm_haiku", "role": "bot"},
                "result": {"winner": None, "reason": "insufficient"},
                "move_count": 10,
                "stats": {
                    "llm_usage": {
                        "white": {
                            "user_id": "nano-id",
                            "username": "llm_gptnano",
                            "calls": 2,
                            "input_tokens": 100,
                            "cached_input_tokens": 0,
                            "output_tokens": 15,
                            "total_tokens": 115,
                            "cost_usd": 0.024,
                        },
                        "black": {
                            "user_id": "haiku-id",
                            "username": "llm_haiku",
                            "calls": 2,
                            "input_tokens": 150,
                            "cached_input_tokens": 20,
                            "output_tokens": 30,
                            "total_tokens": 180,
                            "cost_usd": 0.015,
                        },
                    }
                },
            },
            {
                "state": "completed",
                "game_code": "HUMAN1",
                "updated_at": datetime(2026, 7, 5, 10, tzinfo=UTC),
                "white": {"user_id": "haiku-id", "username": "llm_haiku", "role": "bot"},
                "black": {"user_id": "human-id", "username": "playerone", "role": "user"},
                "result": {"winner": "black", "reason": "resignation"},
                "move_count": 8,
            },
            {
                "state": "completed",
                "game_code": "HID001",
                "updated_at": datetime(2026, 7, 5, 11, tzinfo=UTC),
                "white": {"user_id": "hidden-id", "username": "hiddenbot", "role": "bot"},
                "black": {"user_id": "haiku-id", "username": "llm_haiku", "role": "bot"},
                "result": {"winner": "black", "reason": "timeout"},
                "move_count": 6,
            },
        ]
    )

    report = await UserService(users).get_bot_matrix_report(
        FakeDB(users=users, game_archives=archives),
        period="today",
        now=now,
    )

    assert report["period"] == "today"
    assert report["unique_game_count"] == 1
    assert report["row_record_count"] == 2
    assert [player["username"] for player in report["players"]] == ["llm_haiku", "llm_gptnano"]
    assert report["end_condition_rows"] == [{"condition": "insufficient", "label": "Insufficient material", "games": 1}]

    haiku_row = report["matrix_rows"][0]
    nano_cell = haiku_row["cells"][1]["summary"]
    assert nano_cell["games"] == 1
    assert nano_cell["record"] == "0-1-0"
    assert nano_cell["average_plies"] == 10
    assert nano_cell["usage_recorded_games"] == 1
    assert nano_cell["usage_eligible_games"] == 1
    assert nano_cell["avg_calls"] == 2
    assert nano_cell["player_tokens"] == 180
    assert nano_cell["player_input_tokens"] == 130
    assert nano_cell["player_cache_tokens"] == 20
    assert nano_cell["player_output_tokens"] == 30
    assert nano_cell["player_cost"] == pytest.approx(0.015)
    assert nano_cell["opponent_tokens"] == 115
    assert nano_cell["opponent_input_tokens"] == 100
    assert nano_cell["opponent_cache_tokens"] == 0
    assert nano_cell["opponent_output_tokens"] == 15
    assert nano_cell["opponent_cost"] == pytest.approx(0.024)

    haiku_all = report["total_rows"]["all"][0]
    assert haiku_all["games"] == 3
    assert haiku_all["record"] == "1-1-1"
    assert haiku_all["avg_plies"] == 8
    assert haiku_all["avg_calls"] == 2
    assert haiku_all["avg_tokens"] == 180
    assert haiku_all["avg_input_tokens"] == 130
    assert haiku_all["avg_cache_tokens"] == 20
    assert haiku_all["avg_output_tokens"] == 30
    assert haiku_all["avg_cost"] == pytest.approx(0.015)
    assert haiku_all["usage_eligible_games"] == 3
    assert haiku_all["usage_recorded_games"] == 1
    assert report["total_rows"]["bots"][0]["games"] == 2
    assert report["total_rows"]["humans"][0]["record"] == "0-0-1"
    assert report["usage_available"] is True
    assert report["usage_start_date"] == "2026-07-04"

    filtered_report = await UserService(users).get_bot_matrix_report(
        FakeDB(users=users, game_archives=archives),
        period="today",
        outcomes=["insufficient"],
        now=now,
    )

    assert filtered_report["outcomes"] == ["insufficient"]
    assert filtered_report["unique_game_count"] == 1
    assert filtered_report["row_record_count"] == 2
    assert filtered_report["end_condition_rows"] == [{"condition": "insufficient", "label": "Insufficient material", "games": 1}]
    assert filtered_report["matrix_rows"][0]["cells"][1]["summary"]["record"] == "0-1-0"
    assert filtered_report["total_rows"]["all"][0]["games"] == 1
    assert filtered_report["total_rows"]["all"][0]["record"] == "0-1-0"
    assert filtered_report["total_rows"]["humans"][0]["games"] == 0


@pytest.mark.asyncio
async def test_get_bot_matrix_report_maps_generic_openrouter_usage_by_model() -> None:
    users = FakeUsersCollection()
    users.docs.extend(
        [
            {
                "_id": "haiku-id",
                "username": "llm_haiku",
                "username_display": "LLM Haiku (bot)",
                "role": "bot",
                "bot_profile": {"listed": True, "display_name": "LLM Haiku (bot)"},
            },
            {
                "_id": "llama-id",
                "username": "llm_llama31_8b",
                "username_display": "LLM Llama 3.5 8B (bot)",
                "role": "bot",
                "bot_profile": {"listed": True, "display_name": "LLM Llama 3.5 8B (bot)"},
            },
        ]
    )
    archives = FakeUsersCollection()
    archives.docs.append(
        {
            "_id": "llama-game-id",
            "state": "completed",
            "game_code": "LLAMA1",
            "updated_at": datetime(2026, 7, 5, 11, tzinfo=UTC),
            "white": {"user_id": "haiku-id", "username": "llm_haiku", "role": "bot"},
            "black": {"user_id": "llama-id", "username": "llm_llama31_8b", "role": "bot"},
            "result": {"winner": "white", "reason": "timeout"},
            "move_count": 355,
            "stats": {
                "llm_usage": {
                    "black": {
                        "user_id": "llama-id",
                        "username": "llm_llama31_8b",
                        "calls": 1,
                        "input_tokens": 1000,
                        "cached_input_tokens": 200,
                        "output_tokens": 50,
                        "total_tokens": 1050,
                        "cost_usd": 0.00123,
                    }
                }
            },
        }
    )

    report = await UserService(users).get_bot_matrix_report(
        FakeDB(users=users, game_archives=archives),
        period="lifetime",
        now=datetime(2026, 7, 5, 12, tzinfo=UTC),
    )

    assert report["unique_game_count"] == 1
    assert report["row_record_count"] == 2
    assert [player["username"] for player in report["players"]] == ["llm_haiku", "llm_llama31_8b"]

    haiku_vs_llama = report["matrix_rows"][0]["cells"][1]["summary"]
    assert haiku_vs_llama["games"] == 1
    assert haiku_vs_llama["record"] == "1-0-0"
    assert haiku_vs_llama["average_plies"] == 355
    assert haiku_vs_llama["player_tokens"] is None
    assert haiku_vs_llama["player_cost"] is None
    assert haiku_vs_llama["opponent_usage_eligible_games"] == 1
    assert haiku_vs_llama["opponent_usage_recorded_games"] == 1
    assert haiku_vs_llama["opponent_tokens"] == 1050
    assert haiku_vs_llama["opponent_input_tokens"] == 800
    assert haiku_vs_llama["opponent_cache_tokens"] == 200
    assert haiku_vs_llama["opponent_output_tokens"] == 50
    assert haiku_vs_llama["opponent_cost"] == pytest.approx(0.00123)

    llama_vs_haiku = report["matrix_rows"][1]["cells"][0]["summary"]
    assert llama_vs_haiku["record"] == "0-0-1"
    assert llama_vs_haiku["usage_eligible_games"] == 1
    assert llama_vs_haiku["usage_recorded_games"] == 1
    assert llama_vs_haiku["player_tokens"] == 1050
    assert llama_vs_haiku["player_input_tokens"] == 800
    assert llama_vs_haiku["player_cache_tokens"] == 200
    assert llama_vs_haiku["player_output_tokens"] == 50
    assert llama_vs_haiku["player_cost"] == pytest.approx(0.00123)
    assert llama_vs_haiku["opponent_tokens"] is None
    assert report["usage_available"] is True


@pytest.mark.asyncio
async def test_get_listed_bot_daily_report_aggregates_daily_win_rates(monkeypatch: pytest.MonkeyPatch) -> None:
    users = object()
    archives = object()
    db = FakeDB(users=users, game_archives=archives)
    service = UserService(FakeUsersCollection())
    local_tz = ZoneInfo("America/New_York")
    now_local = datetime.now(local_tz)
    midday_local = now_local.replace(hour=12, minute=0, second=0, microsecond=0)
    if midday_local > now_local:
        midday_local -= timedelta(days=1)
    previous_midday_local = midday_local - timedelta(days=1)

    listed_bot_docs = [
        {"username": "llm_haiku"},
        {"username": "llm_gptnano"},
        {"username": "   "},
    ]
    archive_docs = [
        {
            "updated_at": previous_midday_local.astimezone(UTC),
            "white": {"username": "llm_gptnano", "role": "bot"},
            "black": {"username": "humanone", "role": "user"},
            "result": {"winner": "white"},
        },
        {
            "updated_at": midday_local.astimezone(UTC).replace(tzinfo=None),
            "white": {"username": "llm_gptnano", "role": "bot"},
            "black": {"username": "llm_haiku", "role": "bot"},
            "result": {"winner": "black"},
        },
        {
            "updated_at": "bad-timestamp",
            "white": {"username": "llm_gptnano", "role": "bot"},
            "black": {"username": "llm_haiku", "role": "bot"},
            "result": {"winner": "white"},
        },
        {
            "updated_at": (previous_midday_local - timedelta(days=30)).astimezone(UTC),
            "white": {"username": "llm_gptnano", "role": "bot"},
            "black": {"username": "human", "role": "user"},
            "result": {"winner": "white"},
        },
    ]

    def fake_find(collection, query, projection=None):  # noqa: ANN001
        if collection is users:
            assert query == {"role": "bot", "bot_profile.listed": True}
            return FakeCursor(listed_bot_docs)
        assert collection is archives
        return FakeCursor(archive_docs)

    monkeypatch.setattr(service, "_find", fake_find)

    report = await service.get_listed_bot_daily_report(db, days=3, timezone_name="America/New_York")

    assert report["timezone"] == "America/New_York"
    assert [bot["username"] for bot in report["bots"]] == ["llm_gptnano", "llm_haiku"]
    assert len(report["bots"][0]["rows"]) == 3

    gpt_rows = report["bots"][0]["rows"]
    haiku_rows = report["bots"][1]["rows"]
    assert sum(row["stats"]["overall"]["total_games"] for row in gpt_rows) == 2
    assert sum(row["stats"]["overall"]["wins"] for row in gpt_rows) == 1
    assert any(row["stats"]["vs_humans"] == {"total_games": 1, "wins": 1, "win_rate": 1.0} for row in gpt_rows)
    assert sum(row["stats"]["vs_bots"]["total_games"] for row in haiku_rows) == 1
    assert sum(row["stats"]["vs_bots"]["wins"] for row in haiku_rows) == 1
