from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import bcrypt
import pytest
from bson import ObjectId
from pymongo.errors import DuplicateKeyError

from app.models.auth import BotRegisterRequest, RegisterRequest
from app.models.user import UserModel, default_user_stats_payload
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

    def find(self, query: dict, projection: dict | None = None):
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


class FakeDB:
    def __init__(self, users: FakeUsersCollection, game_archives: FakeUsersCollection):
        self.users = users
        self.game_archives = game_archives


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
    assert profile["stats"]["elo"] == 1337
    assert profile["stats"]["ratings"]["overall"]["elo"] == 1337
    assert bot_profile is not None
    assert bot_profile["owner_email"] == "bot-random-any@kriegspiel.org"
    assert missing is None


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
                    "overall": {"white_before": 1200, "white_after": 1216, "white_delta": 16, "black_before": 1200, "black_after": 1184, "black_delta": -16},
                    "specific": {"white_before": 1200, "white_after": 1216, "white_delta": 16, "black_before": 1200, "black_after": 1184, "black_delta": -16},
                    "white_track": "vs_bots",
                    "black_track": "vs_humans",
                },
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
                "moves": [{"move_done": True}, {"move_done": True}],
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
            "moves": [{"move_done": True}, {"move_done": True}, {"move_done": True}],
            "created_at": datetime(2026, 3, 10, tzinfo=UTC),
            "updated_at": datetime(2026, 3, 10, tzinfo=UTC),
        }
    )
    db = FakeDB(users=users, game_archives=archives)
    service = UserService(users)

    page, total = await service.get_game_history(db, str(user_id), page=1, per_page=10)

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
            "white": {"user_id": str(user_id), "username": "gptnano", "role": "bot"},
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
            "moves": [{"move_done": True}],
            "created_at": datetime(2026, 4, 6, tzinfo=UTC),
            "updated_at": datetime(2026, 4, 6, tzinfo=UTC),
        }
    )
    db = FakeDB(users=users, game_archives=archives)
    service = UserService(users)

    page, total = await service.get_game_history(db, str(user_id), page=1, per_page=10)

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
    assert UserService._normalized_result_reason({"moves": [{"special_announcement": "CHECKMATE_BLACK_WINS"}]}) == "checkmate"


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

    report = await UserService(users).get_listed_bot_daily_report(FakeDB(users=users, game_archives=FakeUsersCollection()), days=5)

    assert report == {"timezone": "America/New_York", "bots": []}


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
        {"username": "haiku"},
        {"username": "gptnano"},
        {"username": "   "},
    ]
    archive_docs = [
        {
            "updated_at": previous_midday_local.astimezone(UTC),
            "white": {"username": "gptnano", "role": "bot"},
            "black": {"username": "humanone", "role": "user"},
            "result": {"winner": "white"},
        },
        {
            "updated_at": midday_local.astimezone(UTC).replace(tzinfo=None),
            "white": {"username": "gptnano", "role": "bot"},
            "black": {"username": "haiku", "role": "bot"},
            "result": {"winner": "black"},
        },
        {
            "updated_at": "bad-timestamp",
            "white": {"username": "gptnano", "role": "bot"},
            "black": {"username": "haiku", "role": "bot"},
            "result": {"winner": "white"},
        },
        {
            "updated_at": (previous_midday_local - timedelta(days=30)).astimezone(UTC),
            "white": {"username": "outsider", "role": "user"},
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
    assert [bot["username"] for bot in report["bots"]] == ["gptnano", "haiku"]
    assert len(report["bots"][0]["rows"]) == 3

    gpt_rows = report["bots"][0]["rows"]
    haiku_rows = report["bots"][1]["rows"]
    assert sum(row["stats"]["overall"]["total_games"] for row in gpt_rows) == 2
    assert sum(row["stats"]["overall"]["wins"] for row in gpt_rows) == 1
    assert any(row["stats"]["vs_humans"] == {"total_games": 1, "wins": 1, "win_rate": 1.0} for row in gpt_rows)
    assert sum(row["stats"]["vs_bots"]["total_games"] for row in haiku_rows) == 1
    assert sum(row["stats"]["vs_bots"]["wins"] for row in haiku_rows) == 1
