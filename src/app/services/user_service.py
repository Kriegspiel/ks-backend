from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import hmac
import secrets
import time
from typing import Any

import bcrypt
from bson import ObjectId
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from app.config import get_settings
from app.models.auth import BotRegisterRequest, RegisterRequest
from app.models.user import UserModel, default_user_stats_payload, normalize_user_stats_payload, utcnow


class UserConflictError(Exception):
    def __init__(self, *, field: str, code: str, message: str):
        self.field = field
        self.code = code
        super().__init__(message)


class UserService:
    _bot_token_cache: dict[str, tuple[float, UserModel]] = {}
    _bot_token_cache_ttl_seconds = get_settings().BOT_TOKEN_CACHE_TTL_SECONDS

    def __init__(self, users_collection: Any):
        self._users = users_collection

    @staticmethod
    def canonical_username(username: str) -> str:
        return username.strip().lower()

    @staticmethod
    def canonical_email(email: str) -> str:
        return email.strip().lower()

    @staticmethod
    def hash_password(plain_password: str) -> str:
        return bcrypt.hashpw(plain_password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

    @staticmethod
    def verify_password(plain_password: str, password_hash: str) -> bool:
        return bcrypt.checkpw(plain_password.encode("utf-8"), password_hash.encode("utf-8"))

    @staticmethod
    def bot_token_digest(token_secret: str) -> str:
        settings = get_settings()
        return hmac.new(
            settings.BOT_TOKEN_HMAC_SECRET.encode("utf-8"),
            token_secret.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    @classmethod
    def _cache_bot_user(cls, token: str, user: UserModel) -> None:
        cls._bot_token_cache[token] = (time.monotonic() + cls._bot_token_cache_ttl_seconds, user)

    @classmethod
    def _get_cached_bot_user(cls, token: str) -> UserModel | None:
        cached = cls._bot_token_cache.get(token)
        if cached is None:
            return None
        expires_at, user = cached
        if expires_at <= time.monotonic():
            cls._bot_token_cache.pop(token, None)
            return None
        return user

    @classmethod
    def clear_bot_token_cache(cls) -> None:
        cls._bot_token_cache.clear()

    @staticmethod
    def issue_bot_token() -> tuple[str, str, str]:
        token_id = secrets.token_hex(8)
        token_secret = secrets.token_urlsafe(24)
        token = f"ksbot_{token_id}.{token_secret}"
        token_digest = UserService.bot_token_digest(token_secret)
        return token_id, token_secret, token_digest

    @staticmethod
    def parse_bot_token(token: str) -> tuple[str, str] | None:
        if not token.startswith("ksbot_") or "." not in token:
            return None
        token_id, token_secret = token[len("ksbot_") :].split(".", 1)
        if not token_id or not token_secret:
            return None
        return token_id, token_secret

    @staticmethod
    def _safe_datetime(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        return datetime.now(UTC)

    @staticmethod
    def _to_object_id(user_id: str) -> ObjectId:
        try:
            return ObjectId(user_id)
        except Exception as exc:  # noqa: BLE001
            raise ValueError("Invalid user id") from exc

    @staticmethod
    def _winner_result(winner: str | None, play_as: str) -> str:
        if winner is None:
            return "draw"
        return "win" if winner == play_as else "loss"

    @staticmethod
    def _normalized_result_reason(game: dict[str, Any]) -> str | None:
        result = game.get("result") if isinstance(game.get("result"), dict) else {}
        reason = result.get("reason")
        if isinstance(reason, str) and reason:
            return reason

        for move in reversed(game.get("moves", [])):
            special = move.get("special_announcement")
            if special == "DRAW_INSUFFICIENT":
                return "insufficient"
            if special == "DRAW_STALEMATE":
                return "stalemate"
            if special == "DRAW_TOOMANYREVERSIBLEMOVES":
                return "too_many_reversible_moves"
            if special in {"CHECKMATE_WHITE_WINS", "CHECKMATE_BLACK_WINS"}:
                return "checkmate"
        return None

    @staticmethod
    def _completed_turn_count(game: dict[str, Any]) -> int:
        return sum(1 for move in game.get("moves", []) if move.get("move_done"))

    async def create_user(self, registration: RegisterRequest) -> UserModel:
        username = self.canonical_username(registration.username)
        email = self.canonical_email(registration.email)

        existing = await self._users.find_one({"$or": [{"username": username}, {"email": email}]})
        if existing:
            if existing.get("username") == username:
                raise UserConflictError(field="username", code="USERNAME_TAKEN", message="Username already exists")
            raise UserConflictError(field="email", code="EMAIL_TAKEN", message="Email already registered")

        now = utcnow()
        payload = {
            "username": username,
            "username_display": registration.username.strip(),
            "email": email,
            "email_verified": False,
            "email_verification_sent_at": None,
            "email_verified_at": None,
            "password_hash": self.hash_password(registration.password),
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
            "last_active_at": now,
            "created_at": now,
            "updated_at": now,
        }

        try:
            result = await self._users.insert_one(payload)
        except DuplicateKeyError as exc:
            details = str(exc)
            if "username" in details:
                raise UserConflictError(field="username", code="USERNAME_TAKEN", message="Username already exists") from exc
            raise UserConflictError(field="email", code="EMAIL_TAKEN", message="Email already registered") from exc

        payload["_id"] = result.inserted_id
        return UserModel.from_mongo(payload)

    @staticmethod
    def _default_bot_listed(*, username: str, display_name: str, description: str) -> bool:
        combined = f"{username} {display_name} {description}".lower()
        blocked_markers = ("e2e", "test", "probe")
        return not any(marker in combined for marker in blocked_markers)

    @staticmethod
    def _default_supported_rule_variants(*, username: str) -> list[str]:
        if username == "randobotany":
            return ["berkeley_any"]
        return ["berkeley", "berkeley_any"]

    async def create_bot(self, registration: BotRegisterRequest) -> tuple[UserModel, str]:
        username = self.canonical_username(registration.username)
        existing = await self._users.find_one({"username": username})
        if existing:
            raise UserConflictError(field="username", code="USERNAME_TAKEN", message="Username already exists")

        now = utcnow()
        token_id, token_secret, token_digest = self.issue_bot_token()
        token = f"ksbot_{token_id}.{token_secret}"
        owner_email = self.canonical_email(registration.owner_email)
        requested_listed = getattr(registration, "listed", None)
        listed = requested_listed if requested_listed is not None else self._default_bot_listed(
            username=username,
            display_name=registration.display_name.strip(),
            description=registration.description.strip(),
        )
        supported_rule_variants = getattr(registration, "supported_rule_variants", None) or self._default_supported_rule_variants(username=username)
        payload = {
            "username": username,
            "username_display": registration.display_name.strip(),
            "email": f"{username}@bots.kriegspiel.local",
            "email_verified": True,
            "email_verification_sent_at": None,
            "email_verified_at": now,
            "password_hash": self.hash_password(secrets.token_urlsafe(24)),
            "auth_providers": ["bot_token"],
            "profile": {"bio": registration.description.strip(), "avatar_url": None, "country": None},
            "bot_profile": {
                "display_name": registration.display_name.strip(),
                "owner_email": owner_email,
                "description": registration.description.strip(),
                "listed": listed,
                "api_token_id": token_id,
                "api_token_hash": None,
                "api_token_digest": token_digest,
                "registered_at": now,
                "supported_rule_variants": supported_rule_variants,
            },
            "stats": default_user_stats_payload(),
            "settings": {
                "board_theme": "default",
                "piece_set": "cburnett",
                "sound_enabled": False,
                "auto_ask_any": False,
            },
            "role": "bot",
            "status": "active",
            "last_active_at": now,
            "created_at": now,
            "updated_at": now,
        }

        try:
            result = await self._users.insert_one(payload)
        except DuplicateKeyError as exc:
            raise UserConflictError(field="username", code="USERNAME_TAKEN", message="Username already exists") from exc

        payload["_id"] = result.inserted_id
        return UserModel.from_mongo(payload), token

    async def authenticate(self, username: str, password: str) -> UserModel | None:
        canonical_username = self.canonical_username(username)
        user = await self._users.find_one({"username": canonical_username})
        if user is None:
            return None

        if not self.verify_password(password, user["password_hash"]):
            return None

        return UserModel.from_mongo(user)

    async def authenticate_bot_token(self, token: str) -> UserModel | None:
        cached_user = self._get_cached_bot_user(token)
        if cached_user is not None:
            return cached_user
        parsed = self.parse_bot_token(token)
        if parsed is None:
            return None
        token_id, token_secret = parsed
        user = await self._users.find_one({"role": "bot", "bot_profile.api_token_id": token_id, "status": "active"})
        if user is None:
            return None
        bot_profile = user.get("bot_profile", {})
        token_digest = bot_profile.get("api_token_digest")
        computed_digest = self.bot_token_digest(token_secret)
        if token_digest:
            if not hmac.compare_digest(computed_digest, token_digest):
                return None
            authenticated = UserModel.from_mongo(user)
            self._cache_bot_user(token, authenticated)
            return authenticated

        token_hash = bot_profile.get("api_token_hash")
        if not token_hash or not self.verify_password(token_secret, token_hash):
            return None

        now = utcnow()
        updated_user = await self._users.find_one_and_update(
            {"_id": user["_id"]},
            {
                "$set": {
                    "bot_profile.api_token_digest": computed_digest,
                    "updated_at": now,
                },
                "$unset": {"bot_profile.api_token_hash": ""},
            },
            return_document=ReturnDocument.AFTER,
        )
        authenticated = UserModel.from_mongo(updated_user or user)
        self._cache_bot_user(token, authenticated)
        return authenticated

    async def get_public_profile(self, db: Any, username: str) -> dict[str, Any] | None:
        canonical = self.canonical_username(username)
        user = await db.users.find_one({"username": canonical})
        if user is None:
            return None

        bot_profile = user.get("bot_profile") or {}
        display_name = (
            bot_profile.get("display_name")
            or user.get("username_display")
            or user.get("username")
        )

        return {
            "username": user.get("username"),
            "display_name": display_name,
            "role": user.get("role", "user"),
            "is_bot": user.get("role") == "bot",
            "profile": user.get("profile", {}),
            "stats": normalize_user_stats_payload(user.get("stats")),
            "member_since": self._safe_datetime(user.get("created_at")),
        }

    async def get_game_history(self, db: Any, user_id: str, page: int, per_page: int) -> tuple[list[dict[str, Any]], int]:
        bounded_page = max(page, 1)
        bounded_per_page = min(max(per_page, 1), 100)
        offset = (bounded_page - 1) * bounded_per_page

        query = {
            "$or": [
                {"white.user_id": user_id},
                {"black.user_id": user_id},
            ]
        }

        total = await db.game_archives.count_documents(query)
        cursor = db.game_archives.find(query).sort("created_at", -1).skip(offset).limit(bounded_per_page)
        games: list[dict[str, Any]] = []
        async for game in cursor:
            play_as = "white" if game.get("white", {}).get("user_id") == user_id else "black"
            opponent = game.get("black") if play_as == "white" else game.get("white")
            result = game.get("result") if isinstance(game.get("result"), dict) else {}
            winner = result.get("winner")
            rating_snapshot = game.get("rating_snapshot") if isinstance(game.get("rating_snapshot"), dict) else {}
            prefix = "white" if play_as == "white" else "black"
            overall_snapshot = rating_snapshot.get("overall") if isinstance(rating_snapshot.get("overall"), dict) else rating_snapshot
            games.append(
                {
                    "game_id": str(game.get("_id")),
                    "game_code": game.get("game_code"),
                    "opponent": opponent.get("username") if isinstance(opponent, dict) else None,
                    "opponent_role": opponent.get("role") if isinstance(opponent, dict) else None,
                    "play_as": play_as,
                    "result": self._winner_result(winner, play_as),
                    "reason": self._normalized_result_reason(game),
                    "move_count": len(game.get("moves", [])),
                    "turn_count": self._completed_turn_count(game),
                    "played_at": self._safe_datetime(game.get("updated_at") or game.get("created_at")),
                    "elo_before": overall_snapshot.get(f"{prefix}_before"),
                    "elo_after": overall_snapshot.get(f"{prefix}_after"),
                    "elo_delta": overall_snapshot.get(f"{prefix}_delta"),
                    "rating_snapshot": rating_snapshot,
                }
            )

        return games, total

    async def update_settings(self, db: Any, user_id: str, settings: dict[str, Any]) -> dict[str, Any]:
        update_fields = {f"settings.{key}": value for key, value in settings.items()}
        update_fields["updated_at"] = utcnow()

        updated = await db.users.find_one_and_update(
            {"_id": self._to_object_id(user_id)},
            {"$set": update_fields},
            return_document=ReturnDocument.AFTER,
        )
        if updated is None:
            raise ValueError("User not found")
        return updated.get("settings", {})

    async def get_leaderboard(self, db: Any, page: int, per_page: int) -> tuple[list[dict[str, Any]], int]:
        bounded_page = max(page, 1)
        bounded_per_page = min(max(per_page, 1), 100)
        offset = (bounded_page - 1) * bounded_per_page

        query = {
            "status": "active",
            "$or": [
                {"role": {"$ne": "bot"}, "stats.games_played": {"$gte": 5}},
                {"role": "bot", "bot_profile.listed": True},
            ],
        }
        total = await db.users.count_documents(query)
        cursor = db.users.find(query).sort([("stats.elo", -1), ("username", 1)]).skip(offset).limit(bounded_per_page)

        players: list[dict[str, Any]] = []
        rank = offset + 1
        async for user in cursor:
            stats = normalize_user_stats_payload(user.get("stats"))
            games_played = int(stats.get("games_played", 0))
            games_won = int(stats.get("games_won", 0))
            ratings = stats.get("ratings", {})
            bot_profile = user.get("bot_profile") or {}
            players.append(
                {
                    "rank": rank,
                    "username": user.get("username"),
                    "display_name": bot_profile.get("display_name") or user.get("username_display") or user.get("username"),
                    "role": user.get("role", "user"),
                    "is_bot": user.get("role") == "bot",
                    "profile_path": f"/players/{user.get('username')}",
                    "elo": int(stats.get("elo", 1200)),
                    "ratings": ratings,
                    "games_played": games_played,
                    "win_rate": round((games_won / games_played) if games_played else 0.0, 4),
                }
            )
            rank += 1

        return players, total
