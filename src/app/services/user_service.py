from __future__ import annotations

from datetime import UTC, datetime, timedelta
import hashlib
import hmac
import math
import secrets
import time
from typing import Any
from zoneinfo import ZoneInfo

import bcrypt
from bson import ObjectId
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from app.config import get_settings
from app.models.auth import BotRegisterRequest, RegisterRequest
from app.models.bot import supported_rule_variants_for_bot
from app.models.user import UserModel, default_user_stats_payload, normalize_user_stats_payload, utcnow
from app.services.guest_names import GUEST_FIRST_NAMES, GUEST_LAST_NAMES

DEFAULT_BOT_OWNER_EMAIL = "bots@kriegspiel.org"
PASSWORD_HASH_SCHEME_BCRYPT_SHA256 = "bcrypt_sha256$"


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
    def _password_material(plain_password: str) -> bytes:
        # Pre-hash so longer passwords behave predictably before bcrypt.
        return hashlib.sha256(plain_password.encode("utf-8")).hexdigest().encode("ascii")

    @classmethod
    def _uses_password_prehash(cls, password_hash: str) -> bool:
        return password_hash.startswith(PASSWORD_HASH_SCHEME_BCRYPT_SHA256)

    @classmethod
    def hash_password(cls, plain_password: str) -> str:
        bcrypt_hash = bcrypt.hashpw(cls._password_material(plain_password), bcrypt.gensalt()).decode("utf-8")
        return f"{PASSWORD_HASH_SCHEME_BCRYPT_SHA256}{bcrypt_hash}"

    @classmethod
    def verify_password(cls, plain_password: str, password_hash: str) -> bool:
        candidate = plain_password.encode("utf-8")
        bcrypt_hash = password_hash
        if cls._uses_password_prehash(password_hash):
            candidate = cls._password_material(plain_password)
            bcrypt_hash = password_hash[len(PASSWORD_HASH_SCHEME_BCRYPT_SHA256) :]
        try:
            return bcrypt.checkpw(candidate, bcrypt_hash.encode("utf-8"))
        except ValueError:
            return False

    @classmethod
    def needs_password_rehash(cls, password_hash: str) -> bool:
        return not cls._uses_password_prehash(password_hash)

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
    def _optional_datetime(value: Any) -> datetime | None:
        if not isinstance(value, datetime):
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value

    @classmethod
    def _created_day(cls, doc: dict[str, Any]) -> str | None:
        created_at = cls._optional_datetime(doc.get("created_at"))
        if created_at is None:
            created_at = cls._optional_datetime(getattr(doc.get("_id"), "generation_time", None))
        return created_at.date().isoformat() if created_at is not None else None

    @staticmethod
    def _find(collection: Any, query: dict[str, Any], projection: dict[str, Any] | None = None):
        if projection is None:
            return collection.find(query)
        try:
            return collection.find(query, projection)
        except TypeError:
            return collection.find(query)

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
        completed_plies = 0
        for move in game.get("moves", []):
            if isinstance(move, dict):
                completed_plies += 1 if move.get("move_done") else 0
            else:
                completed_plies += 1
        return math.ceil(completed_plies / 2)

    @staticmethod
    def _result_track_template() -> dict[str, dict[str, int]]:
        return {
            "overall": {"games_played": 0, "games_won": 0, "games_lost": 0, "games_drawn": 0},
            "vs_humans": {"games_played": 0, "games_won": 0, "games_lost": 0, "games_drawn": 0},
            "vs_bots": {"games_played": 0, "games_won": 0, "games_lost": 0, "games_drawn": 0},
        }

    @classmethod
    def _increment_result_track(cls, results: dict[str, dict[str, int]], track: str, outcome: str) -> None:
        bucket = results[track]
        bucket["games_played"] += 1
        if outcome == "win":
            bucket["games_won"] += 1
        elif outcome == "loss":
            bucket["games_lost"] += 1
        else:
            bucket["games_drawn"] += 1

    @staticmethod
    def _track_for_opponent_role(opponent_role: str | None) -> str:
        return "vs_bots" if str(opponent_role or "user").lower() == "bot" else "vs_humans"

    @staticmethod
    def _history_rating_snapshot_for_player(game: dict[str, Any], *, play_as: str) -> tuple[dict[str, Any], dict[str, Any]]:
        rating_snapshot = game.get("rating_snapshot") if isinstance(game.get("rating_snapshot"), dict) else {}
        prefix = "white" if play_as == "white" else "black"
        overall_snapshot = rating_snapshot.get("overall") if isinstance(rating_snapshot.get("overall"), dict) else {}
        specific_snapshot = rating_snapshot.get("specific") if isinstance(rating_snapshot.get("specific"), dict) else {}
        specific_track = rating_snapshot.get(f"{prefix}_track")

        normalized = {
            "overall": {
                "elo_before": overall_snapshot.get(f"{prefix}_before"),
                "elo_after": overall_snapshot.get(f"{prefix}_after"),
                "elo_delta": overall_snapshot.get(f"{prefix}_delta"),
            },
            "vs_humans": {"elo_before": None, "elo_after": None, "elo_delta": None},
            "vs_bots": {"elo_before": None, "elo_after": None, "elo_delta": None},
        }

        if specific_track in {"vs_humans", "vs_bots"}:
            normalized[specific_track] = {
                "elo_before": specific_snapshot.get(f"{prefix}_before"),
                "elo_after": specific_snapshot.get(f"{prefix}_after"),
                "elo_delta": specific_snapshot.get(f"{prefix}_delta"),
            }

        return normalized, overall_snapshot

    async def _compute_result_tracks(self, db: Any, user_id: str) -> dict[str, dict[str, int]]:
        query = {"$or": [{"white.user_id": user_id}, {"black.user_id": user_id}]}
        cursor = self._find(
            db.game_archives,
            query,
            {
                "white": 1,
                "black": 1,
                "result": 1,
            },
        )
        results = self._result_track_template()
        async for game in cursor:
            play_as = "white" if game.get("white", {}).get("user_id") == user_id else "black"
            opponent = game.get("black") if play_as == "white" else game.get("white")
            outcome = self._winner_result((game.get("result") or {}).get("winner"), play_as)
            self._increment_result_track(results, "overall", outcome)
            self._increment_result_track(results, self._track_for_opponent_role((opponent or {}).get("role")), outcome)
        return results

    async def _ensure_result_tracks(self, db: Any, user: dict[str, Any]) -> dict[str, Any]:
        stats = normalize_user_stats_payload(user.get("stats"))
        raw_stats = user.get("stats") if isinstance(user.get("stats"), dict) else {}
        raw_results = raw_stats.get("results") if isinstance(raw_stats.get("results"), dict) else None
        result_keys = ("overall", "vs_humans", "vs_bots")
        has_results_shape = raw_results is not None and all(isinstance(raw_results.get(key), dict) for key in result_keys)
        if has_results_shape and raw_stats.get("results_synced_at"):
            user["stats"] = stats
            return user

        computed_results = await self._compute_result_tracks(db, str(user["_id"]))
        stats["results"] = computed_results
        stored_summary = {
            "games_played": int(raw_stats.get("games_played", 0)),
            "games_won": int(raw_stats.get("games_won", 0)),
            "games_lost": int(raw_stats.get("games_lost", 0)),
            "games_drawn": int(raw_stats.get("games_drawn", 0)),
        }
        if any(stored_summary.values()):
            stats["results"]["overall"] = stored_summary
        else:
            overall_results = computed_results["overall"]
            stats["games_played"] = int(overall_results.get("games_played", 0))
            stats["games_won"] = int(overall_results.get("games_won", 0))
            stats["games_lost"] = int(overall_results.get("games_lost", 0))
            stats["games_drawn"] = int(overall_results.get("games_drawn", 0))
        updated = await db.users.find_one_and_update(
            {"_id": user["_id"]},
            {"$set": {"stats": {**stats, "results_synced_at": utcnow()}, "updated_at": utcnow()}},
            return_document=ReturnDocument.AFTER,
        )
        normalized_user = dict(updated or user)
        normalized_user["stats"] = normalize_user_stats_payload(normalized_user.get("stats"))
        return normalized_user

    @staticmethod
    def _aggregate_series(points: list[dict[str, Any]], *, limit: int, label_key: str) -> list[dict[str, Any]]:
        if len(points) <= limit:
            return points

        bucket_size = math.ceil(len(points) / limit)
        aggregated: list[dict[str, Any]] = []
        previous_elo: int | None = None
        for start in range(0, len(points), bucket_size):
            bucket = points[start : start + bucket_size]
            first = bucket[0]
            last = bucket[-1]
            label = last[label_key] if first[label_key] == last[label_key] else f"{first[label_key]} - {last[label_key]}"
            elo = int(last["elo"])
            delta = elo - previous_elo if previous_elo is not None else int(last.get("delta", 0))
            aggregated.append(
                {
                    "label": label,
                    "elo": elo,
                    "delta": delta,
                    "played_at": last.get("played_at"),
                    "game_number": last.get("game_number"),
                }
            )
            previous_elo = elo
        return aggregated

    async def get_rating_history(self, db: Any, user_id: str, *, track: str, limit: int = 100) -> dict[str, Any]:
        bounded_limit = min(max(limit, 10), 100)
        query = {"$or": [{"white.user_id": user_id}, {"black.user_id": user_id}]}
        cursor = self._find(
            db.game_archives,
            query,
            {
                "white": 1,
                "black": 1,
                "result": 1,
                "updated_at": 1,
                "created_at": 1,
                "rating_snapshot": 1,
            },
        ).sort("created_at", 1)

        base_points: list[dict[str, Any]] = []
        async for game in cursor:
            play_as = "white" if game.get("white", {}).get("user_id") == user_id else "black"
            opponent = game.get("black") if play_as == "white" else game.get("white")
            if track != "overall" and self._track_for_opponent_role((opponent or {}).get("role")) != track:
                continue
            snapshots, overall_snapshot = self._history_rating_snapshot_for_player(game, play_as=play_as)
            selected_snapshot = snapshots.get(track) or {}
            elo_after = selected_snapshot.get("elo_after") if track != "overall" else selected_snapshot.get("elo_after", overall_snapshot.get(f"{play_as}_after"))
            elo_delta = selected_snapshot.get("elo_delta") if track != "overall" else selected_snapshot.get("elo_delta", overall_snapshot.get(f"{play_as}_delta"))
            if not isinstance(elo_after, (int, float)):
                continue
            played_at = self._safe_datetime(game.get("updated_at") or game.get("created_at"))
            base_points.append(
                {
                    "elo": int(elo_after),
                    "delta": int(elo_delta or 0),
                    "played_at": played_at.isoformat(),
                    "date_label": played_at.date().isoformat(),
                    "game_number": len(base_points) + 1,
                    "game_label": f"Game {len(base_points) + 1}",
                }
            )

        game_points = [
            {
                "label": point["game_label"],
                "elo": point["elo"],
                "delta": point["delta"],
                "played_at": point["played_at"],
                "game_number": point["game_number"],
            }
            for point in base_points
        ]

        by_date: dict[str, dict[str, Any]] = {}
        for point in base_points:
            by_date[point["date_label"]] = {
                "label": point["date_label"],
                "elo": point["elo"],
                "delta": point["delta"],
                "played_at": point["played_at"],
                "game_number": point["game_number"],
            }
        date_points = list(by_date.values())
        for index, point in enumerate(date_points):
            if index > 0:
                point["delta"] = point["elo"] - date_points[index - 1]["elo"]

        return {
            "track": track,
            "series": {
                "game": self._aggregate_series(game_points, limit=bounded_limit, label_key="label"),
                "date": self._aggregate_series(date_points, limit=bounded_limit, label_key="label"),
            },
        }

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

    @classmethod
    def _guest_username_for_index(cls, index: int) -> str:
        first = GUEST_FIRST_NAMES[index % len(GUEST_FIRST_NAMES)]
        last = GUEST_LAST_NAMES[(index // len(GUEST_FIRST_NAMES)) % len(GUEST_LAST_NAMES)]
        return f"guest_{first}_{last}"

    @classmethod
    def guest_name_pool_size(cls) -> int:
        return len(GUEST_FIRST_NAMES) * len(GUEST_LAST_NAMES)

    async def create_guest_user(self) -> UserModel:
        total_names = self.guest_name_pool_size()
        start_index = secrets.randbelow(total_names)

        for offset in range(total_names):
            username = self._guest_username_for_index(start_index + offset)
            if await self._users.find_one({"username": username}) is not None:
                continue

            now = utcnow()
            payload = {
                "username": username,
                "username_display": username,
                "email": f"{username}@guests.kriegspiel.local",
                "email_verified": True,
                "email_verification_sent_at": None,
                "email_verified_at": now,
                "password_hash": self.hash_password(secrets.token_urlsafe(32)),
                "auth_providers": ["guest"],
                "profile": {"bio": "Guest player", "avatar_url": None, "country": None},
                "bot_profile": None,
                "stats": default_user_stats_payload(),
                "settings": {
                    "board_theme": "default",
                    "piece_set": "cburnett",
                    "sound_enabled": True,
                    "auto_ask_any": False,
                },
                "role": "guest",
                "status": "active",
                "last_active_at": now,
                "created_at": now,
                "updated_at": now,
            }

            try:
                result = await self._users.insert_one(payload)
            except DuplicateKeyError:
                continue

            payload["_id"] = result.inserted_id
            return UserModel.from_mongo(payload)

        raise UserConflictError(
            field="username",
            code="GUEST_NAME_POOL_EXHAUSTED",
            message="No guest names are available right now",
        )

    @staticmethod
    def _default_bot_listed(*, username: str, display_name: str, description: str) -> bool:
        combined = f"{username} {display_name} {description}".lower()
        blocked_markers = ("e2e", "test", "probe")
        return not any(marker in combined for marker in blocked_markers)

    @staticmethod
    def _default_supported_rule_variants(*, username: str) -> list[str]:
        return supported_rule_variants_for_bot(username)

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
        supported_rule_variants = getattr(registration, "supported_rule_variants", None) or self._default_supported_rule_variants(
            username=username
        )
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

        stored_hash = user["password_hash"]
        if not self.verify_password(password, stored_hash):
            return None

        if self.needs_password_rehash(stored_hash):
            updated = await self._users.find_one_and_update(
                {"_id": user["_id"]},
                {"$set": {"password_hash": self.hash_password(password), "updated_at": utcnow()}},
                return_document=ReturnDocument.AFTER,
            )
            if updated is not None:
                user = updated

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
        if not token_digest or not hmac.compare_digest(computed_digest, token_digest):
            return None

        authenticated = UserModel.from_mongo(user)
        self._cache_bot_user(token, authenticated)
        return authenticated

    async def get_public_profile(self, db: Any, username: str) -> dict[str, Any] | None:
        canonical = self.canonical_username(username)
        user = await db.users.find_one({"username": canonical})
        if user is None:
            return None
        user = await self._ensure_result_tracks(db, user)

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
            "owner_email": bot_profile.get("owner_email") or DEFAULT_BOT_OWNER_EMAIL if user.get("role") == "bot" else None,
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
            rating_snapshot, overall_snapshot = self._history_rating_snapshot_for_player(game, play_as=play_as)
            prefix = "white" if play_as == "white" else "black"
            games.append(
                {
                    "game_id": str(game.get("_id")),
                    "game_code": game.get("game_code"),
                    "rule_variant": game.get("rule_variant"),
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
                {"role": {"$nin": ["bot", "guest"]}, "stats.games_played": {"$gte": 5}},
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

    async def get_guest_report(self, db: Any) -> dict[str, Any]:
        guest_cursor = self._find(
            db.users,
            {"role": "guest"},
            {
                "_id": 1,
                "username": 1,
                "username_display": 1,
                "created_at": 1,
            },
        ).sort([("created_at", -1), ("username", 1)])

        guests_by_id: dict[str, dict[str, Any]] = {}
        guest_ids: list[str] = []
        async for user in guest_cursor:
            user_id = user.get("_id")
            username = user.get("username")
            if user_id is None or not isinstance(username, str) or not username.strip():
                continue
            user_id_string = str(user_id)
            display_name = user.get("username_display") if isinstance(user.get("username_display"), str) else username
            guests_by_id[user_id_string] = {
                "name": display_name,
                "username": username,
                "day_started": self._created_day(user),
                "last_game": None,
                "number_of_games": 0,
                "_seen_games": set(),
            }
            guest_ids.append(user_id_string)

        if not guest_ids:
            return {"guests": [], "total": 0, "available_guest_accounts": self.guest_name_pool_size()}

        query = {
            "$or": [
                {"white.user_id": {"$in": guest_ids}},
                {"black.user_id": {"$in": guest_ids}},
            ]
        }
        projection = {
            "_id": 1,
            "game_code": 1,
            "white": 1,
            "black": 1,
            "created_at": 1,
            "updated_at": 1,
        }

        for collection_name in ("game_archives", "games"):
            collection = getattr(db, collection_name, None)
            if collection is None:
                continue
            async for game in self._find(collection, query, projection):
                game_key = str(game.get("_id") or game.get("game_code") or id(game))
                played_at = self._optional_datetime(game.get("updated_at")) or self._optional_datetime(game.get("created_at"))
                for color in ("white", "black"):
                    player = game.get(color) if isinstance(game.get(color), dict) else {}
                    guest_row = guests_by_id.get(str(player.get("user_id") or ""))
                    if guest_row is None:
                        continue
                    seen_games = guest_row["_seen_games"]
                    if game_key not in seen_games:
                        seen_games.add(game_key)
                        guest_row["number_of_games"] += 1
                    last_game = guest_row["last_game"]
                    if played_at is not None and (last_game is None or played_at > last_game):
                        guest_row["last_game"] = played_at

        guests: list[dict[str, Any]] = []
        for guest_row in guests_by_id.values():
            last_game = guest_row["last_game"]
            guests.append(
                {
                    "name": guest_row["name"],
                    "username": guest_row["username"],
                    "day_started": guest_row["day_started"],
                    "last_game": last_game.isoformat() if isinstance(last_game, datetime) else None,
                    "number_of_games": guest_row["number_of_games"],
                }
            )

        total_guests = len(guests)
        return {
            "guests": guests,
            "total": total_guests,
            "available_guest_accounts": max(self.guest_name_pool_size() - total_guests, 0),
        }

    @staticmethod
    def _activity_time(game: dict[str, Any]) -> datetime | None:
        return UserService._optional_datetime(game.get("updated_at")) or UserService._optional_datetime(game.get("created_at"))

    @staticmethod
    def _activity_player_role(player: dict[str, Any], bot_usernames: set[str]) -> str:
        role = str(player.get("role") or "").strip().lower()
        username = str(player.get("username") or "").strip().lower()
        if role == "bot" or username in bot_usernames:
            return "bot"
        if role == "guest":
            return "guest"
        return "user"

    @staticmethod
    def _activity_player_key(player: dict[str, Any]) -> str | None:
        user_id = str(player.get("user_id") or "").strip()
        username = str(player.get("username") or "").strip().lower()
        if user_id:
            return f"id:{user_id}"
        if username:
            return f"username:{username}"
        return None

    @staticmethod
    def _month_start(value: datetime) -> datetime:
        return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    @classmethod
    def _shift_months(cls, value: datetime, months: int) -> datetime:
        month_index = value.year * 12 + (value.month - 1) + months
        year = month_index // 12
        month = (month_index % 12) + 1
        return value.replace(year=year, month=month)

    @classmethod
    def _activity_period_rows(cls, *, key: str, now_local: datetime) -> list[dict[str, Any]]:
        if key == "dau":
            count = 14
            start = now_local.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=count - 1)
            return [
                {
                    "label": (start + timedelta(days=index)).date().isoformat(),
                    "start": start + timedelta(days=index),
                    "end": start + timedelta(days=index + 1),
                }
                for index in range(count)
            ]
        if key == "wau":
            count = 12
            week_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=now_local.weekday())
            start = week_start - timedelta(weeks=count - 1)
            return [
                {
                    "label": (start + timedelta(weeks=index)).date().isoformat(),
                    "start": start + timedelta(weeks=index),
                    "end": start + timedelta(weeks=index + 1),
                }
                for index in range(count)
            ]

        count = 12
        start = cls._shift_months(cls._month_start(now_local), -(count - 1))
        return [
            {
                "label": cls._shift_months(start, index).strftime("%Y-%m"),
                "start": cls._shift_months(start, index),
                "end": cls._shift_months(start, index + 1),
            }
            for index in range(count)
        ]

    async def get_user_activity_report(
        self,
        db: Any,
        *,
        timezone_name: str = "America/New_York",
        now: datetime | None = None,
    ) -> dict[str, Any]:
        tz = ZoneInfo(timezone_name)
        now_local = (now or datetime.now(tz)).astimezone(tz)
        section_specs = [
            ("dau", "DAU"),
            ("wau", "WAU"),
            ("mau", "MAU"),
        ]
        sections = [
            {"key": key, "title": title, "rows": self._activity_period_rows(key=key, now_local=now_local)}
            for key, title in section_specs
        ]
        earliest_start = min(row["start"].astimezone(UTC) for section in sections for row in section["rows"])

        bot_usernames = {
            user["username"].strip().lower()
            async for user in self._find(db.users, {"role": "bot"}, {"username": 1, "_id": 0})
            if isinstance(user.get("username"), str) and user["username"].strip()
        }

        query = {
            "$or": [
                {"updated_at": {"$gte": earliest_start}},
                {"created_at": {"$gte": earliest_start}},
            ]
        }
        projection = {
            "_id": 1,
            "game_code": 1,
            "rule_variant": 1,
            "state": 1,
            "white": 1,
            "black": 1,
            "result": 1,
            "created_at": 1,
            "updated_at": 1,
            "turn_count": 1,
            "move_count": 1,
        }
        games_by_key: dict[str, dict[str, Any]] = {}
        for collection_name in ("game_archives", "games"):
            collection = getattr(db, collection_name, None)
            if collection is None:
                continue
            async for game in self._find(collection, query, projection):
                played_at = self._activity_time(game)
                if played_at is None:
                    continue
                played_at = played_at.astimezone(UTC)
                if played_at < earliest_start:
                    continue
                game_key = str(game.get("game_code") or game.get("_id") or id(game))
                previous = games_by_key.get(game_key)
                previous_played_at = self._activity_time(previous) if previous else None
                if previous_played_at is None or played_at >= previous_played_at.astimezone(UTC):
                    game["_activity_at"] = played_at
                    games_by_key[game_key] = game

        for section in sections:
            for row in section["rows"]:
                row["_game_keys"] = set()
                row["_active_users"] = set()
                row["_active_bots"] = set()

        for game_key, game in games_by_key.items():
            played_at = game["_activity_at"]
            players = [
                game.get("white") if isinstance(game.get("white"), dict) else {},
                game.get("black") if isinstance(game.get("black"), dict) else {},
            ]
            for section in sections:
                for row in section["rows"]:
                    start_utc = row["start"].astimezone(UTC)
                    end_utc = row["end"].astimezone(UTC)
                    if not (start_utc <= played_at < end_utc):
                        continue
                    row["_game_keys"].add(game_key)
                    for player in players:
                        player_key = self._activity_player_key(player)
                        if player_key is None:
                            continue
                        if self._activity_player_role(player, bot_usernames) == "bot":
                            row["_active_bots"].add(player_key)
                        else:
                            row["_active_users"].add(player_key)

        public_sections: list[dict[str, Any]] = []
        for section in sections:
            rows = []
            for row in section["rows"]:
                rows.append(
                    {
                        "label": row["label"],
                        "start": row["start"].date().isoformat(),
                        "end": row["end"].date().isoformat(),
                        "active_users": len(row["_active_users"]),
                        "active_bots": len(row["_active_bots"]),
                        "total_games": len(row["_game_keys"]),
                    }
                )
            public_sections.append({"key": section["key"], "title": section["title"], "rows": rows})

        recent_games_by_key: dict[str, dict[str, Any]] = {}
        for collection_name in ("game_archives", "games"):
            collection = getattr(db, collection_name, None)
            if collection is None:
                continue
            async for game in self._find(collection, {}, projection).sort("updated_at", -1).limit(500):
                played_at = self._activity_time(game)
                if played_at is None:
                    continue
                game_key = str(game.get("game_code") or game.get("_id") or id(game))
                previous = recent_games_by_key.get(game_key)
                previous_played_at = self._activity_time(previous) if previous else None
                played_at = played_at.astimezone(UTC)
                if previous_played_at is None or played_at >= previous_played_at.astimezone(UTC):
                    game["_activity_at"] = played_at
                    recent_games_by_key[game_key] = game

        user_games: list[dict[str, Any]] = []
        for game in sorted(recent_games_by_key.values(), key=lambda item: item["_activity_at"], reverse=True):
            white = game.get("white") if isinstance(game.get("white"), dict) else {}
            black = game.get("black") if isinstance(game.get("black"), dict) else {}
            white_role = self._activity_player_role(white, bot_usernames)
            black_role = self._activity_player_role(black, bot_usernames)
            if white_role == "bot" and black_role == "bot":
                continue
            game_code = str(game.get("game_code") or "").strip()
            user_games.append(
                {
                    "game_id": str(game.get("_id") or ""),
                    "game_code": game_code,
                    "rule_variant": game.get("rule_variant") or "berkeley_any",
                    "state": game.get("state") or "completed",
                    "white": {"username": white.get("username"), "role": white_role},
                    "black": {"username": black.get("username"), "role": black_role},
                    "result": game.get("result") or {},
                    "turn_count": game.get("turn_count"),
                    "move_count": game.get("move_count"),
                    "played_at": game["_activity_at"].isoformat(),
                    "review_path": f"/game/{game_code}/review" if game_code else None,
                }
            )
            if len(user_games) >= 100:
                break

        return {"timezone": timezone_name, "sections": public_sections, "last_games": user_games}

    async def get_listed_bot_daily_report(self, db: Any, *, days: int = 10, timezone_name: str = "America/New_York") -> dict[str, Any]:
        bounded_days = max(1, min(days, 31))
        tz = ZoneInfo(timezone_name)
        now_local = datetime.now(tz)
        start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=bounded_days - 1)
        end_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        start_utc = start_local.astimezone(UTC)
        end_utc = end_local.astimezone(UTC)

        listed_bots = sorted([
            user.get("username")
            async for user in self._find(
                db.users,
                {"role": "bot", "bot_profile.listed": True},
                {"username": 1, "_id": 0},
            )
            if isinstance(user.get("username"), str) and user["username"].strip()
        ])

        def empty_bucket() -> dict[str, dict[str, int | float]]:
            return {
                "overall": {"total_games": 0, "wins": 0, "win_rate": 0.0},
                "vs_humans": {"total_games": 0, "wins": 0, "win_rate": 0.0},
                "vs_bots": {"total_games": 0, "wins": 0, "win_rate": 0.0},
            }

        bot_rows = {
            bot: [
                {"date": (start_local + timedelta(days=day_index)).date().isoformat(), "stats": empty_bucket()}
                for day_index in range(bounded_days)
            ]
            for bot in listed_bots
        }
        bot_days = {
            bot: {row["date"]: row["stats"] for row in rows}
            for bot, rows in bot_rows.items()
        }

        if not listed_bots:
            return {"timezone": timezone_name, "bots": []}

        cursor = self._find(
            db.game_archives,
            {
                "state": "completed",
                "updated_at": {"$gte": start_utc, "$lt": end_utc},
                "$or": [
                    {"white.username": {"$in": listed_bots}},
                    {"black.username": {"$in": listed_bots}},
                ],
            },
            {
                "updated_at": 1,
                "white": 1,
                "black": 1,
                "result": 1,
            },
        )
        async for game in cursor:
            updated_at = game.get("updated_at")
            if not isinstance(updated_at, datetime):
                continue
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=UTC)
            day = updated_at.astimezone(tz).date().isoformat()
            winner = (game.get("result") or {}).get("winner")

            for color, opponent_color in (("white", "black"), ("black", "white")):
                player = game.get(color) or {}
                username = player.get("username")
                if username not in bot_days:
                    continue
                day_stats = bot_days[username].get(day)
                if day_stats is None:
                    continue
                opponent = game.get(opponent_color) or {}
                outcome = self._winner_result(winner, color)
                tracks = ("overall", self._track_for_opponent_role(opponent.get("role")))
                for track in tracks:
                    bucket = day_stats[track]
                    bucket["total_games"] += 1
                    if outcome == "win":
                        bucket["wins"] += 1

        for rows in bot_rows.values():
            for row in rows:
                for bucket in row["stats"].values():
                    total_games = int(bucket["total_games"])
                    wins = int(bucket["wins"])
                    bucket["win_rate"] = round((wins / total_games) if total_games else 0.0, 4)

        return {
            "timezone": timezone_name,
            "bots": [{"username": bot, "rows": bot_rows[bot]} for bot in listed_bots],
        }
