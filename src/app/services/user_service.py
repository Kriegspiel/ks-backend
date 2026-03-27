from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import bcrypt
from bson import ObjectId
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from app.models.auth import RegisterRequest
from app.models.user import UserModel, utcnow


class UserConflictError(Exception):
    def __init__(self, *, field: str, code: str, message: str):
        self.field = field
        self.code = code
        super().__init__(message)


class UserService:
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

    async def authenticate(self, username: str, password: str) -> UserModel | None:
        canonical_username = self.canonical_username(username)
        user = await self._users.find_one({"username": canonical_username})
        if user is None:
            return None

        if not self.verify_password(password, user["password_hash"]):
            return None

        return UserModel.from_mongo(user)

    async def get_public_profile(self, db: Any, username: str) -> dict[str, Any] | None:
        canonical = self.canonical_username(username)
        user = await db.users.find_one({"username": canonical})
        if user is None:
            return None

        return {
            "username": user.get("username"),
            "profile": user.get("profile", {}),
            "stats": user.get("stats", {}),
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
            winner = game.get("result", {}).get("winner")
            games.append(
                {
                    "game_id": str(game.get("_id")),
                    "opponent": opponent.get("username") if isinstance(opponent, dict) else None,
                    "play_as": play_as,
                    "result": self._winner_result(winner, play_as),
                    "reason": game.get("result", {}).get("reason"),
                    "move_count": len(game.get("moves", [])),
                    "played_at": self._safe_datetime(game.get("updated_at") or game.get("created_at")),
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

        query = {"status": "active", "stats.games_played": {"$gte": 5}}
        total = await db.users.count_documents(query)
        cursor = db.users.find(query).sort([("stats.elo", -1), ("username", 1)]).skip(offset).limit(bounded_per_page)

        players: list[dict[str, Any]] = []
        rank = offset + 1
        async for user in cursor:
            stats = user.get("stats", {})
            games_played = int(stats.get("games_played", 0))
            games_won = int(stats.get("games_won", 0))
            players.append(
                {
                    "rank": rank,
                    "username": user.get("username"),
                    "elo": int(stats.get("elo", 1200)),
                    "games_played": games_played,
                    "win_rate": round((games_won / games_played) if games_played else 0.0, 4),
                }
            )
            rank += 1

        return players, total
