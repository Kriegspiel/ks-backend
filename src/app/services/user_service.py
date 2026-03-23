from __future__ import annotations

from typing import Any

import bcrypt
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
