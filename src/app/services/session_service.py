from __future__ import annotations

import secrets
from datetime import UTC, datetime, timedelta
from typing import Any

from bson import ObjectId

from app.models.user import UserModel


class SessionService:
    COOKIE_NAME = "session_id"
    SESSION_MAX_AGE_SECONDS = 60 * 60 * 24 * 30
    GUEST_SESSION_MAX_AGE_SECONDS = 60 * 60 * 24 * 365

    def __init__(self, sessions_collection: Any):
        self._sessions = sessions_collection

    @classmethod
    def utcnow(cls) -> datetime:
        return datetime.now(UTC)

    @classmethod
    def generate_session_id(cls) -> str:
        return secrets.token_hex(32)

    @classmethod
    def expires_at(cls, now: datetime | None = None, *, max_age_seconds: int | None = None) -> datetime:
        ref = now or cls.utcnow()
        return ref + timedelta(seconds=max_age_seconds or cls.SESSION_MAX_AGE_SECONDS)

    @classmethod
    def max_age_seconds_for_user(cls, user: UserModel) -> int:
        return cls.GUEST_SESSION_MAX_AGE_SECONDS if user.role == "guest" else cls.SESSION_MAX_AGE_SECONDS

    @classmethod
    def max_age_seconds_for_session(cls, session: dict[str, Any]) -> int:
        value = session.get("max_age_seconds")
        return value if isinstance(value, int) and value > 0 else cls.SESSION_MAX_AGE_SECONDS

    async def create_session(self, *, user: UserModel, ip: str | None, user_agent: str | None) -> str:
        now = self.utcnow()
        session_id = self.generate_session_id()
        max_age_seconds = self.max_age_seconds_for_user(user)
        await self._sessions.insert_one(
            {
                "_id": session_id,
                "user_id": ObjectId(user.id),
                "username": user.username,
                "ip": ip,
                "user_agent": user_agent,
                "created_at": now,
                "expires_at": self.expires_at(now, max_age_seconds=max_age_seconds),
                "max_age_seconds": max_age_seconds,
            }
        )
        return session_id

    async def delete_session(self, session_id: str) -> None:
        await self._sessions.delete_one({"_id": session_id})

    async def get_active_session(self, session_id: str) -> dict[str, Any] | None:
        session = await self._sessions.find_one({"_id": session_id})
        if session is None:
            return None

        now = self.utcnow()
        expires_at = session["expires_at"]
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        else:
            expires_at = expires_at.astimezone(UTC)

        if expires_at <= now:
            await self.delete_session(session_id)
            return None

        max_age_seconds = self.max_age_seconds_for_session(session)
        next_expires_at = self.expires_at(now, max_age_seconds=max_age_seconds)
        await self._sessions.update_one(
            {"_id": session_id},
            {"$set": {"expires_at": next_expires_at, "max_age_seconds": max_age_seconds}},
        )
        session["expires_at"] = next_expires_at
        session["max_age_seconds"] = max_age_seconds
        return session
