from __future__ import annotations

import secrets
from datetime import UTC, datetime, timedelta
from typing import Any

from bson import ObjectId

from app.models.user import UserModel


class SessionService:
    COOKIE_NAME = "session_id"
    SESSION_MAX_AGE_SECONDS = 60 * 60 * 24 * 30

    def __init__(self, sessions_collection: Any):
        self._sessions = sessions_collection

    @classmethod
    def utcnow(cls) -> datetime:
        return datetime.now(UTC)

    @classmethod
    def generate_session_id(cls) -> str:
        return secrets.token_hex(32)

    @classmethod
    def expires_at(cls, now: datetime | None = None) -> datetime:
        ref = now or cls.utcnow()
        return ref + timedelta(seconds=cls.SESSION_MAX_AGE_SECONDS)

    async def create_session(self, *, user: UserModel, ip: str | None, user_agent: str | None) -> str:
        now = self.utcnow()
        session_id = self.generate_session_id()
        await self._sessions.insert_one(
            {
                "_id": session_id,
                "user_id": ObjectId(user.id),
                "username": user.username,
                "ip": ip,
                "user_agent": user_agent,
                "created_at": now,
                "expires_at": self.expires_at(now),
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
        if session["expires_at"] <= now:
            await self.delete_session(session_id)
            return None

        await self._sessions.update_one({"_id": session_id}, {"$set": {"expires_at": self.expires_at(now)}})
        session["expires_at"] = self.expires_at(now)
        return session
