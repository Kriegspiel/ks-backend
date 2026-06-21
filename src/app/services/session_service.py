from __future__ import annotations

import asyncio
from dataclasses import dataclass
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any

from bson import ObjectId

from app.models.user import UserModel


_CACHE_MISS = object()


@dataclass
class CachedSessionEntry:
    session: dict[str, Any] | None
    cached_until: datetime


class SessionService:
    COOKIE_NAME = "session_id"
    SESSION_MAX_AGE_SECONDS = 60 * 60 * 24 * 30
    GUEST_COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24 * 400
    GUEST_SESSION_MAX_AGE_SECONDS = 60 * 60 * 24 * 365 * 5
    SESSION_CACHE_TTL_SECONDS = 60
    NEGATIVE_SESSION_CACHE_TTL_SECONDS = 5
    SESSION_TOUCH_INTERVAL_SECONDS = 60 * 5
    SESSION_CACHE_MAX_ENTRIES = 2048

    def __init__(
        self,
        sessions_collection: Any,
        *,
        cache_ttl_seconds: int = SESSION_CACHE_TTL_SECONDS,
        negative_cache_ttl_seconds: int = NEGATIVE_SESSION_CACHE_TTL_SECONDS,
        touch_interval_seconds: int = SESSION_TOUCH_INTERVAL_SECONDS,
        cache_max_entries: int = SESSION_CACHE_MAX_ENTRIES,
    ):
        self._sessions = sessions_collection
        self._cache_ttl_seconds = max(0, cache_ttl_seconds)
        self._negative_cache_ttl_seconds = max(0, negative_cache_ttl_seconds)
        self._touch_interval_seconds = max(0, touch_interval_seconds)
        self._cache_max_entries = max(0, cache_max_entries)
        self._cache: dict[str, CachedSessionEntry] = {}
        self._cache_lock = asyncio.Lock()

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
    def cookie_max_age_seconds_for_user(cls, user: UserModel) -> int:
        return cls.GUEST_COOKIE_MAX_AGE_SECONDS if user.role == "guest" else cls.SESSION_MAX_AGE_SECONDS

    @classmethod
    def max_age_seconds_for_session(cls, session: dict[str, Any]) -> int:
        value = session.get("max_age_seconds")
        return value if isinstance(value, int) and value > 0 else cls.SESSION_MAX_AGE_SECONDS

    @classmethod
    def _normalize_datetime(cls, value: datetime | None) -> datetime | None:
        if not isinstance(value, datetime):
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    @classmethod
    def _expires_at(cls, session: dict[str, Any]) -> datetime | None:
        return cls._normalize_datetime(session.get("expires_at"))

    @classmethod
    def _last_touched_at(cls, session: dict[str, Any]) -> datetime | None:
        return (
            cls._normalize_datetime(session.get("last_touched_at"))
            or cls._normalize_datetime(session.get("updated_at"))
            or cls._normalize_datetime(session.get("created_at"))
        )

    @staticmethod
    def _copy_session(session: dict[str, Any]) -> dict[str, Any]:
        return dict(session)

    def _cache_deadline(self, session: dict[str, Any] | None, *, now: datetime) -> datetime:
        ttl_seconds = self._negative_cache_ttl_seconds if session is None else self._cache_ttl_seconds
        deadline = now + timedelta(seconds=ttl_seconds)
        if session is None:
            return deadline
        expires_at = self._expires_at(session)
        return min(deadline, expires_at) if expires_at is not None else deadline

    def _prune_cache_locked(self, *, now: datetime) -> None:
        expired_keys = [session_id for session_id, entry in self._cache.items() if entry.cached_until <= now]
        for session_id in expired_keys:
            self._cache.pop(session_id, None)

        while self._cache_max_entries and len(self._cache) >= self._cache_max_entries:
            oldest_session_id = next(iter(self._cache))
            self._cache.pop(oldest_session_id, None)

    async def _get_cached_session(self, session_id: str, *, now: datetime) -> dict[str, Any] | None | object:
        if self._cache_ttl_seconds <= 0 and self._negative_cache_ttl_seconds <= 0:
            return _CACHE_MISS

        async with self._cache_lock:
            entry = self._cache.get(session_id)
            if entry is None:
                return _CACHE_MISS
            if entry.cached_until <= now:
                self._cache.pop(session_id, None)
                return _CACHE_MISS
            if entry.session is None:
                return None
            return self._copy_session(entry.session)

    async def _store_cached_session(self, session_id: str, session: dict[str, Any] | None, *, now: datetime) -> None:
        if self._cache_max_entries <= 0:
            return
        if session is None and self._negative_cache_ttl_seconds <= 0:
            return
        if session is not None and self._cache_ttl_seconds <= 0:
            return

        async with self._cache_lock:
            self._prune_cache_locked(now=now)
            self._cache[session_id] = CachedSessionEntry(
                session=self._copy_session(session) if session is not None else None,
                cached_until=self._cache_deadline(session, now=now),
            )

    async def _evict_cached_session(self, session_id: str) -> None:
        async with self._cache_lock:
            self._cache.pop(session_id, None)

    async def clear_cache(self) -> None:
        async with self._cache_lock:
            self._cache.clear()

    def _should_touch_session(self, session: dict[str, Any], *, now: datetime) -> bool:
        if self._touch_interval_seconds <= 0:
            return True
        last_touched_at = self._last_touched_at(session)
        if last_touched_at is None:
            return True
        return now - last_touched_at >= timedelta(seconds=self._touch_interval_seconds)

    async def _touch_session(self, session_id: str, session: dict[str, Any], *, now: datetime) -> dict[str, Any] | None:
        max_age_seconds = self.max_age_seconds_for_session(session)
        next_expires_at = self.expires_at(now, max_age_seconds=max_age_seconds)
        result = await self._sessions.update_one(
            {"_id": session_id},
            {
                "$set": {
                    "expires_at": next_expires_at,
                    "max_age_seconds": max_age_seconds,
                    "last_touched_at": now,
                }
            },
        )
        if getattr(result, "matched_count", 1) == 0:
            await self._evict_cached_session(session_id)
            return None

        touched_session = self._copy_session(session)
        touched_session["expires_at"] = next_expires_at
        touched_session["max_age_seconds"] = max_age_seconds
        touched_session["last_touched_at"] = now
        await self._store_cached_session(session_id, touched_session, now=now)
        return self._copy_session(touched_session)

    async def create_session(
        self,
        *,
        user: UserModel,
        ip: str | None,
        user_agent: str | None,
        attribution: dict[str, Any] | None = None,
    ) -> str:
        now = self.utcnow()
        session_id = self.generate_session_id()
        max_age_seconds = self.max_age_seconds_for_user(user)
        document = {
            "_id": session_id,
            "user_id": ObjectId(user.id),
            "username": user.username,
            "ip": ip,
            "user_agent": user_agent,
            "created_at": now,
            "last_touched_at": now,
            "expires_at": self.expires_at(now, max_age_seconds=max_age_seconds),
            "max_age_seconds": max_age_seconds,
            "cookie_max_age_seconds": self.cookie_max_age_seconds_for_user(user),
        }
        if attribution is not None:
            document["attribution"] = dict(attribution)
        await self._sessions.insert_one(document)
        await self._store_cached_session(session_id, document, now=now)
        return session_id

    async def delete_session(self, session_id: str) -> None:
        await self._evict_cached_session(session_id)
        await self._sessions.delete_one({"_id": session_id})

    async def update_session_for_user(self, session_id: str, user: UserModel) -> None:
        now = self.utcnow()
        max_age_seconds = self.max_age_seconds_for_user(user)
        next_expires_at = self.expires_at(now, max_age_seconds=max_age_seconds)
        update = {
            "$set": {
                "user_id": ObjectId(user.id),
                "username": user.username,
                "expires_at": next_expires_at,
                "max_age_seconds": max_age_seconds,
                "cookie_max_age_seconds": self.cookie_max_age_seconds_for_user(user),
                "last_touched_at": now,
            }
        }
        result = await self._sessions.update_one(
            {"_id": session_id},
            update,
        )
        if getattr(result, "matched_count", 1) == 0:
            await self._evict_cached_session(session_id)
            return

        cached_session = {
            "_id": session_id,
            **update["$set"],
        }
        await self._store_cached_session(session_id, cached_session, now=now)

    async def get_active_session(self, session_id: str) -> dict[str, Any] | None:
        now = self.utcnow()
        cached = await self._get_cached_session(session_id, now=now)
        if cached is not _CACHE_MISS:
            if cached is None:
                return None
            expires_at = self._expires_at(cached)
            if expires_at is None or expires_at <= now:
                await self.delete_session(session_id)
                await self._store_cached_session(session_id, None, now=now)
                return None
            if self._should_touch_session(cached, now=now):
                return await self._touch_session(session_id, cached, now=now)
            return self._copy_session(cached)

        session = await self._sessions.find_one({"_id": session_id})
        if session is None:
            await self._store_cached_session(session_id, None, now=now)
            return None

        expires_at = self._expires_at(session)
        if expires_at is None or expires_at <= now:
            await self.delete_session(session_id)
            await self._store_cached_session(session_id, None, now=now)
            return None

        if self._should_touch_session(session, now=now):
            return await self._touch_session(session_id, session, now=now)

        max_age_seconds = self.max_age_seconds_for_session(session)
        session["max_age_seconds"] = max_age_seconds
        await self._store_cached_session(session_id, session, now=now)
        return self._copy_session(session)
