from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from bson import ObjectId
from pymongo import ReturnDocument

from app.models.bot import BotListItem, BotListResponse, supported_rule_variants_for_bot
from app.models.user import normalize_user_stats_payload


MODEL_AVAILABILITY_REQUIRED_BOTS = {
    "gptnano": "openai",
    "haiku": "anthropic",
}
MODEL_AVAILABILITY_STALE_AFTER = timedelta(seconds=120)


class BotService:
    def __init__(self, users_collection: Any, *, now_factory: Callable[[], datetime] | None = None):
        self._users = users_collection
        self._now_factory = now_factory or (lambda: datetime.now(UTC))

    @staticmethod
    def _normalize_utc_datetime(value: datetime | None) -> datetime | None:
        if not isinstance(value, datetime):
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    @staticmethod
    def _supported_rule_variants(doc: dict[str, Any]) -> list[str]:
        profile = doc.get("bot_profile") or {}
        return supported_rule_variants_for_bot(str(doc.get("username") or ""), profile.get("supported_rule_variants"))

    @staticmethod
    def _active_bot_queries(user_id: str) -> list[dict[str, Any]]:
        queries: list[dict[str, Any]] = []
        try:
            queries.append({"_id": ObjectId(user_id), "role": "bot", "status": "active"})
        except Exception:
            pass
        queries.append({"_id": user_id, "role": "bot", "status": "active"})
        return queries

    @classmethod
    def model_availability_required_provider(cls, doc: dict[str, Any]) -> str | None:
        username = str(doc.get("username") or "").strip().lower()
        return MODEL_AVAILABILITY_REQUIRED_BOTS.get(username)

    @classmethod
    def bot_can_start_games(cls, doc: dict[str, Any], *, now: datetime | None = None) -> bool:
        provider = cls.model_availability_required_provider(doc)
        if provider is None:
            return True

        profile = doc.get("bot_profile") or {}
        availability = profile.get("model_availability") if isinstance(profile, dict) else None
        if not isinstance(availability, dict):
            return False
        if str(availability.get("provider") or "").strip().lower() != provider:
            return False
        if availability.get("ready") is not True:
            return False

        checked_at = cls._normalize_utc_datetime(availability.get("checked_at"))
        if checked_at is None:
            return False
        current = cls._normalize_utc_datetime(now) or datetime.now(UTC)
        return current - checked_at <= MODEL_AVAILABILITY_STALE_AFTER

    async def list_bots(self) -> BotListResponse:
        cursor = self._users.find({"role": "bot", "status": "active"}).sort("username", 1)
        bots: list[BotListItem] = []
        now = self._now_factory()
        async for doc in cursor:
            profile = doc.get("bot_profile") or {}
            if profile.get("listed", True) is False:
                continue
            if not self.bot_can_start_games(doc, now=now):
                continue
            stats = normalize_user_stats_payload(doc.get("stats"))
            bots.append(
                BotListItem(
                    bot_id=str(doc["_id"]),
                    username=doc["username"],
                    display_name=profile.get("display_name") or doc.get("username_display") or doc["username"],
                    description=profile.get("description") or "",
                    elo=int(stats.get("elo", 1200)),
                    ratings=stats.get("ratings", {}),
                    supported_rule_variants=self._supported_rule_variants(doc),
                )
            )
        return BotListResponse(bots=bots)

    async def get_bot_by_id(self, bot_id: str) -> dict[str, Any] | None:
        from bson import ObjectId

        try:
            oid = ObjectId(bot_id)
        except Exception:
            return None
        return await self._users.find_one({"_id": oid, "role": "bot", "status": "active"})

    async def report_model_availability(
        self,
        *,
        user_id: str,
        provider: str,
        ready: bool,
        reason: str,
    ) -> dict[str, Any] | None:
        now = self._now_factory()
        availability = {
            "provider": provider,
            "ready": bool(ready),
            "reason": str(reason or "")[:500],
            "checked_at": now,
        }
        update = {"$set": {"bot_profile.model_availability": availability, "updated_at": now}}

        for query in self._active_bot_queries(user_id):
            updated = await self._users.find_one_and_update(query, update, return_document=ReturnDocument.AFTER)
            if updated is not None:
                return updated
        return None

    async def sync_supported_rule_variants(
        self,
        *,
        user_id: str,
        supported_rule_variants: list[str],
    ) -> dict[str, Any] | None:
        now = self._now_factory()
        update = {
            "$set": {
                "bot_profile.supported_rule_variants": list(supported_rule_variants),
                "updated_at": now,
            }
        }

        for query in self._active_bot_queries(user_id):
            updated = await self._users.find_one_and_update(query, update, return_document=ReturnDocument.AFTER)
            if updated is not None:
                return updated
        return None
