from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from bson import ObjectId
from pymongo import ReturnDocument

from app.models.bot import BotListItem, BotListResponse, BotUsageReportRequest, supported_rule_variants_for_bot
from app.models.user import normalize_user_stats_payload
from app.llm_bot_policy import (
    is_llm_bot_document,
    llm_bot_limit_label_for_tier,
    llm_bot_ply_limit_for_tier,
    normalize_llm_bot_tier,
    tier_allows_llm_bots,
)


MODEL_AVAILABILITY_REQUIRED_BOTS = {
    "llm_gptnano": "openai",
    "llm_haiku": "anthropic",
}
MODEL_AVAILABILITY_STALE_AFTER = timedelta(seconds=120)


class BotService:
    def __init__(
        self,
        users_collection: Any,
        *,
        usage_collection: Any | None = None,
        now_factory: Callable[[], datetime] | None = None,
    ):
        self._users = users_collection
        self._usage = usage_collection
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

    async def list_bots(self, *, viewer_role: str = "user", viewer_llm_bot_tier: str | None = None) -> BotListResponse:
        cursor = self._users.find({"role": "bot", "status": "active"}).sort("username", 1)
        bots: list[BotListItem] = []
        now = self._now_factory()
        tier = normalize_llm_bot_tier(viewer_llm_bot_tier, role=viewer_role)
        async for doc in cursor:
            profile = doc.get("bot_profile") or {}
            if profile.get("listed", True) is False:
                continue
            if not self.bot_can_start_games(doc, now=now):
                continue
            llm_backed = is_llm_bot_document(doc)
            if llm_backed and not tier_allows_llm_bots(tier):
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
                    llm_backed=llm_backed,
                    llm_bot_tier=tier if llm_backed else None,
                    llm_bot_ply_limit=llm_bot_ply_limit_for_tier(tier) if llm_backed else None,
                    llm_bot_limit_label=llm_bot_limit_label_for_tier(tier) if llm_backed else None,
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

    async def record_usage(self, *, user_id: str, username: str, payload: BotUsageReportRequest) -> bool:
        if self._usage is None:
            return False

        now = self._now_factory()
        response_id = payload.response_id.strip() if isinstance(payload.response_id, str) else None
        total_tokens = int(payload.total_tokens or 0)
        if total_tokens <= 0:
            total_tokens = (
                int(payload.input_tokens)
                + int(payload.output_tokens)
                + int(payload.cache_read_input_tokens)
                + int(payload.cache_creation_input_tokens)
            )

        game_code = (
            payload.game_code.strip().upper()
            if isinstance(payload.game_code, str) and payload.game_code.strip()
            else None
        )

        record = {
            "game_id": payload.game_id.strip(),
            "game_code": game_code,
            "bot_user_id": str(user_id),
            "bot_username": username.strip(),
            "provider": payload.provider.strip().lower(),
            "model": payload.model.strip(),
            "input_tokens": int(payload.input_tokens),
            "cached_input_tokens": int(payload.cached_input_tokens),
            "output_tokens": int(payload.output_tokens),
            "cache_read_input_tokens": int(payload.cache_read_input_tokens),
            "cache_creation_input_tokens": int(payload.cache_creation_input_tokens),
            "total_tokens": total_tokens,
            "cost_usd": float(payload.cost_usd),
            "recorded_at": now,
            "updated_at": now,
        }
        if response_id:
            record["response_id"] = response_id

        if response_id:
            await self._usage.update_one(
                {"bot_user_id": str(user_id), "response_id": response_id},
                {
                    "$set": record,
                    "$setOnInsert": {"created_at": now},
                },
                upsert=True,
            )
        else:
            record["created_at"] = now
            await self._usage.insert_one(record)
        return True

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
