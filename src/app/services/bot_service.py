from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from bson import ObjectId
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from app.models.bot import BotListItem, BotListResponse, BotUsageReportRequest, supported_rule_variants_for_bot
from app.models.user import normalize_user_stats_payload
from app.llm_bot_policy import (
    bot_required_tier_for_document,
    is_llm_bot_document,
    llm_bot_limit_label_for_tier,
    llm_bot_ply_limit_for_tier,
    normalize_llm_bot_tier,
    tier_allows_bot,
)
from app.services.game_usage_stats import LlmUsageReport, store_llm_usage_in_game_stats


MODEL_AVAILABILITY_REQUIRED_BOTS = {
    "llm_gptnano": "openai",
    "llm_gpt45nano": "openai",
    "llm_haiku": "anthropic",
    "llm_deepseekv4_flash": "openai",
    "llm_gemini25_lite": "openai",
    "llm_gemini31_lite": "openai",
    "llm_gptoss120b": "openai",
    "llm_llama31_8b": "openai",
    "llm_llama4_scout": "openai",
    "llm_llama4_maverick": "openai",
    "llm_mistral_nemo": "openai",
    "llm_mistral_small32": "openai",
    "llm_mistral_large3": "openai",
    "llm_gemma3_4b": "openai",
    "llm_gemma3_27b": "openai",
    "llm_gemma4_31b": "openai",
    "llm_glm47_flash": "openai",
    "llm_glm45_air": "openai",
    "llm_nemotron_nano": "openai",
    "llm_nemotron_super": "openai",
    "llm_nemotron_ultra": "openai",
    "llm_kimi_k25": "openai",
    "llm_hermes4_70b": "openai",
    "llm_phi4": "openai",
    "llm_gpt55": "openai",
    "llm_sonnet5": "anthropic",
    "llm_gemini25_flash": "openai",
    "llm_qwen36_flash": "openai",
    "llm_qwen_plus": "openai",
    "llm_kimi_k2_thinking": "openai",
    "llm_hermes3_70b": "openai",
    "openrouter_deepseekv4_pro": "openai",
    "bot_deepseekv4_pro": "openai",
    "llm_opus48": "anthropic",
    "llm_gemini31_pro_preview": "openai",
    "llm_glm52": "openai",
    "llm_kimi_k27_code": "openai",
    "llm_hermes4_405b": "openai",
}
MODEL_AVAILABILITY_STALE_AFTER = timedelta(seconds=120)
CATALOG_HIDDEN_BOT_USERNAMES = frozenset(
    {
        "llm_gemma3_4b",
        "llm_gemma3_27b",
        "llm_gemini25_flash",
        "llm_gemini25_lite",
        "llm_llama31_8b",
        "llm_llama4_scout",
        "llm_mistral_nemo",
        "openrouter_gemini25_lite",
        "openrouter_gemini31_lite",
        "openrouter_llama31_8b",
    }
)


class BotProfileConflictError(Exception):
    pass


class BotService:
    def __init__(
        self,
        users_collection: Any,
        *,
        game_collections: tuple[Any, ...] = (),
        game_usage_recorder: Callable[[LlmUsageReport], Awaitable[bool]] | None = None,
        now_factory: Callable[[], datetime] | None = None,
    ):
        self._users = users_collection
        self._game_collections = tuple(collection for collection in game_collections if collection is not None)
        self._game_usage_recorder = game_usage_recorder
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

    async def list_bots(
        self,
        *,
        viewer_role: str = "user",
        viewer_llm_bot_tier: str | None = None,
        profile_username: str | None = None,
    ) -> BotListResponse:
        cursor = self._users.find({"role": "bot", "status": "active"}).sort("username", 1)
        bots: list[BotListItem] = []
        now = self._now_factory()
        tier = normalize_llm_bot_tier(viewer_llm_bot_tier, role=viewer_role)
        profile_username_normalized = str(profile_username or "").strip().lower()
        async for doc in cursor:
            username = str(doc.get("username") or "").strip().lower()
            if username in CATALOG_HIDDEN_BOT_USERNAMES and username != profile_username_normalized:
                continue
            profile = doc.get("bot_profile") or {}
            if profile.get("listed", True) is False:
                continue
            if not self.bot_can_start_games(doc, now=now):
                continue
            llm_backed = is_llm_bot_document(doc)
            required_tier = bot_required_tier_for_document(doc)
            available_for_viewer = tier_allows_bot(tier, required_tier)
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
                    required_tier=required_tier,
                    available_for_viewer=available_for_viewer,
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
        report = LlmUsageReport.from_payload(user_id=user_id, username=username, payload=payload)
        if self._game_usage_recorder is not None:
            return await self._game_usage_recorder(report)
        return await store_llm_usage_in_game_stats(self._game_collections, report, now=self._now_factory())

    async def sync_supported_rule_variants(
        self,
        *,
        user_id: str,
        supported_rule_variants: list[str],
        username: str | None = None,
        display_name: str | None = None,
        description: str | None = None,
    ) -> dict[str, Any] | None:
        current = await self._find_active_bot(user_id)
        if current is None:
            return None

        now = self._now_factory()
        old_username = str(current.get("username") or "").strip().lower()
        new_username = username.strip().lower() if isinstance(username, str) and username.strip() else old_username
        if new_username != old_username:
            existing = await self._users.find_one({"username": new_username, "_id": {"$ne": current["_id"]}})
            if existing is not None:
                raise BotProfileConflictError(f"Username already exists: {new_username}")

        changes: dict[str, Any] = {
            "bot_profile.supported_rule_variants": list(supported_rule_variants),
            "updated_at": now,
        }
        if new_username != old_username:
            changes["username"] = new_username
        if isinstance(display_name, str) and display_name.strip():
            normalized_display = display_name.strip()
            changes["username_display"] = normalized_display
            changes["bot_profile.display_name"] = normalized_display
        if isinstance(description, str):
            normalized_description = description.strip()
            changes["bot_profile.description"] = normalized_description
            changes["profile.bio"] = normalized_description

        update = {"$set": changes}

        try:
            updated = await self._users.find_one_and_update(
                {"_id": current["_id"], "role": "bot", "status": "active"},
                update,
                return_document=ReturnDocument.AFTER,
            )
        except DuplicateKeyError as exc:
            raise BotProfileConflictError(f"Username already exists: {new_username}") from exc
        if updated is not None and new_username != old_username:
            await self._update_bot_username_references(
                user_id=str(current["_id"]),
                old_username=old_username,
                new_username=new_username,
            )
        return updated

    async def _find_active_bot(self, user_id: str) -> dict[str, Any] | None:
        for query in self._active_bot_queries(user_id):
            found = await self._users.find_one(query)
            if found is not None:
                return found
        return None

    async def _update_bot_username_references(self, *, user_id: str, old_username: str, new_username: str) -> None:
        for collection in self._game_collections:
            for side in ("white", "black"):
                await collection.update_many({f"{side}.user_id": user_id}, {"$set": {f"{side}.username": new_username}})
                await collection.update_many(
                    {f"{side}.username": old_username},
                    {"$set": {f"{side}.username": new_username}},
                )
                await collection.update_many(
                    {f"stats.llm_usage.{side}.user_id": user_id},
                    {"$set": {f"stats.llm_usage.{side}.username": new_username}},
                )
                await collection.update_many(
                    {f"stats.llm_usage.{side}.username": old_username},
                    {"$set": {f"stats.llm_usage.{side}.username": new_username}},
                )
            await collection.update_many({"created_by": old_username}, {"$set": {"created_by": new_username}})
