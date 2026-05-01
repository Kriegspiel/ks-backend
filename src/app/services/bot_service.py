from __future__ import annotations

from typing import Any

from app.models.bot import BotListItem, BotListResponse, supported_rule_variants_for_bot
from app.models.user import normalize_user_stats_payload


class BotService:
    def __init__(self, users_collection: Any):
        self._users = users_collection

    @staticmethod
    def _supported_rule_variants(doc: dict[str, Any]) -> list[str]:
        profile = doc.get("bot_profile") or {}
        return supported_rule_variants_for_bot(str(doc.get("username") or ""), profile.get("supported_rule_variants"))

    async def list_bots(self) -> BotListResponse:
        cursor = self._users.find({"role": "bot", "status": "active"}).sort("username", 1)
        bots: list[BotListItem] = []
        async for doc in cursor:
            profile = doc.get("bot_profile") or {}
            if profile.get("listed", True) is False:
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
