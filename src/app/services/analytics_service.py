from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

from bson import ObjectId

from app.models.analytics import AcquisitionReportResponse, AcquisitionReportRow, AttributionUtm, CampaignVisitRequest


class AnalyticsService:
    ATTRIBUTION_COOKIE_NAME = "ks_attribution_id"
    ATTRIBUTION_COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24 * 365
    EVENT_RETENTION_SECONDS = 60 * 60 * 24 * 395

    def __init__(self, events_collection: Any):
        self._events = events_collection

    @classmethod
    def utcnow(cls) -> datetime:
        return datetime.now(UTC)

    @classmethod
    def attribution_cookie_domain(cls, *, environment: str) -> str | None:
        return ".kriegspiel.org" if environment == "production" else None

    @classmethod
    def is_valid_attribution_id(cls, value: str | None) -> bool:
        return isinstance(value, str) and ObjectId.is_valid(value)

    @classmethod
    def _compact_utm(cls, utm: AttributionUtm | dict[str, Any]) -> dict[str, str]:
        raw = utm.model_dump() if isinstance(utm, AttributionUtm) else dict(utm)
        return {key: value for key, value in raw.items() if isinstance(value, str) and value}

    @classmethod
    def snapshot_from_event(cls, event: dict[str, Any]) -> dict[str, Any]:
        return {
            "attribution_id": str(event["attribution_id"]),
            "utm": cls._compact_utm(event.get("utm") or {}),
            "landing_path": event.get("landing_path") or "/",
            "referrer_host": event.get("referrer_host"),
            "captured_at": event.get("occurred_at") or cls.utcnow(),
        }

    async def record_campaign_visit(self, payload: CampaignVisitRequest) -> dict[str, Any]:
        now = self.utcnow()
        event_id = ObjectId()
        event = {
            "_id": event_id,
            "event_type": "campaign_visit",
            "attribution_id": str(event_id),
            "occurred_at": now,
            "expires_at": now + timedelta(seconds=self.EVENT_RETENTION_SECONDS),
            "landing_path": payload.landing_path,
            "referrer_host": payload.referrer_host,
            "utm": self._compact_utm(payload.utm),
        }
        await self._events.insert_one(event)
        return event

    async def attribution_snapshot_for_id(self, attribution_id: str | None) -> dict[str, Any] | None:
        if not self.is_valid_attribution_id(attribution_id):
            return None
        event = await self._events.find_one({"attribution_id": attribution_id, "event_type": "campaign_visit"})
        if event is None:
            return None
        return self.snapshot_from_event(event)

    @staticmethod
    def _row_key(source: str | None, medium: str | None, campaign: str | None) -> tuple[str | None, str | None, str | None]:
        return source, medium, campaign

    @classmethod
    def _field_path(cls, collection_name: str, key: str) -> str:
        if collection_name == "analytics_events":
            return f"$utm.{key}"
        if collection_name == "users":
            return f"$acquisition.utm.{key}"
        return f"$attribution.utm.{key}"

    @classmethod
    def _group_pipeline(
        cls,
        *,
        collection_name: str,
        since: datetime,
        completed_archives: bool = False,
    ) -> list[dict[str, Any]]:
        match: dict[str, Any] = {"created_at": {"$gte": since}}
        if collection_name == "analytics_events":
            match = {"event_type": "campaign_visit", "occurred_at": {"$gte": since}}
        elif collection_name == "users":
            match["acquisition"] = {"$exists": True}
        elif collection_name == "game_archives" and completed_archives:
            match = {"created_at": {"$gte": since}, "attribution": {"$exists": True}}
        else:
            match["attribution"] = {"$exists": True}

        return [
            {"$match": match},
            {
                "$group": {
                    "_id": {
                        "source": cls._field_path(collection_name, "source"),
                        "medium": cls._field_path(collection_name, "medium"),
                        "campaign": cls._field_path(collection_name, "campaign"),
                    },
                    "count": {"$sum": 1},
                }
            },
        ]

    @staticmethod
    def _merge_counts(
        rows: dict[tuple[str | None, str | None, str | None], AcquisitionReportRow],
        grouped: Iterable[dict[str, Any]],
        field: str,
    ) -> None:
        for item in grouped:
            key_doc = item.get("_id") or {}
            key = (
                key_doc.get("source") if key_doc.get("source") is not None else None,
                key_doc.get("medium") if key_doc.get("medium") is not None else None,
                key_doc.get("campaign") if key_doc.get("campaign") is not None else None,
            )
            row = rows.setdefault(key, AcquisitionReportRow(source=key[0], medium=key[1], campaign=key[2]))
            setattr(row, field, int(item.get("count") or 0))

    async def acquisition_report(self, db: Any, *, days: int) -> AcquisitionReportResponse:
        now = self.utcnow()
        since = now - timedelta(days=days)
        rows: dict[tuple[str | None, str | None, str | None], AcquisitionReportRow] = {}

        sources = [
            ("analytics_events", db.analytics_events, "visits"),
            ("sessions", db.sessions, "sessions"),
            ("users", db.users, "acquired_users"),
            ("games", db.games, "games_created"),
            ("game_archives", db.game_archives, "games_completed"),
        ]
        for collection_name, collection, field in sources:
            grouped = collection.aggregate(
                self._group_pipeline(
                    collection_name=collection_name,
                    since=since,
                    completed_archives=collection_name == "game_archives",
                )
            )
            self._merge_counts(rows, await grouped.to_list(length=None), field)

        ordered = sorted(
            rows.values(),
            key=lambda row: (
                -(row.visits + row.sessions + row.acquired_users + row.games_created + row.games_completed),
                row.source or "",
                row.medium or "",
                row.campaign or "",
            ),
        )
        return AcquisitionReportResponse(days=days, generated_at=now, rows=ordered)
