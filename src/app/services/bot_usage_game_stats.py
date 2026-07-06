from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.services.game_usage_stats import LlmUsageReport, store_llm_usage_in_game_stats

BOT_USAGE_GAME_STATS_MIGRATION_ID = "bot_usage_game_stats_v1"


def _normalize_utc_datetime(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


async def backfill_bot_usage_records_to_game_stats(
    db: Any,
    *,
    limit: int | None = None,
) -> dict[str, Any]:
    usage_collection = getattr(db, "bot_usage_records", None)
    game_collections = tuple(
        collection
        for collection in (
            getattr(db, "games", None),
            getattr(db, "game_archives", None),
        )
        if collection is not None
    )
    summary: dict[str, Any] = {
        "scanned": 0,
        "stored": 0,
        "missing_game_ref": 0,
        "not_stored": 0,
        "skipped": False,
    }
    if usage_collection is None or not game_collections:
        summary["skipped"] = True
        return summary

    projection = {
        "_id": 1,
        "game_id": 1,
        "game_code": 1,
        "bot_user_id": 1,
        "bot_username": 1,
        "provider": 1,
        "model": 1,
        "response_id": 1,
        "input_tokens": 1,
        "cached_input_tokens": 1,
        "output_tokens": 1,
        "cache_read_input_tokens": 1,
        "cache_creation_input_tokens": 1,
        "total_tokens": 1,
        "cost_usd": 1,
        "recorded_at": 1,
        "created_at": 1,
    }
    cursor = usage_collection.find({}, projection)
    if limit is not None and hasattr(cursor, "limit"):
        cursor = cursor.limit(max(0, limit))
    if hasattr(cursor, "batch_size"):
        cursor = cursor.batch_size(500)

    async for record in cursor:
        summary["scanned"] += 1
        report = LlmUsageReport.from_record(record)
        if not report.game_id and not report.game_code:
            summary["missing_game_ref"] += 1
            summary["not_stored"] += 1
            continue
        recorded_at = (
            _normalize_utc_datetime(record.get("recorded_at"))
            or _normalize_utc_datetime(record.get("created_at"))
            or datetime.now(UTC)
        )
        stored = await store_llm_usage_in_game_stats(game_collections, report, now=recorded_at)
        if stored:
            summary["stored"] += 1
        else:
            summary["not_stored"] += 1
    return summary


async def run_bot_usage_game_stats_migration_once(db: Any, *, limit: int | None = None) -> dict[str, Any]:
    maintenance = getattr(db, "maintenance_state", None)
    if maintenance is None:
        summary = await backfill_bot_usage_records_to_game_stats(db, limit=limit)
        summary["maintenance_state"] = "missing"
        return summary

    existing = await maintenance.find_one({"_id": BOT_USAGE_GAME_STATS_MIGRATION_ID})
    if existing is not None and existing.get("status") == "completed":
        return {
            "skipped": True,
            "reason": "already_completed",
            "summary": existing.get("summary", {}),
        }

    started_at = datetime.now(UTC)
    try:
        summary = await backfill_bot_usage_records_to_game_stats(db, limit=limit)
    except Exception as exc:
        await maintenance.update_one(
            {"_id": BOT_USAGE_GAME_STATS_MIGRATION_ID},
            {
                "$set": {
                    "status": "failed",
                    "error": type(exc).__name__,
                    "updated_at": datetime.now(UTC),
                    "started_at": started_at,
                }
            },
            upsert=True,
        )
        raise

    await maintenance.update_one(
        {"_id": BOT_USAGE_GAME_STATS_MIGRATION_ID},
        {
            "$set": {
                "status": "completed",
                "summary": summary,
                "started_at": started_at,
                "completed_at": datetime.now(UTC),
                "updated_at": datetime.now(UTC),
            }
        },
        upsert=True,
    )
    return {**summary, "skipped": False}
