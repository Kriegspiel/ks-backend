from __future__ import annotations

import math
from typing import Any

from pymongo import UpdateOne

from app.models.user import utcnow

ARCHIVE_TURN_COUNT_MIGRATION_ID = "archive_turn_counts_v1"
ARCHIVE_COUNT_MISSING_FILTER = {"$or": [{"move_count": {"$exists": False}}, {"turn_count": {"$exists": False}}]}


def archive_count_fields(game: dict[str, Any]) -> dict[str, int]:
    moves = game.get("moves")
    if not isinstance(moves, list):
        moves = []
    completed_plies = 0
    for move in moves:
        if isinstance(move, dict):
            completed_plies += 1 if move.get("move_done") else 0
        else:
            completed_plies += 1
    return {
        "move_count": len(moves),
        "turn_count": math.ceil(completed_plies / 2),
    }


def _public_game_id(doc: dict[str, Any]) -> dict[str, str | None]:
    return {
        "_id": str(doc.get("_id")),
        "game_code": doc.get("game_code"),
    }


def _record_example(summary: dict[str, Any], key: str, doc: dict[str, Any], *, max_details: int) -> None:
    examples = summary[key]
    if len(examples) < max_details:
        examples.append(_public_game_id(doc))


def archive_count_update_pipeline() -> list[dict[str, Any]]:
    moves_or_empty: dict[str, Any] = {"$cond": [{"$isArray": "$moves"}, "$moves", []]}
    completed_plies = {
        "$size": {
            "$filter": {
                "input": moves_or_empty,
                "as": "move",
                "cond": {
                    "$cond": [
                        {"$eq": [{"$type": "$$move"}, "object"]},
                        {"$eq": ["$$move.move_done", True]},
                        True,
                    ]
                },
            }
        }
    }
    return [
        {
            "$set": {
                "move_count": {"$cond": [{"$isArray": "$moves"}, {"$size": "$moves"}, 0]},
                "turn_count": {
                    "$cond": [
                        {"$isArray": "$moves"},
                        {"$toInt": {"$ceil": {"$divide": [completed_plies, 2]}}},
                        0,
                    ]
                },
            }
        }
    ]


async def backfill_archive_turn_counts_server_side(db: Any) -> dict[str, Any]:
    result = await db.game_archives.update_many(
        ARCHIVE_COUNT_MISSING_FILTER,
        archive_count_update_pipeline(),
    )
    return {
        "apply": True,
        "mode": "server_side_update_many",
        "matched": int(getattr(result, "matched_count", 0)),
        "updated": int(getattr(result, "modified_count", 0)),
    }


async def backfill_archive_turn_counts(
    db: Any,
    *,
    apply: bool,
    batch_size: int = 500,
    limit: int | None = None,
    max_details: int = 20,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "apply": apply,
        "scanned": 0,
        "already_current": 0,
        "would_update": 0,
        "updated": 0,
        "missing_moves_list": 0,
        "missing_moves_examples": [],
    }
    updates: list[UpdateOne] = []

    cursor = db.game_archives.find(
        {},
        {
            "_id": 1,
            "game_code": 1,
            "moves": 1,
            "move_count": 1,
            "turn_count": 1,
        },
    )
    if limit is not None:
        cursor = cursor.limit(limit)

    async for doc in cursor:
        summary["scanned"] += 1
        if not isinstance(doc.get("moves"), list):
            summary["missing_moves_list"] += 1
            _record_example(summary, "missing_moves_examples", doc, max_details=max_details)

        counts = archive_count_fields(doc)
        if doc.get("move_count") == counts["move_count"] and doc.get("turn_count") == counts["turn_count"]:
            summary["already_current"] += 1
            continue

        summary["would_update"] += 1
        if not apply:
            continue

        updates.append(UpdateOne({"_id": doc["_id"]}, {"$set": counts}))
        if len(updates) >= batch_size:
            result = await db.game_archives.bulk_write(updates, ordered=False)
            summary["updated"] += int(getattr(result, "modified_count", len(updates)))
            updates.clear()

    if apply and updates:
        result = await db.game_archives.bulk_write(updates, ordered=False)
        summary["updated"] += int(getattr(result, "modified_count", len(updates)))

    return summary


async def run_archive_turn_count_migration_once(db: Any, *, batch_size: int = 500) -> dict[str, Any]:
    marker = await db.maintenance_state.find_one({"_id": ARCHIVE_TURN_COUNT_MIGRATION_ID})
    if marker and marker.get("status") == "completed":
        return {
            "apply": True,
            "skipped": True,
            "reason": "already_completed",
            "completed_at": marker.get("completed_at"),
        }

    started_at = utcnow()
    await db.maintenance_state.update_one(
        {"_id": ARCHIVE_TURN_COUNT_MIGRATION_ID},
        {"$set": {"status": "running", "started_at": started_at, "updated_at": started_at}},
        upsert=True,
    )
    try:
        summary = await backfill_archive_turn_counts_server_side(db)
    except Exception as exc:
        failed_at = utcnow()
        await db.maintenance_state.update_one(
            {"_id": ARCHIVE_TURN_COUNT_MIGRATION_ID},
            {"$set": {"status": "failed", "error": type(exc).__name__, "updated_at": failed_at}},
            upsert=True,
        )
        raise

    completed_at = utcnow()
    await db.maintenance_state.update_one(
        {"_id": ARCHIVE_TURN_COUNT_MIGRATION_ID},
        {"$set": {"status": "completed", "completed_at": completed_at, "updated_at": completed_at, "summary": summary}},
        upsert=True,
    )
    return {**summary, "skipped": False}
