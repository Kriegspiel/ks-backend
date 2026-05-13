#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def _public_game_id(doc: dict[str, Any]) -> dict[str, str | None]:
    return {
        "_id": str(doc.get("_id")),
        "game_code": doc.get("game_code"),
    }


async def cleanup_completed_games(db: Any, *, apply: bool, limit: int | None = None, max_details: int = 20) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "apply": apply,
        "scanned": 0,
        "identical": 0,
        "deleted": 0,
        "would_delete": 0,
        "missing_archive": 0,
        "mismatched_archive": 0,
        "delete_missed": 0,
        "missing_archive_examples": [],
        "mismatched_archive_examples": [],
        "delete_missed_examples": [],
    }

    cursor = db.games.find({"state": "completed"})
    if limit is not None:
        cursor = cursor.limit(limit)

    async for live_doc in cursor:
        summary["scanned"] += 1
        archive_doc = await db.game_archives.find_one({"_id": live_doc["_id"]})
        if archive_doc is None:
            summary["missing_archive"] += 1
            if len(summary["missing_archive_examples"]) < max_details:
                summary["missing_archive_examples"].append(_public_game_id(live_doc))
            continue

        if archive_doc != live_doc:
            summary["mismatched_archive"] += 1
            if len(summary["mismatched_archive_examples"]) < max_details:
                summary["mismatched_archive_examples"].append(_public_game_id(live_doc))
            continue

        summary["identical"] += 1
        if not apply:
            summary["would_delete"] += 1
            continue

        result = await db.games.delete_one({"_id": live_doc["_id"]})
        if getattr(result, "deleted_count", 0) == 1:
            summary["deleted"] += 1
        else:
            summary["delete_missed"] += 1
            if len(summary["delete_missed_examples"]) < max_details:
                summary["delete_missed_examples"].append(_public_game_id(live_doc))

    return summary


async def run(*, apply: bool, limit: int | None, max_details: int) -> None:
    from app.config import get_settings
    from motor.motor_asyncio import AsyncIOMotorClient

    settings = get_settings()
    client = AsyncIOMotorClient(settings.MONGO_URI)
    try:
        db = client.get_default_database()
        summary = await cleanup_completed_games(db, apply=apply, limit=limit, max_details=max_details)
        print(json.dumps(summary, indent=2, sort_keys=True))
    finally:
        client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete completed games from games only when an identical archived document already exists."
    )
    parser.add_argument("--apply", action="store_true", help="Actually delete matching completed games from games.")
    parser.add_argument("--limit", type=int, default=None, help="Scan at most this many completed live games.")
    parser.add_argument("--max-details", type=int, default=20, help="Maximum example ids to print per skipped bucket.")
    args = parser.parse_args()
    limit = args.limit if args.limit is None else max(1, args.limit)
    asyncio.run(run(apply=args.apply, limit=limit, max_details=max(0, args.max_details)))


if __name__ == "__main__":
    main()
