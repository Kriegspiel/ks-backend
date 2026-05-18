#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from copy import deepcopy
import json
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from app.services.mongo_document_compare import mongo_documents_equal  # noqa: E402


def _public_game_id(doc: dict[str, Any]) -> dict[str, str | None]:
    return {
        "_id": str(doc.get("_id")),
        "game_code": doc.get("game_code"),
    }


def _record_example(summary: dict[str, Any], key: str, doc: dict[str, Any], *, max_details: int) -> None:
    examples = summary[key]
    if len(examples) < max_details:
        examples.append(_public_game_id(doc))


async def _replace_archive(db: Any, live_doc: dict[str, Any]) -> None:
    document = deepcopy(live_doc)
    await db.game_archives.replace_one({"_id": document["_id"]}, document, upsert=True)


async def _delete_live_completed_game(db: Any, live_doc: dict[str, Any]) -> int:
    result = await db.games.delete_one({"_id": live_doc["_id"], "state": "completed"})
    return int(getattr(result, "deleted_count", 0))


async def reconcile_completed_games(
    db: Any,
    *,
    apply: bool,
    repair_mismatched: bool = False,
    limit: int | None = None,
    max_details: int = 20,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "apply": apply,
        "repair_mismatched": repair_mismatched,
        "scanned": 0,
        "identical": 0,
        "missing_archive": 0,
        "mismatched_archive": 0,
        "would_archive_missing": 0,
        "would_repair_mismatched": 0,
        "would_delete_identical": 0,
        "archived_missing": 0,
        "repaired_mismatched": 0,
        "deleted": 0,
        "delete_missed": 0,
        "archive_verify_failed": 0,
        "missing_archive_examples": [],
        "mismatched_archive_examples": [],
        "archive_verify_failed_examples": [],
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
            _record_example(summary, "missing_archive_examples", live_doc, max_details=max_details)
            if not apply:
                summary["would_archive_missing"] += 1
                continue

            await _replace_archive(db, live_doc)
            verified = await db.game_archives.find_one({"_id": live_doc["_id"]})
            if not mongo_documents_equal(verified, live_doc):
                summary["archive_verify_failed"] += 1
                _record_example(summary, "archive_verify_failed_examples", live_doc, max_details=max_details)
                continue
            summary["archived_missing"] += 1

        elif mongo_documents_equal(archive_doc, live_doc):
            summary["identical"] += 1
            if not apply:
                summary["would_delete_identical"] += 1
                continue

        else:
            summary["mismatched_archive"] += 1
            _record_example(summary, "mismatched_archive_examples", live_doc, max_details=max_details)
            if not repair_mismatched:
                continue
            if not apply:
                summary["would_repair_mismatched"] += 1
                continue

            await _replace_archive(db, live_doc)
            verified = await db.game_archives.find_one({"_id": live_doc["_id"]})
            if not mongo_documents_equal(verified, live_doc):
                summary["archive_verify_failed"] += 1
                _record_example(summary, "archive_verify_failed_examples", live_doc, max_details=max_details)
                continue
            summary["repaired_mismatched"] += 1

        deleted_count = await _delete_live_completed_game(db, live_doc)
        if deleted_count == 1:
            summary["deleted"] += 1
        else:
            summary["delete_missed"] += 1
            _record_example(summary, "delete_missed_examples", live_doc, max_details=max_details)

    return summary


async def run(*, apply: bool, repair_mismatched: bool, limit: int | None, max_details: int) -> None:
    from app.config import get_settings
    from motor.motor_asyncio import AsyncIOMotorClient

    settings = get_settings()
    client = AsyncIOMotorClient(settings.MONGO_URI)
    try:
        db = client.get_default_database()
        summary = await reconcile_completed_games(
            db,
            apply=apply,
            repair_mismatched=repair_mismatched,
            limit=limit,
            max_details=max_details,
        )
        print(json.dumps(summary, indent=2, sort_keys=True))
    finally:
        client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill completed game archives from the live games collection, "
            "verify the archive copy, then delete the completed live document."
        )
    )
    parser.add_argument("--apply", action="store_true", help="Actually write archives and delete completed live docs.")
    parser.add_argument(
        "--repair-mismatched",
        action="store_true",
        help="Replace mismatched archive docs with the live completed doc before deleting the live copy.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Scan at most this many completed live games.")
    parser.add_argument("--max-details", type=int, default=20, help="Maximum example ids to print per skipped bucket.")
    args = parser.parse_args()
    limit = args.limit if args.limit is None else max(1, args.limit)
    asyncio.run(
        run(
            apply=args.apply,
            repair_mismatched=args.repair_mismatched,
            limit=limit,
            max_details=max(0, args.max_details),
        )
    )


if __name__ == "__main__":
    main()
