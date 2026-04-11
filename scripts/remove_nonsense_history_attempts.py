#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from copy import deepcopy
from pathlib import Path
import sys
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from app.config import get_settings
from app.services.game_service import NONSENSE_HISTORY_ANNOUNCEMENTS


def _clean_moves(doc: dict[str, Any]) -> list[dict[str, Any]] | None:
    moves = doc.get("moves")
    if not isinstance(moves, list):
        return None

    cleaned: list[dict[str, Any]] = []
    changed = False
    for move in moves:
        if not isinstance(move, dict):
            cleaned.append(move)
            continue
        announcement = move.get("announcement")
        if isinstance(announcement, str) and announcement in NONSENSE_HISTORY_ANNOUNCEMENTS:
            changed = True
            continue
        normalized = deepcopy(move)
        cleaned.append(normalized)

    if not changed:
        return None

    for index, move in enumerate(cleaned, start=1):
        if isinstance(move, dict):
            move["ply"] = index
    return cleaned


async def migrate_collection(*, collection: Any, batch_size: int, dry_run: bool) -> int:
    ops: list[UpdateOne] = []
    changed = 0
    cursor = collection.find(
        {"moves.announcement": {"$in": list(NONSENSE_HISTORY_ANNOUNCEMENTS)}},
        {"moves": 1},
    )
    async for doc in cursor:
        cleaned = _clean_moves(doc)
        if cleaned is None:
            continue
        changed += 1
        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": {"moves": cleaned}}))
        if not dry_run and len(ops) >= batch_size:
            await collection.bulk_write(ops, ordered=False)
            ops.clear()

    if not dry_run and ops:
        await collection.bulk_write(ops, ordered=False)
    return changed


async def run(*, dry_run: bool, batch_size: int) -> None:
    settings = get_settings()
    client = AsyncIOMotorClient(settings.MONGO_URI)
    db = client.get_default_database()
    games_changed = await migrate_collection(collection=db.games, batch_size=batch_size, dry_run=dry_run)
    archives_changed = await migrate_collection(collection=db.game_archives, batch_size=batch_size, dry_run=dry_run)
    print(
        {
            "dry_run": dry_run,
            "games_changed": games_changed,
            "archives_changed": archives_changed,
            "announcements_removed": sorted(NONSENSE_HISTORY_ANNOUNCEMENTS),
        }
    )
    client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Remove impossible or invalid attempts from stored game history.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run, batch_size=max(1, args.batch_size)))


if __name__ == "__main__":
    main()
