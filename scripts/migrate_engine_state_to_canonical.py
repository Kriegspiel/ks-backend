#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from datetime import UTC, datetime
import gzip
from pathlib import Path
import sys
from typing import Any

from bson import json_util
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from app.config import get_settings
from app.services.engine_state_migration import canonicalize_game_document, classify_engine_state


async def backup_collection(*, collection: Any, backup_path: Path) -> int:
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with gzip.open(backup_path, "wt", encoding="utf-8") as handle:
        async for doc in collection.find({}):
            handle.write(json_util.dumps(doc))
            handle.write("\n")
            count += 1
    return count


async def migrate_collection(*, collection: Any, batch_size: int, dry_run: bool) -> dict[str, Any]:
    ops: list[UpdateOne] = []
    migrated = 0
    skipped = 0
    by_shape: Counter[str] = Counter()

    cursor = collection.find({}, {"engine_state": 1, "moves": 1, "rule_variant": 1})
    async for doc in cursor:
        shape = classify_engine_state(doc.get("engine_state"))
        canonical = canonicalize_game_document(doc)
        if canonical is None:
            skipped += 1
            continue
        migrated += 1
        by_shape[shape] += 1
        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": {"engine_state": canonical}}))
        if not dry_run and len(ops) >= batch_size:
            await collection.bulk_write(ops, ordered=False)
            ops.clear()

    if not dry_run and ops:
        await collection.bulk_write(ops, ordered=False)

    return {"migrated": migrated, "skipped": skipped, "by_shape": dict(by_shape)}


async def run(*, dry_run: bool, batch_size: int, backup_root: Path | None) -> None:
    settings = get_settings()
    client = AsyncIOMotorClient(settings.MONGO_URI)
    db = client.get_default_database()

    backup_counts: dict[str, int] = {}
    if not dry_run:
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        backup_dir = (backup_root or (ROOT / "backups")) / f"engine-state-canonical-{timestamp}"
        backup_counts["games"] = await backup_collection(collection=db.games, backup_path=backup_dir / "games.jsonl.gz")
        backup_counts["game_archives"] = await backup_collection(
            collection=db.game_archives,
            backup_path=backup_dir / "game_archives.jsonl.gz",
        )
        print({"backup_dir": str(backup_dir), "backup_counts": backup_counts})

    games_result = await migrate_collection(collection=db.games, batch_size=batch_size, dry_run=dry_run)
    archives_result = await migrate_collection(collection=db.game_archives, batch_size=batch_size, dry_run=dry_run)

    print(
        {
            "dry_run": dry_run,
            "games": games_result,
            "game_archives": archives_result,
        }
    )
    client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate stored game engine_state payloads to canonical ks-game serialization.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--backup-root", type=Path, default=None)
    args = parser.parse_args()
    asyncio.run(
        run(
            dry_run=args.dry_run,
            batch_size=max(1, args.batch_size),
            backup_root=args.backup_root,
        )
    )


if __name__ == "__main__":
    main()
