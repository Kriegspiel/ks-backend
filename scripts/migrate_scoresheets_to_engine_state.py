#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Any
import sys

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from app.config import get_settings


def _normalized_engine_state(doc: dict[str, Any]) -> dict[str, Any] | None:
    engine_state = dict(doc.get("engine_state") or {}) if isinstance(doc.get("engine_state"), dict) else {}
    white_root = doc.get("white_scoresheet")
    black_root = doc.get("black_scoresheet")

    changed = False
    if isinstance(white_root, dict) and not isinstance(engine_state.get("white_scoresheet"), dict):
        engine_state["white_scoresheet"] = white_root
        changed = True
    if isinstance(black_root, dict) and not isinstance(engine_state.get("black_scoresheet"), dict):
        engine_state["black_scoresheet"] = black_root
        changed = True

    if changed or "white_scoresheet" in doc or "black_scoresheet" in doc:
        return engine_state
    return None


async def migrate_collection(*, collection: Any, batch_size: int, dry_run: bool) -> int:
    ops: list[UpdateOne] = []
    changed = 0
    cursor = collection.find(
        {
            "$or": [
                {"white_scoresheet": {"$exists": True}},
                {"black_scoresheet": {"$exists": True}},
            ]
        },
        {"engine_state": 1, "white_scoresheet": 1, "black_scoresheet": 1},
    )
    async for doc in cursor:
        engine_state = _normalized_engine_state(doc)
        if engine_state is None:
            continue
        changed += 1
        ops.append(
            UpdateOne(
                {"_id": doc["_id"]},
                {
                    "$set": {"engine_state": engine_state},
                    "$unset": {"white_scoresheet": "", "black_scoresheet": ""},
                },
            )
        )
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
    print({"dry_run": dry_run, "games_changed": games_changed, "archives_changed": archives_changed})
    client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Move legacy root scoresheets into engine_state and unset root fields.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run, batch_size=max(1, args.batch_size)))


if __name__ == "__main__":
    main()
