#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from app.services.archive_turn_counts import backfill_archive_turn_counts  # noqa: E402


async def run(*, apply: bool, batch_size: int, limit: int | None, max_details: int) -> None:
    from app.config import get_settings
    from motor.motor_asyncio import AsyncIOMotorClient

    settings = get_settings()
    client = AsyncIOMotorClient(settings.MONGO_URI)
    try:
        db = client.get_default_database()
        summary = await backfill_archive_turn_counts(
            db,
            apply=apply,
            batch_size=batch_size,
            limit=limit,
            max_details=max_details,
        )
        print(json.dumps(summary, indent=2, sort_keys=True))
    finally:
        client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill move_count and turn_count on completed game archives from stored move transcripts."
    )
    parser.add_argument("--apply", action="store_true", help="Actually write move_count and turn_count fields.")
    parser.add_argument("--batch-size", type=int, default=500, help="Bulk write size when applying.")
    parser.add_argument("--limit", type=int, default=None, help="Scan at most this many archived games.")
    parser.add_argument("--max-details", type=int, default=20, help="Maximum example ids to print per skipped bucket.")
    args = parser.parse_args()
    limit = args.limit if args.limit is None else max(1, args.limit)
    asyncio.run(
        run(
            apply=args.apply,
            batch_size=max(1, args.batch_size),
            limit=limit,
            max_details=max(0, args.max_details),
        )
    )


if __name__ == "__main__":
    main()
