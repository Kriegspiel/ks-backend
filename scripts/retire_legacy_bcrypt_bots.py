#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
import sys

from motor.motor_asyncio import AsyncIOMotorClient

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from app.config import get_settings


async def run(*, dry_run: bool) -> None:
    settings = get_settings()
    client = AsyncIOMotorClient(settings.MONGO_URI)
    db = client.get_default_database()
    query = {
        "role": "bot",
        "bot_profile.listed": False,
        "bot_profile.api_token_hash": {"$type": "string", "$ne": ""},
    }
    docs = await db.users.find(query, {"username": 1, "status": 1}).to_list(length=None)
    usernames = [doc.get("username") for doc in docs]
    if not dry_run and usernames:
        await db.users.update_many(
            query,
            {
                "$set": {"status": "inactive"},
                "$unset": {"bot_profile.api_token_hash": ""},
            },
        )
    print({"dry_run": dry_run, "retired_bots": usernames, "count": len(usernames)})
    client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Retire hidden legacy bots that still rely on bcrypt token hashes.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
