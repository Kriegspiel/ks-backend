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
from app.services.user_service import DEFAULT_BOT_OWNER_EMAIL


async def run(*, dry_run: bool) -> None:
    settings = get_settings()
    client = AsyncIOMotorClient(settings.MONGO_URI)
    db = client.get_default_database()
    query = {
        "role": "bot",
        "$or": [
            {"bot_profile.owner_email": {"$exists": False}},
            {"bot_profile.owner_email": None},
            {"bot_profile.owner_email": ""},
        ],
    }
    count = await db.users.count_documents(query)
    if not dry_run and count:
        await db.users.update_many(query, {"$set": {"bot_profile.owner_email": DEFAULT_BOT_OWNER_EMAIL}})
    print({"dry_run": dry_run, "updated_bots": count, "owner_email": DEFAULT_BOT_OWNER_EMAIL})
    client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Impute missing bot owner emails.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
