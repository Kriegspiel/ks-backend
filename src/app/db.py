from __future__ import annotations

from urllib.parse import urlparse

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING

from app.config import Settings

_client: AsyncIOMotorClient | None = None
_db: AsyncIOMotorDatabase | None = None


def _resolve_database_name(mongo_uri: str) -> str:
    parsed = urlparse(mongo_uri)
    db_name = parsed.path.lstrip("/").split("/")[0]
    if not db_name:
        raise RuntimeError("MONGO_URI must include a default database name")
    return db_name


async def _ensure_indexes(db: AsyncIOMotorDatabase) -> None:
    await db.users.create_index([("username", ASCENDING)], unique=True)
    await db.users.create_index([("email", ASCENDING)], unique=True, sparse=True)
    await db.users.create_index([("stats.elo", DESCENDING)])
    await db.users.create_index([("status", ASCENDING), ("last_active_at", ASCENDING)])

    await db.games.create_index([("game_code", ASCENDING)], unique=True)
    await db.games.create_index([("state", ASCENDING), ("created_at", ASCENDING)])
    await db.games.create_index([("white.user_id", ASCENDING), ("state", ASCENDING)])
    await db.games.create_index([("black.user_id", ASCENDING), ("state", ASCENDING)])
    await db.games.create_index([("expires_at", ASCENDING)], expireAfterSeconds=0)

    await db.game_archives.create_index([("white.user_id", ASCENDING), ("created_at", ASCENDING)])
    await db.game_archives.create_index([("black.user_id", ASCENDING), ("created_at", ASCENDING)])
    await db.game_archives.create_index([("result.winner", ASCENDING), ("created_at", ASCENDING)])
    await db.game_archives.create_index([("created_at", DESCENDING)])

    await db.audit_log.create_index([("timestamp", ASCENDING)], expireAfterSeconds=7_776_000)
    await db.audit_log.create_index([("user_id", ASCENDING), ("timestamp", ASCENDING)])
    await db.audit_log.create_index([("game_id", ASCENDING)])

    await db.sessions.create_index([("expires_at", ASCENDING)], expireAfterSeconds=0)
    await db.sessions.create_index([("user_id", ASCENDING)])


async def init_db(settings: Settings) -> AsyncIOMotorDatabase:
    global _client, _db

    mongo_uri = settings.MONGO_URI
    db_name = _resolve_database_name(mongo_uri)
    client = AsyncIOMotorClient(mongo_uri, serverSelectionTimeoutMS=1_500)
    db = client[db_name]

    await db.command("ping")
    await _ensure_indexes(db)

    _client = client
    _db = db
    return db


def get_db() -> AsyncIOMotorDatabase:
    if _db is None:
        raise RuntimeError("Database has not been initialized")
    return _db


async def close_db() -> None:
    global _client, _db
    client = _client
    _client = None
    _db = None
    if client is not None:
        client.close()
