from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio

import app.db as db_module
from app.config import Settings


@pytest_asyncio.fixture(autouse=True)
async def reset_db_state():
    await db_module.close_db()
    yield
    await db_module.close_db()


def _fake_db_collections():
    return {
        name: SimpleNamespace(create_index=AsyncMock()) for name in ["users", "games", "game_archives", "audit_log", "sessions"]
    }


def _make_fake_db():
    fake_db = AsyncMock()
    fake_db.command = AsyncMock(return_value={"ok": 1})
    for name, col in _fake_db_collections().items():
        setattr(fake_db, name, col)
    return fake_db


def test_get_db_raises_when_not_initialized():
    with pytest.raises(RuntimeError, match="Database has not been initialized"):
        db_module.get_db()


@pytest.mark.asyncio
async def test_init_db_builds_motor_client_from_settings(monkeypatch):
    fake_db = _make_fake_db()
    fake_client = Mock()
    fake_client.__getitem__ = Mock(return_value=fake_db)
    client_factory = Mock(return_value=fake_client)

    monkeypatch.setattr(db_module, "AsyncIOMotorClient", client_factory)

    settings = Settings(MONGO_URI="mongodb://localhost:27017/kriegspiel_slice120_test?replicaSet=rs0")
    resolved = await db_module.init_db(settings)

    client_factory.assert_called_once_with(settings.MONGO_URI, serverSelectionTimeoutMS=1_500)
    fake_client.__getitem__.assert_called_once_with("kriegspiel_slice120_test")
    assert resolved is fake_db


@pytest.mark.asyncio
async def test_init_db_creates_required_indexes(monkeypatch):
    collections = _fake_db_collections()
    fake_db = AsyncMock()
    fake_db.command = AsyncMock(return_value={"ok": 1})
    for name, col in collections.items():
        setattr(fake_db, name, col)

    fake_client = Mock()
    fake_client.__getitem__ = Mock(return_value=fake_db)
    monkeypatch.setattr(db_module, "AsyncIOMotorClient", Mock(return_value=fake_client))

    await db_module.init_db(Settings())

    assert collections["users"].create_index.await_args_list == [
        (([("username", 1)],), {"unique": True}),
        (([("email", 1)],), {"unique": True, "sparse": True}),
        (([("stats.elo", -1)],), {}),
        (([("status", 1), ("last_active_at", 1)],), {}),
    ]
    assert collections["games"].create_index.await_args_list == [
        (([("game_code", 1)],), {"unique": True}),
        (([("state", 1), ("created_at", 1)],), {}),
        (([("white.user_id", 1), ("state", 1)],), {}),
        (([("black.user_id", 1), ("state", 1)],), {}),
        (([("expires_at", 1)],), {"expireAfterSeconds": 0}),
    ]
    assert collections["game_archives"].create_index.await_count == 4
    assert collections["audit_log"].create_index.await_args_list[0] == (
        (([("timestamp", 1)],), {"expireAfterSeconds": 7_776_000})
    )
    assert collections["sessions"].create_index.await_args_list == [
        (([("expires_at", 1)],), {"expireAfterSeconds": 0}),
        (([("user_id", 1)],), {}),
    ]


@pytest.mark.asyncio
async def test_close_db_is_idempotent(monkeypatch):
    fake_db = _make_fake_db()
    fake_client = Mock(close=Mock())
    fake_client.__getitem__ = Mock(return_value=fake_db)
    monkeypatch.setattr(db_module, "AsyncIOMotorClient", Mock(return_value=fake_client))

    await db_module.init_db(Settings())
    await db_module.close_db()
    await db_module.close_db()

    fake_client.close.assert_called_once()


@pytest.mark.asyncio
async def test_init_db_returns_handle_for_expected_database(monkeypatch):
    fake_db = _make_fake_db()
    fake_client = Mock()
    fake_client.__getitem__ = Mock(return_value=fake_db)
    monkeypatch.setattr(db_module, "AsyncIOMotorClient", Mock(return_value=fake_client))

    settings = Settings(MONGO_URI="mongodb://localhost:27017/kriegspiel_slice120_test?replicaSet=rs0")
    db = await db_module.init_db(settings)

    assert db is fake_db
    fake_client.__getitem__.assert_called_once_with("kriegspiel_slice120_test")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_init_db_integration_creates_indexes_when_mongo_available():
    if os.getenv("RUN_MONGO_INTEGRATION") != "1":
        pytest.skip("Set RUN_MONGO_INTEGRATION=1 to run Mongo integration test")

    settings = Settings(MONGO_URI=os.environ["MONGO_URI"])

    db = await db_module.init_db(settings)
    games_indexes = await db.games.index_information()

    assert any(spec.get("key") == [("game_code", 1)] and spec.get("unique") for spec in games_indexes.values())
    assert any(
        spec.get("key") == [("expires_at", 1)] and spec.get("expireAfterSeconds") == 0 for spec in games_indexes.values()
    )

    await db_module.close_db()
