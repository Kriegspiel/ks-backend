from __future__ import annotations

from collections.abc import AsyncIterator
import os
import random
from urllib.parse import urlparse

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError

from app.config import Settings, get_settings
from app.db import close_db
from app.main import create_app


@pytest.fixture
def test_settings() -> AsyncIterator[Settings]:
    get_settings.cache_clear()
    settings = Settings(
        ENVIRONMENT="testing",
        SECRET_KEY="kriegspiel-step-150-test-secret",
        SITE_ORIGIN="http://localhost:5173",
        MONGO_URI="mongodb://localhost:27018/kriegspiel_test",
    )
    yield settings
    get_settings.cache_clear()


@pytest_asyncio.fixture
async def test_app(test_settings: Settings):
    app = create_app(settings=test_settings)
    async with app.router.lifespan_context(app):
        yield app


@pytest_asyncio.fixture
async def test_client(test_app):
    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


@pytest.fixture(autouse=True)
def deterministic_test_seed():
    seed = int(os.environ.get("TEST_RANDOM_SEED", "800"))
    random.seed(seed)
    return seed


def _database_name(mongo_uri: str) -> str:
    name = urlparse(mongo_uri).path.lstrip("/").split("/")[0]
    if not name:
        raise RuntimeError("Test MONGO_URI must include a database name")
    return name


@pytest_asyncio.fixture(autouse=True)
async def db_cleanup(test_settings: Settings):
    db_name = _database_name(test_settings.MONGO_URI)
    if "test" not in db_name:
        raise RuntimeError(f"Refusing to cleanup non-test database: {db_name}")

    client = AsyncIOMotorClient(test_settings.MONGO_URI, serverSelectionTimeoutMS=1_500)
    try:
        try:
            await client.drop_database(db_name)
        except PyMongoError:
            pass

        await close_db()
        yield

        await close_db()
        try:
            await client.drop_database(db_name)
        except PyMongoError:
            pass
    finally:
        client.close()
