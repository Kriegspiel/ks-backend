from unittest.mock import AsyncMock

import pytest
from app.version import APP_VERSION


@pytest.mark.asyncio
async def test_health_returns_connected_payload_when_db_ping_succeeds(monkeypatch, test_app, test_client):
    fake_db = AsyncMock()
    fake_db.command = AsyncMock(return_value={"ok": 1})

    test_app.state.db_ready = True
    monkeypatch.setattr("app.main.get_db", lambda: fake_db)

    response = await test_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "db": "connected", "version": APP_VERSION}


@pytest.mark.asyncio
async def test_health_returns_disconnected_payload_when_db_ping_fails(monkeypatch, test_app, test_client):
    fake_db = AsyncMock()
    fake_db.command = AsyncMock(side_effect=RuntimeError("boom"))

    test_app.state.db_ready = True
    monkeypatch.setattr("app.main.get_db", lambda: fake_db)

    response = await test_client.get("/health")

    assert response.status_code == 503
    assert response.json() == {"status": "error", "db": "disconnected", "version": APP_VERSION}


@pytest.mark.asyncio
async def test_health_returns_disconnected_payload_when_db_not_initialized(test_app, test_client):
    test_app.state.db_ready = False

    response = await test_client.get("/health")

    assert response.status_code == 503
    assert response.json() == {"status": "error", "db": "disconnected", "version": APP_VERSION}


@pytest.mark.asyncio
async def test_health_recovers_after_transient_dependency_outage(monkeypatch, test_app, test_client):
    fake_db = AsyncMock()
    fake_db.command = AsyncMock(side_effect=[RuntimeError("mongo down"), {"ok": 1}])

    test_app.state.db_ready = True
    monkeypatch.setattr("app.main.get_db", lambda: fake_db)

    down_response = await test_client.get("/health")
    test_app.state.db_ready = True
    recovered_response = await test_client.get("/health")

    assert down_response.status_code == 503
    assert down_response.json() == {"status": "error", "db": "disconnected", "version": APP_VERSION}
    assert recovered_response.status_code == 200
    assert recovered_response.json() == {"status": "ok", "db": "connected", "version": APP_VERSION}
