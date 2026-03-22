from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


def test_health_returns_connected_payload_when_db_ping_succeeds(monkeypatch):
    fake_db = AsyncMock()
    fake_db.command = AsyncMock(return_value={"ok": 1})

    monkeypatch.setattr("app.main.init_db", AsyncMock(return_value=fake_db))
    monkeypatch.setattr("app.main.close_db", AsyncMock(return_value=None))
    monkeypatch.setattr("app.main.get_db", lambda: fake_db)

    app = create_app(Settings())

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "db": "connected"}


def test_health_returns_disconnected_payload_when_db_ping_fails(monkeypatch):
    fake_db = AsyncMock()
    fake_db.command = AsyncMock(side_effect=RuntimeError("boom"))

    monkeypatch.setattr("app.main.init_db", AsyncMock(return_value=fake_db))
    monkeypatch.setattr("app.main.close_db", AsyncMock(return_value=None))
    monkeypatch.setattr("app.main.get_db", lambda: fake_db)

    app = create_app(Settings())

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 503
    assert response.json() == {"status": "error", "db": "disconnected"}


def test_health_returns_disconnected_payload_when_db_not_initialized(monkeypatch):
    monkeypatch.setattr("app.main.init_db", AsyncMock(side_effect=RuntimeError("no mongo")))
    monkeypatch.setattr("app.main.close_db", AsyncMock(return_value=None))

    app = create_app(Settings())

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 503
    assert response.json() == {"status": "error", "db": "disconnected"}
