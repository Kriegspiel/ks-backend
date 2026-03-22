from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


def test_health_returns_http_200():
    app = create_app(Settings())

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200


def test_health_returns_exact_slice_110_payload():
    app = create_app(Settings())

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.json() == {"status": "ok"}


def test_health_response_is_json():
    app = create_app(Settings())

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.headers["content-type"].startswith("application/json")
