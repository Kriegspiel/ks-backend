from fastapi.testclient import TestClient
from unittest.mock import AsyncMock

from app.config import Settings
from app.main import create_app
from app.version import APP_VERSION


def test_create_app_sets_expected_title():
    app = create_app(Settings())
    assert app.title == "Kriegspiel Chess API"


def test_create_app_stores_settings_on_app_state():
    settings = Settings(SITE_ORIGIN="https://frontend.example")
    app = create_app(settings)

    assert hasattr(app.state, "settings")
    assert app.state.settings.SITE_ORIGIN == "https://frontend.example"


def test_app_factory_is_repeatable():
    app_one = create_app(Settings(SITE_ORIGIN="https://one.example"))
    app_two = create_app(Settings(SITE_ORIGIN="https://two.example"))

    assert app_one is not app_two
    assert app_one.state.settings.SITE_ORIGIN == "https://one.example"
    assert app_two.state.settings.SITE_ORIGIN == "https://two.example"


def test_lifespan_runs_without_external_dependencies_and_reports_unhealthy_db():
    app = create_app(Settings())

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code in (200, 503)


def test_cors_allows_site_origin_preflight():
    app = create_app(Settings(SITE_ORIGIN="http://localhost:5173"))

    with TestClient(app) as client:
        response = client.options(
            "/health",
            headers={
                "Origin": "http://localhost:5173",
                "Access-Control-Request-Method": "GET",
            },
        )

    assert response.headers["access-control-allow-origin"] == "http://localhost:5173"
    assert response.headers["access-control-allow-credentials"] == "true"


def test_cors_allows_local_backend_origin_in_development():
    app = create_app(Settings(ENVIRONMENT="development", SITE_ORIGIN="https://frontend.example"))

    with TestClient(app) as client:
        response = client.options(
            "/health",
            headers={
                "Origin": "http://localhost:8000",
                "Access-Control-Request-Method": "GET",
            },
        )

    assert response.headers["access-control-allow-origin"] == "http://localhost:8000"


def test_cors_does_not_grant_unknown_origin():
    app = create_app(Settings(SITE_ORIGIN="https://frontend.example"))

    with TestClient(app) as client:
        response = client.options(
            "/health",
            headers={
                "Origin": "http://evil.example",
                "Access-Control-Request-Method": "GET",
            },
        )

    assert response.headers.get("access-control-allow-origin") != "http://evil.example"


def test_api_health_mirrors_health_endpoint():
    app = create_app(Settings())

    with TestClient(app) as client:
        api_response = client.get("/api/health")

    assert api_response.status_code in (200, 503)
    assert api_response.json()["version"] == APP_VERSION


def test_lifespan_initializes_and_shuts_down_game_service(monkeypatch) -> None:
    import app.main as main_module

    fake_db = type(
        "FakeDB",
        (),
        {
            "games": object(),
            "users": object(),
            "game_archives": object(),
        },
    )()
    calls: list[str] = []

    class FakeGameService:
        def __init__(self, games, *, users_collection, archives_collection, site_origin) -> None:  # noqa: ANN001
            assert games is fake_db.games
            assert users_collection is fake_db.users
            assert archives_collection is fake_db.game_archives
            assert site_origin == "https://frontend.example"
            self.start = AsyncMock(side_effect=lambda: calls.append("start"))
            self.shutdown = AsyncMock(side_effect=lambda: calls.append("shutdown"))

    monkeypatch.setattr(main_module, "init_db", AsyncMock(return_value=fake_db))
    monkeypatch.setattr(main_module, "close_db", AsyncMock(side_effect=lambda: calls.append("close_db")))
    monkeypatch.setattr(main_module, "GameService", FakeGameService)

    app = create_app(Settings(ENVIRONMENT="testing", SITE_ORIGIN="https://frontend.example"))

    with TestClient(app):
        assert app.state.db is fake_db
        assert app.state.db_ready is True
        assert app.state.game_service is not None

    assert calls == ["start", "shutdown", "close_db"]
