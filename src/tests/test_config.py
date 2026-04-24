from app.config import Settings, get_settings


def test_settings_defaults(monkeypatch):
    for key in [
        "SECRET_KEY",
        "MONGO_URI",
        "ENVIRONMENT",
        "LOG_LEVEL",
        "SITE_ORIGIN",
        "SENTRY_DSN",
        "SENTRY_TRACES_SAMPLE_RATE",
        "SENTRY_SEND_DEFAULT_PII",
    ]:
        monkeypatch.delenv(key, raising=False)

    settings = Settings()

    assert settings.SECRET_KEY == "dev-secret-change-me"
    assert settings.MONGO_URI == "mongodb://localhost:27017/kriegspiel?replicaSet=rs0"
    assert settings.ENVIRONMENT == "development"
    assert settings.LOG_LEVEL == "info"
    assert settings.SITE_ORIGIN == "http://localhost:5173"
    assert settings.SENTRY_DSN is None
    assert settings.SENTRY_TRACES_SAMPLE_RATE == 0.0
    assert settings.SENTRY_SEND_DEFAULT_PII is False


def test_settings_reads_environment_overrides(monkeypatch):
    monkeypatch.setenv("SECRET_KEY", "override-secret")
    monkeypatch.setenv("MONGO_URI", "mongodb://example:27017/override")
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("LOG_LEVEL", "debug")
    monkeypatch.setenv("SITE_ORIGIN", "https://example.com")
    monkeypatch.setenv("SENTRY_DSN", "https://public@example.com/1")
    monkeypatch.setenv("SENTRY_TRACES_SAMPLE_RATE", "0.25")
    monkeypatch.setenv("SENTRY_SEND_DEFAULT_PII", "true")

    settings = Settings()

    assert settings.SECRET_KEY == "override-secret"
    assert settings.MONGO_URI == "mongodb://example:27017/override"
    assert settings.ENVIRONMENT == "production"
    assert settings.LOG_LEVEL == "debug"
    assert settings.SITE_ORIGIN == "https://example.com"
    assert settings.SENTRY_DSN == "https://public@example.com/1"
    assert settings.SENTRY_TRACES_SAMPLE_RATE == 0.25
    assert settings.SENTRY_SEND_DEFAULT_PII is True


def test_get_settings_cache_can_be_cleared_between_tests(monkeypatch):
    get_settings.cache_clear()
    original = get_settings()

    monkeypatch.setenv("SITE_ORIGIN", "https://cache-clear.example")
    get_settings.cache_clear()
    refreshed = get_settings()

    assert original is not refreshed
    assert refreshed.SITE_ORIGIN == "https://cache-clear.example"
