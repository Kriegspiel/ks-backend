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
        "TUTOR_ENABLED",
        "TUTOR_BETA_USER_IDS",
        "TUTOR_MONTHLY_BUDGET_USD",
        "TUTOR_ANALYSIS_MODEL",
        "TUTOR_CACHED_INPUT_COST_PER_MILLION_USD",
        "OPENAI_API_KEY",
        "STRIPE_SECRET_KEY",
        "STRIPE_PUBLISHABLE_KEY",
        "STRIPE_WEBHOOK_SECRET",
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
    assert settings.TUTOR_ENABLED is False
    assert settings.TUTOR_BETA_USER_IDS == ""
    assert settings.TUTOR_MONTHLY_BUDGET_USD == 5.0
    assert settings.TUTOR_ANALYSIS_MODEL == "gpt-5.6-terra"
    assert settings.TUTOR_CACHED_INPUT_COST_PER_MILLION_USD == 0.25
    assert settings.OPENAI_API_KEY is None
    assert settings.STRIPE_SECRET_KEY is None
    assert settings.STRIPE_PUBLISHABLE_KEY is None
    assert settings.STRIPE_WEBHOOK_SECRET is None
    assert settings.STRIPE_API_BASE == "https://api.stripe.com/v1"
    assert settings.STRIPE_PRICE_T2_MONTHLY is None


def test_settings_reads_environment_overrides(monkeypatch):
    monkeypatch.setenv("SECRET_KEY", "override-secret")
    monkeypatch.setenv("MONGO_URI", "mongodb://example:27017/override")
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("LOG_LEVEL", "debug")
    monkeypatch.setenv("SITE_ORIGIN", "https://example.com")
    monkeypatch.setenv("SENTRY_DSN", "https://public@example.com/1")
    monkeypatch.setenv("SENTRY_TRACES_SAMPLE_RATE", "0.25")
    monkeypatch.setenv("SENTRY_SEND_DEFAULT_PII", "true")
    monkeypatch.setenv("TUTOR_ENABLED", "true")
    monkeypatch.setenv("TUTOR_BETA_USER_IDS", "fil-id")
    monkeypatch.setenv("TUTOR_MONTHLY_BUDGET_USD", "5")
    monkeypatch.setenv("TUTOR_ANALYSIS_MODEL", "gpt-5.6-terra")
    monkeypatch.setenv("TUTOR_CACHED_INPUT_COST_PER_MILLION_USD", "0.2")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_123")
    monkeypatch.setenv("STRIPE_PUBLISHABLE_KEY", "pk_test_123")
    monkeypatch.setenv("STRIPE_WEBHOOK_SECRET", "whsec_123")
    monkeypatch.setenv("STRIPE_PRICE_T2_MONTHLY", "price_t2_monthly")

    settings = Settings()

    assert settings.SECRET_KEY == "override-secret"
    assert settings.MONGO_URI == "mongodb://example:27017/override"
    assert settings.ENVIRONMENT == "production"
    assert settings.LOG_LEVEL == "debug"
    assert settings.SITE_ORIGIN == "https://example.com"
    assert settings.SENTRY_DSN == "https://public@example.com/1"
    assert settings.SENTRY_TRACES_SAMPLE_RATE == 0.25
    assert settings.SENTRY_SEND_DEFAULT_PII is True
    assert settings.TUTOR_ENABLED is True
    assert settings.TUTOR_BETA_USER_IDS == "fil-id"
    assert settings.TUTOR_MONTHLY_BUDGET_USD == 5
    assert settings.TUTOR_ANALYSIS_MODEL == "gpt-5.6-terra"
    assert settings.TUTOR_CACHED_INPUT_COST_PER_MILLION_USD == 0.2
    assert settings.OPENAI_API_KEY == "test-key"
    assert settings.STRIPE_SECRET_KEY == "sk_test_123"
    assert settings.STRIPE_PUBLISHABLE_KEY == "pk_test_123"
    assert settings.STRIPE_WEBHOOK_SECRET == "whsec_123"
    assert settings.STRIPE_PRICE_T2_MONTHLY == "price_t2_monthly"


def test_get_settings_cache_can_be_cleared_between_tests(monkeypatch):
    get_settings.cache_clear()
    original = get_settings()

    monkeypatch.setenv("SITE_ORIGIN", "https://cache-clear.example")
    get_settings.cache_clear()
    refreshed = get_settings()

    assert original is not refreshed
    assert refreshed.SITE_ORIGIN == "https://cache-clear.example"
