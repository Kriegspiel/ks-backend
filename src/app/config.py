from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict
from app.version import APP_VERSION


class Settings(BaseSettings):
    APP_VERSION: str = APP_VERSION
    SECRET_KEY: str = "dev-secret-change-me"
    BOT_TOKEN_HMAC_SECRET: str = "dev-bot-token-hmac-change-me"
    BOT_TOKEN_CACHE_TTL_SECONDS: float = 3600.0
    MONGO_URI: str = "mongodb://localhost:27017/kriegspiel?replicaSet=rs0"
    ENVIRONMENT: str = "development"
    LOG_LEVEL: str = "info"
    SITE_ORIGIN: str = "http://localhost:5173"
    SENTRY_DSN: str | None = None
    SENTRY_TRACES_SAMPLE_RATE: float = 0.0
    SENTRY_SEND_DEFAULT_PII: bool = False
    TECH_REPORT_USERNAMES: str = "fil"
    STRIPE_SECRET_KEY: str | None = None
    STRIPE_PUBLISHABLE_KEY: str | None = None
    STRIPE_WEBHOOK_SECRET: str | None = None
    STRIPE_API_BASE: str = "https://api.stripe.com/v1"
    STRIPE_PRICE_T2_MONTHLY: str | None = None
    STRIPE_PRICE_T2_YEARLY: str | None = None
    STRIPE_PRICE_T3_MONTHLY: str | None = None
    STRIPE_PRICE_T3_YEARLY: str | None = None
    STRIPE_PRICE_T4_MONTHLY: str | None = None
    STRIPE_PRICE_T4_YEARLY: str | None = None

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
