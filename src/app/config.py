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
    TUTOR_ENABLED: bool = False
    TUTOR_BETA_USER_IDS: str = ""
    TUTOR_MONTHLY_BUDGET_USD: float = 5.0
    TUTOR_MIN_COMPLETED_TURNS: int = 4
    TUTOR_ANALYSIS_MODEL: str = "gpt-5.6-terra"
    TUTOR_ANALYSIS_VERSION: str = "tutor-evidence-v1"
    TUTOR_PROMPT_VERSION: str = "tutor-private-beta-v1"
    TUTOR_REASONING_EFFORT: str = "medium"
    TUTOR_MAX_OUTPUT_TOKENS: int = 2400
    TUTOR_OPENAI_TIMEOUT_SECONDS: float = 45.0
    TUTOR_INPUT_COST_PER_MILLION_USD: float = 2.5
    TUTOR_CACHED_INPUT_COST_PER_MILLION_USD: float = 0.25
    TUTOR_OUTPUT_COST_PER_MILLION_USD: float = 15.0
    OPENAI_API_KEY: str | None = None
    OPENAI_BASE_URL: str = "https://api.openai.com/v1"
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
