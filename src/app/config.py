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
    OPENAI_API_KEY: str | None = None
    OPENAI_BASE_URL: str = "https://api.openai.com/v1"
    OPENAI_ANALYSIS_MODEL: str = "gpt-5.5"
    T3_REVIEW_OPENAI_ENABLED: bool = True
    T3_REVIEW_OPENAI_TIMEOUT_SECONDS: float = 20.0
    T3_REVIEW_OPENAI_MAX_OUTPUT_TOKENS: int = 12000
    T3_REVIEW_MCTS_MAX_ITERATIONS: int = 96
    T3_REVIEW_MCTS_TIME_BUDGET_SECONDS: float = 0.02

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
