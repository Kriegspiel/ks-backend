from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict
from app.version import APP_VERSION


class Settings(BaseSettings):
    APP_VERSION: str = APP_VERSION
    SECRET_KEY: str = "dev-secret-change-me"
    MONGO_URI: str = "mongodb://localhost:27017/kriegspiel?replicaSet=rs0"
    ENVIRONMENT: str = "development"
    LOG_LEVEL: str = "info"
    SITE_ORIGIN: str = "http://localhost:5173"
    BOT_REGISTRATION_KEY: str = "dev-bot-registration-key"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
