from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    SECRET_KEY: str = "dev-secret-change-me"
    MONGO_URI: str = "mongodb://localhost:27017/kriegspiel?replicaSet=rs0"
    ENVIRONMENT: str = "development"
    LOG_LEVEL: str = "info"
    SITE_ORIGIN: str = "http://localhost:5173"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
