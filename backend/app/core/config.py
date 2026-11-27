from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    app_name: str = "FlowForge"
    app_env: str = "development"
    log_level: str = "INFO"
    database_url: str = "postgresql+psycopg://flowforge:flowforge_dev@localhost:5432/flowforge"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    """Return cached application settings."""
    return Settings()
