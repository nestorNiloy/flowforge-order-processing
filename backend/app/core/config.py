from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    app_name: str = "FlowForge"
    app_env: str = "development"
    log_level: str = "INFO"
    database_url: str = "postgresql+psycopg://flowforge:flowforge_dev@localhost:5432/flowforge"
    rabbitmq_url: str = "amqp://flowforge:flowforge_dev@localhost:5672/%2F"
    outbox_batch_size: int = 10
    outbox_poll_interval_seconds: float = 2.0
    outbox_lease_seconds: int = 60
    publisher_reconnect_seconds: float = 5.0
    worker_reconnect_seconds: float = 5.0

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    """Return cached application settings."""
    return Settings()
