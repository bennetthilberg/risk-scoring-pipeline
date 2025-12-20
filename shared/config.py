from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = Field(
        default="postgresql://postgres:postgres@localhost:5432/riskdb"
    )
    kafka_brokers: str = Field(default="localhost:9092")
    kafka_topic: str = Field(default="risk.events")
    dlq_topic: str = Field(default="risk.events.dlq")
    consumer_group: str = Field(default="risk-scorer")

    model_path: str = Field(default="models/model.pkl")

    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)

    worker_metrics_port: int = Field(default=9100)

    max_retries: int = Field(default=3)
    retry_base_delay_ms: int = Field(default=100)


@lru_cache
def get_settings() -> Settings:
    return Settings()
