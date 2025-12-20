import pytest

from shared.config import Settings


@pytest.mark.unit
class TestSettings:
    def test_default_values(self):
        settings = Settings()
        assert settings.database_url == "postgresql://postgres:postgres@localhost:5432/riskdb"
        assert settings.kafka_brokers == "localhost:9092"
        assert settings.kafka_topic == "risk.events"
        assert settings.dlq_topic == "risk.events.dlq"
        assert settings.consumer_group == "risk-scorer"

    def test_default_api_settings(self):
        settings = Settings()
        assert settings.api_host == "0.0.0.0"
        assert settings.api_port == 8000

    def test_default_retry_settings(self):
        settings = Settings()
        assert settings.max_retries == 3
        assert settings.retry_base_delay_ms == 100

    def test_override_from_env(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://test:test@db:5432/testdb")
        monkeypatch.setenv("KAFKA_BROKERS", "kafka:9092")
        monkeypatch.setenv("API_PORT", "9000")

        settings = Settings()
        assert settings.database_url == "postgresql://test:test@db:5432/testdb"
        assert settings.kafka_brokers == "kafka:9092"
        assert settings.api_port == 9000

    def test_extra_env_vars_ignored(self, monkeypatch):
        monkeypatch.setenv("UNKNOWN_SETTING", "should_be_ignored")
        settings = Settings()
        assert not hasattr(settings, "unknown_setting")
