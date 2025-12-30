"""
Pytest fixtures for the risk scoring pipeline.

Fixture organization:
- Deterministic test data (UUIDs, timestamps)
- Database fixtures (engine, session, migrations)
- Kafka fixtures (admin, producer, consumer, topics)
- FastAPI app fixtures (app, client)

Usage:
- Unit tests: use deterministic fixtures only
- Integration tests: use DB and/or Kafka fixtures
- E2E tests: use all fixtures together
"""

import contextlib
import os
import uuid
from collections.abc import Generator
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

# ============================================================================
# Environment setup for tests
# ============================================================================

# Set test environment variables before importing app modules
os.environ.setdefault("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/riskdb")
os.environ.setdefault("KAFKA_BROKERS", "localhost:9092")
os.environ.setdefault("KAFKA_TOPIC", "risk.events")
os.environ.setdefault("DLQ_TOPIC", "risk.events.dlq")


# ============================================================================
# Deterministic test data fixtures
# ============================================================================


@pytest.fixture
def fixed_uuid() -> uuid.UUID:
    """A fixed UUID for deterministic testing."""
    return uuid.UUID("12345678-1234-5678-1234-567812345678")


@pytest.fixture
def fixed_user_id() -> str:
    """A fixed user ID for deterministic testing."""
    return "user-test-001"


@pytest.fixture
def fixed_timestamp() -> datetime:
    """A fixed UTC timestamp for deterministic testing."""
    return datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def sample_signup_event(
    fixed_uuid: uuid.UUID, fixed_user_id: str, fixed_timestamp: datetime
) -> dict:
    """A sample signup event payload."""
    return {
        "event_id": str(fixed_uuid),
        "event_type": "signup",
        "user_id": fixed_user_id,
        "ts": fixed_timestamp.isoformat(),
        "schema_version": 1,
        "payload": {
            "email_domain": "example.com",
            "country": "US",
            "device_id": "device-abc-123",
        },
    }


@pytest.fixture
def sample_login_event(fixed_user_id: str, fixed_timestamp: datetime) -> dict:
    """A sample login event payload."""
    return {
        "event_id": str(uuid.UUID("22345678-1234-5678-1234-567812345678")),
        "event_type": "login",
        "user_id": fixed_user_id,
        "ts": fixed_timestamp.isoformat(),
        "schema_version": 1,
        "payload": {
            "ip": "192.168.1.100",
            "success": True,
            "device_id": "device-abc-123",
        },
    }


@pytest.fixture
def sample_transaction_event(fixed_user_id: str, fixed_timestamp: datetime) -> dict:
    """A sample transaction event payload."""
    return {
        "event_id": str(uuid.UUID("32345678-1234-5678-1234-567812345678")),
        "event_type": "transaction",
        "user_id": fixed_user_id,
        "ts": fixed_timestamp.isoformat(),
        "schema_version": 1,
        "payload": {
            "amount": 150.00,
            "currency": "USD",
            "merchant": "Test Merchant",
            "country": "US",
        },
    }


# ============================================================================
# Database fixtures (integration tests)
# ============================================================================


@pytest.fixture(scope="session")
def db_url() -> str:
    """Database URL from environment."""
    return os.environ["DATABASE_URL"]


@pytest.fixture(scope="session")
def engine(db_url: str):
    """SQLAlchemy engine for integration tests.

    Note: Only import SQLAlchemy in integration tests to avoid
    import errors in unit tests that don't need it.
    """
    from sqlalchemy import create_engine

    engine = create_engine(db_url, echo=False)
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def migrate_db(engine) -> None:
    """Run Alembic migrations once per test session.

    This ensures the database schema is up to date before running
    integration tests.
    """
    from alembic import command
    from alembic.config import Config

    # Find alembic.ini relative to repo root
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    alembic_cfg = Config(os.path.join(repo_root, "alembic.ini"))
    alembic_cfg.set_main_option("script_location", os.path.join(repo_root, "migrations"))

    command.upgrade(alembic_cfg, "head")


@pytest.fixture
def db_session(engine, migrate_db) -> Generator:
    """Database session with nested transaction isolation.

    Uses a connection-level transaction with SAVEPOINT for test isolation.
    All changes are rolled back after each test, including commits.
    """
    from sqlalchemy.orm import Session

    connection = engine.connect()
    transaction = connection.begin()

    session = Session(bind=connection, join_transaction_mode="create_savepoint")

    yield session

    session.close()
    transaction.rollback()
    connection.close()


# ============================================================================
# Kafka fixtures (integration tests)
# ============================================================================


@pytest.fixture(scope="session")
def kafka_brokers() -> str:
    """Kafka broker address from environment."""
    return os.environ["KAFKA_BROKERS"]


@pytest.fixture(scope="session")
def kafka_admin(kafka_brokers: str):
    """Kafka AdminClient for topic management."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": kafka_brokers})
    yield admin


@pytest.fixture
def test_topic_suffix() -> str:
    """Unique suffix for test topics to avoid collisions."""
    return str(uuid.uuid4())[:8]


@pytest.fixture
def test_topics(test_topic_suffix: str) -> dict[str, str]:
    """Unique topic names for this test run."""
    return {
        "events": f"risk.events.test.{test_topic_suffix}",
        "dlq": f"risk.events.dlq.test.{test_topic_suffix}",
    }


@pytest.fixture
def create_test_topics(
    kafka_admin, test_topics: dict[str, str]
) -> Generator[dict[str, str], None, None]:
    """Create test topics and clean up after test.

    Yields the topic names dict for use in tests.
    """
    from confluent_kafka.admin import NewTopic

    topics_to_create = [
        NewTopic(test_topics["events"], num_partitions=3, replication_factor=1),
        NewTopic(test_topics["dlq"], num_partitions=1, replication_factor=1),
    ]

    # Create topics
    futures = kafka_admin.create_topics(topics_to_create)
    for _topic, future in futures.items():
        with contextlib.suppress(Exception):
            future.result(timeout=10)  # Topic may already exist

    yield test_topics

    # Cleanup: delete test topics (best effort)
    delete_futures = kafka_admin.delete_topics(list(test_topics.values()))
    for _topic, future in delete_futures.items():
        with contextlib.suppress(Exception):
            future.result(timeout=10)  # Ignore cleanup errors


@pytest.fixture
def kafka_producer(kafka_brokers: str):
    """Kafka producer for sending test messages."""
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": kafka_brokers})
    yield producer
    producer.flush()


@pytest.fixture
def kafka_consumer(kafka_brokers: str, test_topics: dict[str, str]):
    """Kafka consumer for reading test messages.

    Configured with a unique group ID and to read from the beginning.
    """
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": kafka_brokers,
            "group.id": f"test-consumer-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([test_topics["events"]])
    yield consumer
    consumer.close()


# ============================================================================
# FastAPI app fixtures
# ============================================================================


@pytest.fixture
def mock_producer() -> MagicMock:
    """Mock Kafka producer for unit tests."""
    producer = MagicMock()
    producer.produce = MagicMock()
    producer.flush = MagicMock()
    return producer


@pytest.fixture
def mock_db_session() -> MagicMock:
    """Mock database session for unit tests."""
    session = MagicMock()
    session.add = MagicMock()
    session.commit = MagicMock()
    session.rollback = MagicMock()
    session.close = MagicMock()
    return session


@pytest.fixture
def app(mock_db_session, mock_producer):
    """FastAPI app with mocked dependencies for unit tests."""
    from services.api.dependencies import set_db_session_factory, set_kafka_producer
    from services.api.main import create_app

    set_db_session_factory(lambda: mock_db_session)
    set_kafka_producer(mock_producer)

    return create_app()


@pytest.fixture
async def client(app):
    """Async HTTP client for testing the API."""
    from httpx import ASGITransport, AsyncClient

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client
