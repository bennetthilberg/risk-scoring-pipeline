"""
Smoke tests that validate the entire quickstart flow works end-to-end.

These tests ensure that anyone following the quickstart guide will have success.
They test the full integration: Docker services → API → Kafka → Worker → DB → Score Query.

Run with: pytest tests/smoke/ -v
Requires: docker compose deps (postgres, redpanda) running
"""

import json
import os
import uuid
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture(scope="module")
def quickstart_db_url() -> str:
    return os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/riskdb")


@pytest.fixture(scope="module")
def quickstart_kafka_brokers() -> str:
    return os.environ.get("KAFKA_BROKERS", "localhost:9092")


@pytest.fixture(scope="module")
def quickstart_engine(quickstart_db_url: str):
    from sqlalchemy import create_engine

    engine = create_engine(quickstart_db_url, echo=False)
    yield engine
    engine.dispose()


@pytest.fixture(scope="module")
def quickstart_session_factory(quickstart_engine):
    from sqlalchemy.orm import sessionmaker

    return sessionmaker(bind=quickstart_engine, autocommit=False, autoflush=False)


@pytest.fixture
def quickstart_db_session(quickstart_session_factory):
    session = quickstart_session_factory()
    yield session
    session.rollback()
    session.close()


@pytest.fixture
def quickstart_producer(quickstart_kafka_brokers: str):
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": quickstart_kafka_brokers})
    yield producer
    producer.flush()


class TestQuickstartValidation:
    """
    These tests validate the quickstart guide works correctly.
    Each test corresponds to a step a user would take following the quickstart.
    """

    def test_database_connection_works(self, quickstart_engine):
        """Verify PostgreSQL connection is functional."""
        from sqlalchemy import text

        with quickstart_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1

    def test_database_schema_exists(self, quickstart_db_session):
        """Verify all required tables exist after running migrations."""
        from sqlalchemy import inspect

        inspector = inspect(quickstart_db_session.bind)
        tables = inspector.get_table_names()

        required_tables = ["events", "risk_scores", "processed_events", "dlq_events"]
        for table in required_tables:
            assert table in tables, f"Required table '{table}' not found in database"

    def test_database_constraints_exist(self, quickstart_db_session):
        """Verify critical unique constraints exist for idempotency."""
        from sqlalchemy import inspect

        inspector = inspect(quickstart_db_session.bind)

        events_unique_constraints = inspector.get_unique_constraints("events")
        events_indexes = inspector.get_indexes("events")

        event_id_unique = any(
            "event_id" in (c.get("column_names", []) or []) for c in events_unique_constraints
        ) or any(
            idx.get("unique") and "event_id" in (idx.get("column_names", []) or [])
            for idx in events_indexes
        )
        assert event_id_unique, "events.event_id should have a unique constraint"

    def test_kafka_connection_works(self, quickstart_kafka_brokers: str):
        """Verify Kafka/Redpanda connection is functional."""
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": quickstart_kafka_brokers})
        metadata = admin.list_topics(timeout=10)
        assert metadata is not None, "Could not connect to Kafka"

    def test_kafka_topics_exist(self, quickstart_kafka_brokers: str):
        """Verify required Kafka topics exist."""
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": quickstart_kafka_brokers})
        metadata = admin.list_topics(timeout=10)
        topic_names = set(metadata.topics.keys())

        assert "risk.events" in topic_names, "risk.events topic not found"
        assert "risk.events.dlq" in topic_names, "risk.events.dlq topic not found"


class TestAPIFunctionality:
    """Tests that verify the API service works correctly."""

    @pytest.fixture
    def test_app(self, quickstart_session_factory, quickstart_producer):
        from services.api import dependencies
        from services.api.main import create_app

        dependencies._db_session_factory = quickstart_session_factory
        dependencies._kafka_producer = quickstart_producer

        return create_app()

    @pytest.mark.asyncio
    async def test_health_endpoint(self, test_app):
        """Verify health endpoint returns correct response."""
        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as client:
            response = await client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "version" in data

    @pytest.mark.asyncio
    async def test_metrics_endpoint(self, test_app):
        """Verify metrics endpoint returns Prometheus format."""
        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as client:
            response = await client.get("/metrics")

        assert response.status_code == 200
        assert "text/plain" in response.headers.get("content-type", "")

    @pytest.mark.asyncio
    async def test_event_ingestion(self, test_app, quickstart_db_session):
        """Verify events can be ingested through the API."""
        test_event_id = str(uuid.uuid4())
        test_user_id = f"smoke-test-user-{uuid.uuid4().hex[:8]}"

        event = {
            "event_id": test_event_id,
            "event_type": "signup",
            "user_id": test_user_id,
            "ts": datetime.now(UTC).isoformat(),
            "schema_version": 1,
            "payload": {
                "email_domain": "test.com",
                "country": "US",
                "device_id": "test-device",
            },
        }

        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as client:
            response = await client.post("/events", json=event)

        assert response.status_code == 202
        data = response.json()
        assert data["event_id"] == test_event_id
        assert data["status"] == "accepted"

        from shared.db import Event

        db_event = quickstart_db_session.query(Event).filter_by(event_id=test_event_id).first()
        assert db_event is not None
        assert db_event.user_id == test_user_id

    @pytest.mark.asyncio
    async def test_event_validation_rejects_invalid(self, test_app):
        """Verify invalid events are rejected with proper error."""
        invalid_event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "invalid_type",
            "user_id": "test-user",
            "ts": datetime.now(UTC).isoformat(),
            "schema_version": 1,
            "payload": {},
        }

        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as client:
            response = await client.post("/events", json=invalid_event)

        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_score_endpoint_returns_404_for_unknown_user(self, test_app):
        """Verify score endpoint returns 404 for users with no scores."""
        unknown_user = f"nonexistent-{uuid.uuid4().hex}"

        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as client:
            response = await client.get(f"/score/{unknown_user}")

        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_dlq_endpoint_returns_list(self, test_app):
        """Verify DLQ endpoint returns a list."""
        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as client:
            response = await client.get("/dlq")

        assert response.status_code == 200
        data = response.json()
        assert "entries" in data
        assert "total" in data
        assert isinstance(data["entries"], list)


class TestWorkerFunctionality:
    """Tests that verify the scoring worker works correctly."""

    def test_worker_can_process_message(
        self, quickstart_db_session, quickstart_producer, quickstart_kafka_brokers
    ):
        """Verify worker can process a message and create a score."""
        from unittest.mock import MagicMock

        from services.scorer.processor import process_message
        from shared.db import Event, RiskScore

        test_user_id = f"worker-test-{uuid.uuid4().hex[:8]}"
        test_event_id = uuid.uuid4()

        db_event = Event(
            event_id=test_event_id,
            user_id=test_user_id,
            event_type="signup",
            ts=datetime.now(UTC),
            schema_version=1,
            payload_json={
                "email_domain": "test.com",
                "country": "US",
                "device_id": "test",
            },
            raw_payload_hash="test_hash",
            accepted_at=datetime.now(UTC),
        )
        quickstart_db_session.add(db_event)
        quickstart_db_session.commit()

        event_data = {
            "event_id": str(test_event_id),
            "event_type": "signup",
            "user_id": test_user_id,
            "ts": datetime.now(UTC).isoformat(),
            "schema_version": 1,
            "payload": {
                "email_domain": "test.com",
                "country": "US",
                "device_id": "test",
            },
        }

        mock_msg = MagicMock()
        mock_msg.value.return_value = json.dumps(event_data).encode("utf-8")

        success, _ = process_message(mock_msg, quickstart_db_session)

        assert success, "Worker should process message successfully"

        score = quickstart_db_session.query(RiskScore).filter_by(user_id=test_user_id).first()
        assert score is not None, "Score should be created"
        assert 0.0 <= score.score <= 1.0
        assert score.band in ["low", "med", "high"]


class TestFeatureComputation:
    """Tests that verify feature computation works correctly."""

    def test_features_computed_for_user_with_events(self, quickstart_db_session):
        """Verify features are computed correctly from user events."""
        from services.scorer.features import compute_features
        from shared.db import Event

        test_user_id = f"feature-test-{uuid.uuid4().hex[:8]}"

        events = [
            Event(
                event_id=uuid.uuid4(),
                user_id=test_user_id,
                event_type="signup",
                ts=datetime.now(UTC),
                schema_version=1,
                payload_json={"email_domain": "gmail.com", "country": "US", "device_id": "d1"},
                raw_payload_hash=f"hash_{i}",
                accepted_at=datetime.now(UTC),
            )
            for i in range(3)
        ]
        for event in events:
            quickstart_db_session.add(event)
        quickstart_db_session.commit()

        features = compute_features(test_user_id, quickstart_db_session)

        assert isinstance(features, dict)
        expected_features = [
            "txn_count_24h",
            "txn_amount_sum_24h",
            "failed_logins_1h",
            "account_age_days",
            "unique_countries_7d",
            "avg_txn_amount_30d",
        ]
        for feature in expected_features:
            assert feature in features, f"Missing feature: {feature}"


class TestModelArtifacts:
    """Tests that verify model artifacts are correct."""

    def test_model_files_exist(self):
        """Verify model files exist in the expected location."""
        import os

        model_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "models"
        )

        model_path = os.path.join(model_dir, "model.pkl")
        metadata_path = os.path.join(model_dir, "metadata.json")

        if os.path.exists(model_dir):
            assert os.path.exists(model_path), "model.pkl not found"
            assert os.path.exists(metadata_path), "metadata.json not found"

    def test_model_metadata_has_required_fields(self):
        """Verify model metadata contains required fields."""
        import json
        import os

        model_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "models"
        )
        metadata_path = os.path.join(model_dir, "metadata.json")

        if not os.path.exists(metadata_path):
            pytest.skip("Model not trained yet")

        with open(metadata_path) as f:
            metadata = json.load(f)

        required_fields = ["version", "feature_order", "band_thresholds", "params_hash"]
        for field in required_fields:
            assert field in metadata, f"Missing metadata field: {field}"

    def test_model_can_load_and_predict(self):
        """Verify model can be loaded and produce predictions."""
        import os

        from shared.model import RiskModel

        model_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "models"
        )
        model_path = os.path.join(model_dir, "model.pkl")

        if not os.path.exists(model_path):
            pytest.skip("Model not trained yet")

        model = RiskModel.load(model_dir)
        assert model is not None

        test_features = {
            "txn_count_24h": 5.0,
            "txn_amount_sum_24h": 500.0,
            "failed_logins_1h": 0.0,
            "account_age_days": 30.0,
            "unique_countries_7d": 1.0,
            "avg_txn_amount_30d": 100.0,
        }

        score, band, top_features = model.predict(test_features)

        assert 0.0 <= score <= 1.0
        assert band.value in ["low", "med", "high"]
        assert isinstance(top_features, dict)


class TestEndToEndFlow:
    """
    Complete end-to-end test that validates the full flow.
    This is the "big picture" test that ensures quickstart works.
    """

    @pytest.fixture
    def test_app(self, quickstart_session_factory, quickstart_producer):
        from services.api import dependencies
        from services.api.main import create_app

        dependencies._db_session_factory = quickstart_session_factory
        dependencies._kafka_producer = quickstart_producer

        return create_app()

    @pytest.mark.asyncio
    async def test_full_quickstart_flow(self, test_app, quickstart_db_session, quickstart_producer):
        """
        Test the complete quickstart flow:
        1. Send event via API
        2. Event is stored in DB
        3. Event is published to Kafka
        4. Worker processes event (simulated)
        5. Score is created
        6. Score is queryable via API
        """
        from services.scorer.processor import process_message
        from shared.db import Event, RiskScore

        test_user_id = f"e2e-flow-{uuid.uuid4().hex[:8]}"

        signup_event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "signup",
            "user_id": test_user_id,
            "ts": datetime.now(UTC).isoformat(),
            "schema_version": 1,
            "payload": {
                "email_domain": "gmail.com",
                "country": "US",
                "device_id": "e2e-device",
            },
        }

        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as client:
            response = await client.post("/events", json=signup_event)
            assert response.status_code == 202

        quickstart_producer.flush()

        db_event = (
            quickstart_db_session.query(Event).filter_by(event_id=signup_event["event_id"]).first()
        )
        assert db_event is not None, "Event should be stored in DB"
        assert db_event.published_at is not None, "Event should be published to Kafka"

        from unittest.mock import MagicMock

        mock_msg = MagicMock()
        mock_msg.value.return_value = json.dumps(signup_event).encode("utf-8")

        success, _ = process_message(mock_msg, quickstart_db_session)
        assert success, "Worker should process event successfully"

        score = quickstart_db_session.query(RiskScore).filter_by(user_id=test_user_id).first()
        assert score is not None, "Score should be created"
        assert 0.0 <= score.score <= 1.0
        assert score.band in ["low", "med", "high"]

        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as client:
            response = await client.get(f"/score/{test_user_id}")
            assert response.status_code == 200
            data = response.json()
            assert data["user_id"] == test_user_id
            assert data["score"] == score.score
            assert data["band"] == score.band

    @pytest.mark.asyncio
    async def test_idempotency_works(self, test_app, quickstart_db_session):
        """
        Verify idempotency: sending the same event twice should not create duplicates.
        """
        from shared.db import Event

        test_event_id = str(uuid.uuid4())
        test_user_id = f"idempotent-{uuid.uuid4().hex[:8]}"

        event = {
            "event_id": test_event_id,
            "event_type": "signup",
            "user_id": test_user_id,
            "ts": datetime.now(UTC).isoformat(),
            "schema_version": 1,
            "payload": {
                "email_domain": "test.com",
                "country": "US",
                "device_id": "test-device",
            },
        }

        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as client:
            response1 = await client.post("/events", json=event)
            response2 = await client.post("/events", json=event)

        assert response1.status_code == 202
        assert response2.status_code == 202

        count = quickstart_db_session.query(Event).filter_by(event_id=test_event_id).count()
        assert count == 1, "Duplicate events should not create multiple rows"
