import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from services.api.routes import events, health, scores


def create_integration_app() -> FastAPI:
    app = FastAPI()
    app.include_router(health.router, tags=["health"])
    app.include_router(events.router, prefix="/events", tags=["events"])
    app.include_router(scores.router, prefix="/score", tags=["scores"])
    return app


@pytest.fixture
def integration_client(db_session, mock_producer):
    from services.api import dependencies

    original_db_factory = dependencies._db_session_factory
    original_producer = dependencies._kafka_producer

    dependencies._db_session_factory = lambda: db_session
    dependencies._kafka_producer = mock_producer

    app = create_integration_app()

    with TestClient(app) as test_client:
        yield test_client

    dependencies._db_session_factory = original_db_factory
    dependencies._kafka_producer = original_producer


@pytest.mark.integration
class TestAPIWithDatabase:
    def test_post_event_writes_to_db(self, integration_client, sample_signup_event, db_session):
        response = integration_client.post("/events", json=sample_signup_event)
        assert response.status_code == 202

        from shared.db import Event

        event = db_session.query(Event).filter_by(event_id=sample_signup_event["event_id"]).first()

        assert event is not None
        assert event.user_id == sample_signup_event["user_id"]
        assert event.event_type == "signup"
        assert event.raw_payload_hash is not None

    def test_post_event_idempotency_key_stored(
        self, integration_client, sample_signup_event, db_session
    ):
        response = integration_client.post("/events", json=sample_signup_event)
        assert response.status_code == 202

        from shared.db import Event

        event = db_session.query(Event).filter_by(event_id=sample_signup_event["event_id"]).first()

        assert event is not None
        assert len(event.raw_payload_hash) == 64

    def test_get_score_after_scoring(self, integration_client, db_session):
        from shared import utcnow
        from shared.db import RiskScore

        score = RiskScore(
            user_id="user-integration-test",
            score=0.55,
            band="med",
            computed_at=utcnow(),
            top_features_json={"feature1": 0.1},
            model_version="test-v1",
        )
        db_session.add(score)
        db_session.commit()

        response = integration_client.get("/score/user-integration-test")
        assert response.status_code == 200
        data = response.json()
        assert data["user_id"] == "user-integration-test"
        assert data["score"] == 0.55
        assert data["band"] == "med"


@pytest.mark.integration
class TestMigrations:
    def test_migrations_apply_cleanly(self, db_session):
        from shared.db import Event, ProcessedEvent, RiskScore

        assert Event.__tablename__ == "events"
        assert RiskScore.__tablename__ == "risk_scores"
        assert ProcessedEvent.__tablename__ == "processed_events"

    def test_event_table_has_unique_constraint(self, engine):
        from sqlalchemy import inspect

        inspector = inspect(engine)
        pk_constraint = inspector.get_pk_constraint("events")
        assert pk_constraint["constrained_columns"] == ["event_id"]

    def test_indexes_exist(self, engine):
        from sqlalchemy import inspect

        inspector = inspect(engine)

        events_indexes = {idx["name"] for idx in inspector.get_indexes("events")}
        assert "ix_events_user_id_ts" in events_indexes

        scores_indexes = {idx["name"] for idx in inspector.get_indexes("risk_scores")}
        assert "ix_risk_scores_user_id_computed_at" in scores_indexes


@pytest.mark.integration
class TestAPIIdempotency:
    def test_duplicate_event_returns_202(self, integration_client, sample_signup_event, db_session):
        response1 = integration_client.post("/events", json=sample_signup_event)
        assert response1.status_code == 202

        response2 = integration_client.post("/events", json=sample_signup_event)
        assert response2.status_code == 202

        data = response2.json()
        assert data["event_id"] == sample_signup_event["event_id"]
        assert data["status"] == "accepted"

    def test_duplicate_event_no_duplicate_db_record(
        self, integration_client, sample_signup_event, db_session
    ):
        integration_client.post("/events", json=sample_signup_event)
        integration_client.post("/events", json=sample_signup_event)

        from shared.db import Event

        events = db_session.query(Event).filter_by(event_id=sample_signup_event["event_id"]).all()
        assert len(events) == 1

    def test_duplicate_with_unpublished_retries_publish(
        self, integration_client, sample_signup_event, db_session, mock_producer
    ):
        from shared import utcnow
        from shared.db import Event

        event = Event(
            event_id=sample_signup_event["event_id"],
            user_id=sample_signup_event["user_id"],
            event_type="signup",
            ts=utcnow(),
            schema_version=1,
            payload_json=sample_signup_event["payload"],
            raw_payload_hash="abc123",
            accepted_at=utcnow(),
            published_at=None,
        )
        db_session.add(event)
        db_session.commit()

        mock_producer.produce.reset_mock()

        response = integration_client.post("/events", json=sample_signup_event)
        assert response.status_code == 202

        mock_producer.produce.assert_called_once()

    def test_published_event_not_republished(
        self, integration_client, sample_signup_event, db_session, mock_producer
    ):
        from shared import utcnow
        from shared.db import Event

        event = Event(
            event_id=sample_signup_event["event_id"],
            user_id=sample_signup_event["user_id"],
            event_type="signup",
            ts=utcnow(),
            schema_version=1,
            payload_json=sample_signup_event["payload"],
            raw_payload_hash="abc123",
            accepted_at=utcnow(),
            published_at=utcnow(),
        )
        db_session.add(event)
        db_session.commit()

        mock_producer.produce.reset_mock()

        response = integration_client.post("/events", json=sample_signup_event)
        assert response.status_code == 202

        mock_producer.produce.assert_not_called()
