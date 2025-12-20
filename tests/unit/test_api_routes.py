from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI, status
from fastapi.testclient import TestClient

from services.api.routes import events, health, scores


def create_test_app() -> FastAPI:
    app = FastAPI()
    app.include_router(health.router, tags=["health"])
    app.include_router(events.router, prefix="/events", tags=["events"])
    app.include_router(scores.router, prefix="/score", tags=["scores"])
    return app


@pytest.fixture
def mock_db_session():
    session = MagicMock()
    session.add = MagicMock()
    session.commit = MagicMock()
    session.query = MagicMock()
    return session


@pytest.fixture
def mock_producer():
    producer = MagicMock()
    producer.produce = MagicMock()
    producer.poll = MagicMock()
    return producer


@pytest.fixture
def client(mock_db_session, mock_producer):
    from services.api import dependencies

    original_db_factory = dependencies._db_session_factory
    original_producer = dependencies._kafka_producer

    dependencies._db_session_factory = lambda: mock_db_session
    dependencies._kafka_producer = mock_producer

    app = create_test_app()

    with TestClient(app) as test_client:
        yield test_client

    dependencies._db_session_factory = original_db_factory
    dependencies._kafka_producer = original_producer


@pytest.mark.unit
class TestHealthEndpoint:
    def test_health_check_returns_healthy(self, client):
        response = client.get("/health")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["status"] == "healthy"
        assert "version" in data


@pytest.mark.unit
class TestEventsEndpoint:
    def test_post_valid_signup_event(self, client, sample_signup_event, mock_db_session):
        response = client.post("/events", json=sample_signup_event)
        assert response.status_code == status.HTTP_202_ACCEPTED
        data = response.json()
        assert data["event_id"] == sample_signup_event["event_id"]
        assert data["status"] == "accepted"
        mock_db_session.add.assert_called_once()
        mock_db_session.commit.assert_called()

    def test_post_valid_login_event(self, client, sample_login_event, mock_db_session):
        response = client.post("/events", json=sample_login_event)
        assert response.status_code == status.HTTP_202_ACCEPTED
        data = response.json()
        assert data["status"] == "accepted"

    def test_post_valid_transaction_event(self, client, sample_transaction_event, mock_db_session):
        response = client.post("/events", json=sample_transaction_event)
        assert response.status_code == status.HTTP_202_ACCEPTED
        data = response.json()
        assert data["status"] == "accepted"

    def test_post_invalid_event_type(self, client, fixed_uuid, fixed_timestamp):
        event_data = {
            "event_id": str(fixed_uuid),
            "event_type": "invalid_type",
            "user_id": "user-001",
            "ts": fixed_timestamp.isoformat(),
            "schema_version": 1,
            "payload": {},
        }
        response = client.post("/events", json=event_data)
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_post_missing_required_field(self, client, fixed_uuid, fixed_timestamp):
        event_data = {
            "event_id": str(fixed_uuid),
            "event_type": "signup",
            "ts": fixed_timestamp.isoformat(),
            "schema_version": 1,
            "payload": {"email_domain": "ex.com", "country": "US", "device_id": "d1"},
        }
        response = client.post("/events", json=event_data)
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_post_invalid_payload(self, client, fixed_uuid, fixed_timestamp):
        event_data = {
            "event_id": str(fixed_uuid),
            "event_type": "signup",
            "user_id": "user-001",
            "ts": fixed_timestamp.isoformat(),
            "schema_version": 1,
            "payload": {"email_domain": "", "country": "US", "device_id": "d1"},
        }
        response = client.post("/events", json=event_data)
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_kafka_publish_called(self, client, sample_signup_event, mock_producer):
        client.post("/events", json=sample_signup_event)
        mock_producer.produce.assert_called_once()
        call_kwargs = mock_producer.produce.call_args[1]
        assert call_kwargs["topic"] == "risk.events"
        assert call_kwargs["key"] == sample_signup_event["user_id"].encode("utf-8")


@pytest.mark.unit
class TestScoreEndpoint:
    def test_get_score_not_found(self, client, mock_db_session):
        mock_db_session.query.return_value.filter.return_value.order_by.return_value.first.return_value = (
            None
        )
        response = client.get("/score/user-unknown")
        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_get_score_found(self, client, mock_db_session, fixed_timestamp):
        from shared.db import RiskScore

        mock_score = MagicMock(spec=RiskScore)
        mock_score.user_id = "user-001"
        mock_score.score = 0.45
        mock_score.band = "med"
        mock_score.computed_at = fixed_timestamp
        mock_score.top_features_json = {"txn_count_24h": 0.15}

        mock_db_session.query.return_value.filter.return_value.order_by.return_value.first.return_value = (
            mock_score
        )

        response = client.get("/score/user-001")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["user_id"] == "user-001"
        assert data["score"] == 0.45
        assert data["band"] == "med"
