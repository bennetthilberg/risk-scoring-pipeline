import json
import time
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from services.api.routes import events, health, scores
from shared import utcnow


def create_e2e_app() -> FastAPI:
    app = FastAPI()
    app.include_router(health.router, tags=["health"])
    app.include_router(events.router, prefix="/events", tags=["events"])
    app.include_router(scores.router, prefix="/score", tags=["scores"])
    return app


@pytest.mark.e2e
class TestFullPipeline:
    def test_end_to_end_event_to_score(
        self,
        db_session,
        create_test_topics,
        kafka_producer,
        kafka_consumer,
    ):
        from services.api import dependencies
        from services.scorer.processor import process_message
        from shared.db import Event, RiskScore

        dependencies._db_session_factory = lambda: db_session
        dependencies._kafka_producer = kafka_producer

        app = create_e2e_app()

        test_user_id = f"e2e-user-{uuid.uuid4().hex[:8]}"
        test_event_id = str(uuid.uuid4())

        signup_event = {
            "event_id": test_event_id,
            "event_type": "signup",
            "user_id": test_user_id,
            "ts": utcnow().isoformat(),
            "schema_version": 1,
            "payload": {
                "email_domain": "example.com",
                "country": "US",
                "device_id": "device-e2e-001",
            },
        }

        with TestClient(app) as client:
            response = client.post("/events", json=signup_event)
            assert response.status_code == 202

        kafka_producer.flush(timeout=5)

        db_event = db_session.query(Event).filter_by(event_id=test_event_id).first()
        assert db_event is not None
        assert db_event.user_id == test_user_id

        timeout = 10
        start = time.time()
        msg = None
        while time.time() - start < timeout:
            msg = kafka_consumer.poll(timeout=1.0)
            if msg is not None and msg.error() is None:
                break

        assert msg is not None, "No message received from Kafka"
        assert msg.error() is None

        msg_data = json.loads(msg.value().decode("utf-8"))
        assert msg_data["user_id"] == test_user_id

        process_message(msg, db_session)

        score = db_session.query(RiskScore).filter_by(user_id=test_user_id).first()
        assert score is not None
        assert 0.0 <= score.score <= 1.0
        assert score.band in ["low", "med", "high"]

        with TestClient(app) as client:
            response = client.get(f"/score/{test_user_id}")
            assert response.status_code == 200
            data = response.json()
            assert data["user_id"] == test_user_id
            assert data["score"] == score.score

    def test_multiple_events_for_user(
        self,
        db_session,
        create_test_topics,
        kafka_producer,
    ):
        from services.api import dependencies
        from shared.db import RiskScore

        dependencies._db_session_factory = lambda: db_session
        dependencies._kafka_producer = kafka_producer

        app = create_e2e_app()

        test_user_id = f"e2e-multi-{uuid.uuid4().hex[:8]}"

        event_types = ["signup", "login", "transaction"]
        for event_type in event_types:
            if event_type == "signup":
                payload = {"email_domain": "ex.com", "country": "US", "device_id": "d1"}
            elif event_type == "login":
                payload = {"ip": "192.168.1.1", "success": True, "device_id": "d1"}
            else:
                payload = {"amount": 100.0, "currency": "USD", "merchant": "Shop", "country": "US"}

            event = {
                "event_id": str(uuid.uuid4()),
                "event_type": event_type,
                "user_id": test_user_id,
                "ts": utcnow().isoformat(),
                "schema_version": 1,
                "payload": payload,
            }

            with TestClient(app) as client:
                response = client.post("/events", json=event)
                assert response.status_code == 202

        kafka_producer.flush(timeout=5)

        scores = db_session.query(RiskScore).filter_by(user_id=test_user_id).all()
        assert len(scores) == 0

    def test_health_check(self):
        app = create_e2e_app()
        with TestClient(app) as client:
            response = client.get("/health")
            assert response.status_code == 200
            assert response.json()["status"] == "healthy"
