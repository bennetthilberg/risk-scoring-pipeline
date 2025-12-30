import json
import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from services.scorer.processor import is_already_processed, mark_processed, process_message
from services.scorer.retry import send_to_dlq
from shared import ProcessingStatus, utcnow


def make_test_event(event_id: uuid.UUID | None = None, user_id: str | None = None) -> dict:
    """Create a test event with unique identifiers."""
    return {
        "event_id": str(event_id or uuid.uuid4()),
        "event_type": "signup",
        "user_id": user_id or f"test-user-{uuid.uuid4().hex[:8]}",
        "ts": datetime.now(UTC).isoformat(),
        "schema_version": 1,
        "payload": {
            "email_domain": "example.com",
            "country": "US",
            "device_id": f"device-{uuid.uuid4().hex[:8]}",
        },
    }


@pytest.mark.integration
class TestWorkerDeduplication:
    def test_is_already_processed_returns_false_for_new_event(self, db_session):
        test_uuid = uuid.uuid4()
        result = is_already_processed(test_uuid, db_session)
        assert result is False

    def test_is_already_processed_returns_true_for_processed_event(self, db_session):
        from shared.db import ProcessedEvent

        test_uuid = uuid.uuid4()
        processed = ProcessedEvent(
            event_id=test_uuid,
            processed_at=utcnow(),
            status=ProcessingStatus.SUCCESS.value,
        )
        db_session.add(processed)
        db_session.commit()

        result = is_already_processed(test_uuid, db_session)
        assert result is True

    def test_mark_processed_returns_true_for_new_event(self, db_session):
        test_uuid = uuid.uuid4()
        result = mark_processed(test_uuid, ProcessingStatus.SUCCESS, db_session)
        db_session.commit()

        assert result is True

        from shared.db import ProcessedEvent

        record = db_session.query(ProcessedEvent).filter_by(event_id=test_uuid).first()
        assert record is not None
        assert record.status == "success"

    def test_mark_processed_returns_false_for_duplicate(self, db_session):
        test_uuid = uuid.uuid4()
        mark_processed(test_uuid, ProcessingStatus.SUCCESS, db_session)
        db_session.commit()

        result = mark_processed(test_uuid, ProcessingStatus.SUCCESS, db_session)
        assert result is False

    def test_process_message_creates_score_and_marks_processed(self, db_session):
        test_event = make_test_event()

        msg = MagicMock()
        msg.value.return_value = json.dumps(test_event).encode("utf-8")

        success, should_retry = process_message(msg, db_session)

        assert success is True
        assert should_retry is False

        from shared.db import ProcessedEvent, RiskScore

        score = db_session.query(RiskScore).filter_by(user_id=test_event["user_id"]).first()
        assert score is not None
        assert 0.0 <= score.score <= 1.0
        assert score.band in ["low", "med", "high"]

        processed = (
            db_session.query(ProcessedEvent).filter_by(event_id=test_event["event_id"]).first()
        )
        assert processed is not None
        assert processed.status == "success"

    def test_process_message_skips_already_processed(self, db_session):
        from shared.db import ProcessedEvent

        test_event = make_test_event()

        processed = ProcessedEvent(
            event_id=test_event["event_id"],
            processed_at=utcnow(),
            status=ProcessingStatus.SUCCESS.value,
        )
        db_session.add(processed)
        db_session.commit()

        msg = MagicMock()
        msg.value.return_value = json.dumps(test_event).encode("utf-8")

        success, should_retry = process_message(msg, db_session)

        assert success is True
        assert should_retry is False

        from shared.db import RiskScore

        score = db_session.query(RiskScore).filter_by(user_id=test_event["user_id"]).first()
        assert score is None

    def test_multiple_scores_for_same_user(self, db_session):
        user_id = f"multi-score-user-{uuid.uuid4().hex[:8]}"

        msg1 = MagicMock()
        event1 = make_test_event(user_id=user_id)
        msg1.value.return_value = json.dumps(event1).encode("utf-8")

        msg2 = MagicMock()
        event2 = make_test_event(user_id=user_id)
        msg2.value.return_value = json.dumps(event2).encode("utf-8")

        process_message(msg1, db_session)
        process_message(msg2, db_session)

        from shared.db import RiskScore

        scores = db_session.query(RiskScore).filter_by(user_id=user_id).all()
        assert len(scores) == 2


@pytest.mark.integration
class TestDLQIntegration:
    def test_send_to_dlq_creates_db_entry(self, db_session):
        from shared.db import DLQEvent

        test_uuid = uuid.uuid4()
        unique_marker = f"test-marker-{uuid.uuid4().hex[:8]}"
        raw_payload = f'{{"event_id": "test", "marker": "{unique_marker}"}}'.encode()

        send_to_dlq(
            raw_payload=raw_payload,
            failure_reason=f"Test failure reason: {unique_marker}",
            db=db_session,
            event_id=test_uuid,
            retry_count=2,
        )

        entry = (
            db_session.query(DLQEvent)
            .filter(DLQEvent.failure_reason.contains(unique_marker))
            .first()
        )
        assert entry is not None
        assert entry.event_id == test_uuid
        assert unique_marker in entry.failure_reason
        assert entry.retry_count == 2
        assert unique_marker in entry.raw_payload

    def test_invalid_json_goes_to_dlq(self, db_session):
        from shared.db import DLQEvent

        unique_marker = f"invalid-json-{uuid.uuid4().hex[:8]}"
        invalid_payload = f"not valid json at all {unique_marker}"

        msg = MagicMock()
        msg.value.return_value = invalid_payload.encode()

        success, should_retry = process_message(msg, db_session)

        assert success is True
        assert should_retry is False

        entry = (
            db_session.query(DLQEvent).filter(DLQEvent.raw_payload.contains(unique_marker)).first()
        )
        assert entry is not None
        assert (
            "validation" in entry.failure_reason.lower()
            or "json" in entry.failure_reason.lower()
            or "deserialization" in entry.failure_reason.lower()
        )
        assert entry.event_id is None

    def test_unknown_event_type_goes_to_dlq(self, db_session):
        from shared.db import DLQEvent

        test_event = make_test_event()
        test_event["event_type"] = "unknown_type"
        unique_event_id = test_event["event_id"]

        msg = MagicMock()
        msg.value.return_value = json.dumps(test_event).encode("utf-8")

        success, should_retry = process_message(msg, db_session)

        assert success is True
        assert should_retry is False

        entry = (
            db_session.query(DLQEvent)
            .filter(DLQEvent.raw_payload.contains(unique_event_id))
            .first()
        )
        assert entry is not None
        assert entry.event_id is None

    def test_invalid_payload_goes_to_dlq(self, db_session):
        from shared.db import DLQEvent

        test_event = make_test_event()
        test_event["payload"]["email_domain"] = ""
        unique_event_id = test_event["event_id"]

        msg = MagicMock()
        msg.value.return_value = json.dumps(test_event).encode("utf-8")

        success, should_retry = process_message(msg, db_session)

        assert success is True
        assert should_retry is False

        entry = (
            db_session.query(DLQEvent)
            .filter(DLQEvent.raw_payload.contains(unique_event_id))
            .first()
        )
        assert entry is not None

    def test_multiple_dlq_entries_preserved(self, db_session):
        from shared.db import DLQEvent

        unique_marker = f"multi-dlq-{uuid.uuid4().hex[:8]}"

        for i in range(3):
            send_to_dlq(
                raw_payload=f'{{"test": {i}, "marker": "{unique_marker}"}}'.encode(),
                failure_reason=f"Error {i} - {unique_marker}",
                db=db_session,
                event_id=None,
                retry_count=i,
            )

        entries = (
            db_session.query(DLQEvent).filter(DLQEvent.failure_reason.contains(unique_marker)).all()
        )
        assert len(entries) == 3
