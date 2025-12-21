import json
from unittest.mock import MagicMock

import pytest

from services.scorer.processor import (
    is_already_processed,
    mark_processed,
    process_message,
)
from shared import ProcessingStatus, utcnow


@pytest.mark.integration
class TestWorkerDeduplication:
    def test_is_already_processed_returns_false_for_new_event(self, db_session, fixed_uuid):
        result = is_already_processed(fixed_uuid, db_session)
        assert result is False

    def test_is_already_processed_returns_true_for_processed_event(self, db_session, fixed_uuid):
        from shared.db import ProcessedEvent

        processed = ProcessedEvent(
            event_id=fixed_uuid,
            processed_at=utcnow(),
            status=ProcessingStatus.SUCCESS.value,
        )
        db_session.add(processed)
        db_session.commit()

        result = is_already_processed(fixed_uuid, db_session)
        assert result is True

    def test_mark_processed_returns_true_for_new_event(self, db_session, fixed_uuid):
        result = mark_processed(fixed_uuid, ProcessingStatus.SUCCESS, db_session)
        db_session.commit()

        assert result is True

        from shared.db import ProcessedEvent

        record = db_session.query(ProcessedEvent).filter_by(event_id=fixed_uuid).first()
        assert record is not None
        assert record.status == "success"

    def test_mark_processed_returns_false_for_duplicate(self, db_session, fixed_uuid):
        mark_processed(fixed_uuid, ProcessingStatus.SUCCESS, db_session)
        db_session.commit()

        result = mark_processed(fixed_uuid, ProcessingStatus.SUCCESS, db_session)
        assert result is False

    def test_process_message_creates_score_and_marks_processed(
        self, db_session, sample_signup_event
    ):
        msg = MagicMock()
        msg.value.return_value = json.dumps(sample_signup_event).encode("utf-8")

        result = process_message(msg, db_session)

        assert result is True

        from shared.db import ProcessedEvent, RiskScore

        score = (
            db_session.query(RiskScore).filter_by(user_id=sample_signup_event["user_id"]).first()
        )
        assert score is not None
        assert 0.0 <= score.score <= 1.0
        assert score.band in ["low", "med", "high"]

        processed = (
            db_session.query(ProcessedEvent)
            .filter_by(event_id=sample_signup_event["event_id"])
            .first()
        )
        assert processed is not None
        assert processed.status == "success"

    def test_process_message_skips_already_processed(self, db_session, sample_signup_event):
        from shared.db import ProcessedEvent

        processed = ProcessedEvent(
            event_id=sample_signup_event["event_id"],
            processed_at=utcnow(),
            status=ProcessingStatus.SUCCESS.value,
        )
        db_session.add(processed)
        db_session.commit()

        msg = MagicMock()
        msg.value.return_value = json.dumps(sample_signup_event).encode("utf-8")

        result = process_message(msg, db_session)

        assert result is True

        from shared.db import RiskScore

        score = (
            db_session.query(RiskScore).filter_by(user_id=sample_signup_event["user_id"]).first()
        )
        assert score is None

    def test_multiple_scores_for_same_user(self, db_session, sample_signup_event):
        import uuid

        msg1 = MagicMock()
        event1 = sample_signup_event.copy()
        event1["event_id"] = str(uuid.uuid4())
        msg1.value.return_value = json.dumps(event1).encode("utf-8")

        msg2 = MagicMock()
        event2 = sample_signup_event.copy()
        event2["event_id"] = str(uuid.uuid4())
        msg2.value.return_value = json.dumps(event2).encode("utf-8")

        process_message(msg1, db_session)
        process_message(msg2, db_session)

        from shared.db import RiskScore

        scores = db_session.query(RiskScore).filter_by(user_id=sample_signup_event["user_id"]).all()
        assert len(scores) == 2
