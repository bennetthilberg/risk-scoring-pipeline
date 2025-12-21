import json
import uuid
from unittest.mock import MagicMock, PropertyMock

import pytest

from services.scorer.processor import is_already_processed, mark_processed, process_message
from services.scorer.retry import (
    NonRetryableError,
    RetryableError,
    calculate_backoff_ms,
    send_to_dlq,
    should_retry,
)
from services.scorer.scoring import compute_dummy_score
from shared import ProcessingStatus, RiskBand


@pytest.mark.unit
class TestComputeDummyScore:
    def test_returns_tuple(self):
        score, band, top_features = compute_dummy_score("user-001", "signup")
        assert isinstance(score, float)
        assert isinstance(band, RiskBand)
        assert isinstance(top_features, dict)

    def test_score_in_valid_range(self):
        for event_type in ["signup", "login", "transaction"]:
            score, _, _ = compute_dummy_score("user-001", event_type)
            assert 0.0 <= score <= 1.0

    def test_band_matches_score(self):
        from shared import score_to_band

        for user_id in ["user-001", "user-002", "user-003"]:
            score, band, _ = compute_dummy_score(user_id, "transaction")
            expected_band = score_to_band(score)
            assert band == expected_band

    def test_deterministic_for_same_user(self):
        score1, band1, features1 = compute_dummy_score("user-fixed", "signup")
        score2, band2, features2 = compute_dummy_score("user-fixed", "signup")
        assert score1 == score2
        assert band1 == band2
        assert features1 == features2

    def test_different_users_different_scores(self):
        score1, _, _ = compute_dummy_score("user-a", "signup")
        score2, _, _ = compute_dummy_score("user-b", "signup")
        assert score1 != score2

    def test_top_features_has_max_three_items(self):
        _, _, top_features = compute_dummy_score("user-001", "transaction")
        assert len(top_features) <= 3

    def test_top_features_values_are_floats(self):
        _, _, top_features = compute_dummy_score("user-001", "transaction")
        for value in top_features.values():
            assert isinstance(value, float)


@pytest.mark.unit
class TestIsAlreadyProcessed:
    def test_returns_true_when_processed(self):
        db = MagicMock()
        db.execute.return_value.first.return_value = (uuid.uuid4(),)

        event_id = uuid.uuid4()
        result = is_already_processed(event_id, db)

        assert result is True
        db.execute.assert_called_once()

    def test_returns_false_when_not_processed(self):
        db = MagicMock()
        db.execute.return_value.first.return_value = None

        event_id = uuid.uuid4()
        result = is_already_processed(event_id, db)

        assert result is False


@pytest.mark.unit
class TestMarkProcessed:
    def test_returns_true_when_inserted(self):
        db = MagicMock()
        execute_result = MagicMock()
        type(execute_result).rowcount = PropertyMock(return_value=1)
        db.execute.return_value = execute_result

        event_id = uuid.uuid4()
        result = mark_processed(event_id, ProcessingStatus.SUCCESS, db)

        assert result is True
        db.execute.assert_called_once()

    def test_returns_false_when_already_exists(self):
        db = MagicMock()
        execute_result = MagicMock()
        type(execute_result).rowcount = PropertyMock(return_value=0)
        db.execute.return_value = execute_result

        event_id = uuid.uuid4()
        result = mark_processed(event_id, ProcessingStatus.SUCCESS, db)

        assert result is False


@pytest.mark.unit
class TestProcessMessage:
    def test_process_valid_message(self, sample_signup_event):
        msg = MagicMock()
        msg.value.return_value = json.dumps(sample_signup_event).encode("utf-8")

        db = MagicMock()
        not_processed_result = MagicMock()
        not_processed_result.first.return_value = None

        mark_success_result = MagicMock()
        type(mark_success_result).rowcount = PropertyMock(return_value=1)

        db.execute.side_effect = [not_processed_result, mark_success_result]

        success, should_retry_flag = process_message(msg, db)

        assert success is True
        assert should_retry_flag is False
        db.add.assert_called_once()
        db.commit.assert_called_once()

    def test_process_skips_already_processed(self, sample_signup_event):
        msg = MagicMock()
        msg.value.return_value = json.dumps(sample_signup_event).encode("utf-8")

        db = MagicMock()
        db.execute.return_value.first.return_value = (uuid.uuid4(),)

        success, should_retry_flag = process_message(msg, db)

        assert success is True
        assert should_retry_flag is False
        db.add.assert_not_called()

    def test_process_invalid_message_sends_to_dlq(self):
        msg = MagicMock()
        msg.value.return_value = b"not valid json"

        db = MagicMock()

        success, should_retry_flag = process_message(msg, db)

        assert success is True
        assert should_retry_flag is False
        db.add.assert_called_once()
        db.commit.assert_called_once()

    def test_process_creates_risk_score(self, sample_signup_event):
        msg = MagicMock()
        msg.value.return_value = json.dumps(sample_signup_event).encode("utf-8")

        db = MagicMock()
        not_processed_result = MagicMock()
        not_processed_result.first.return_value = None

        mark_success_result = MagicMock()
        type(mark_success_result).rowcount = PropertyMock(return_value=1)

        db.execute.side_effect = [not_processed_result, mark_success_result]

        process_message(msg, db)

        add_calls = list(db.add.call_args_list)
        risk_score_added = None
        for call in add_calls:
            from shared.db import RiskScore

            if isinstance(call[0][0], RiskScore):
                risk_score_added = call[0][0]
                break

        assert risk_score_added is not None
        assert risk_score_added.user_id == sample_signup_event["user_id"]
        assert 0.0 <= risk_score_added.score <= 1.0
        assert risk_score_added.band in ["low", "med", "high"]
        assert risk_score_added.model_version == "dummy-v1"

    def test_process_handles_concurrent_processing(self, sample_signup_event):
        msg = MagicMock()
        msg.value.return_value = json.dumps(sample_signup_event).encode("utf-8")

        db = MagicMock()
        not_processed_result = MagicMock()
        not_processed_result.first.return_value = None

        mark_failed_result = MagicMock()
        type(mark_failed_result).rowcount = PropertyMock(return_value=0)

        db.execute.side_effect = [not_processed_result, mark_failed_result]

        success, should_retry_flag = process_message(msg, db)

        assert success is True
        assert should_retry_flag is False
        db.rollback.assert_called()


@pytest.mark.unit
class TestRetryLogic:
    def test_calculate_backoff_ms_exponential(self):
        assert calculate_backoff_ms(0, 100) == 100
        assert calculate_backoff_ms(1, 100) == 200
        assert calculate_backoff_ms(2, 100) == 400
        assert calculate_backoff_ms(3, 100) == 800

    def test_should_retry_under_max(self):
        assert should_retry(0, 3) is True
        assert should_retry(1, 3) is True
        assert should_retry(2, 3) is True

    def test_should_retry_at_max(self):
        assert should_retry(3, 3) is False
        assert should_retry(4, 3) is False


@pytest.mark.unit
class TestDLQHandling:
    def test_send_to_dlq_creates_entry(self):
        db = MagicMock()
        raw_payload = b'{"test": "data"}'
        event_id = uuid.uuid4()

        send_to_dlq(
            raw_payload=raw_payload,
            failure_reason="Test failure",
            db=db,
            event_id=event_id,
            retry_count=2,
        )

        db.add.assert_called_once()
        db.commit.assert_called_once()

        added_entry = db.add.call_args[0][0]
        from shared.db import DLQEvent

        assert isinstance(added_entry, DLQEvent)
        assert added_entry.event_id == event_id
        assert added_entry.raw_payload == '{"test": "data"}'
        assert added_entry.failure_reason == "Test failure"
        assert added_entry.retry_count == 2

    def test_send_to_dlq_handles_invalid_utf8(self):
        db = MagicMock()
        raw_payload = b"\xff\xfe invalid bytes"

        send_to_dlq(
            raw_payload=raw_payload,
            failure_reason="Invalid data",
            db=db,
            event_id=None,
            retry_count=0,
        )

        db.add.assert_called_once()
        added_entry = db.add.call_args[0][0]
        assert added_entry.event_id is None

    def test_invalid_schema_sends_to_dlq(self, sample_signup_event):
        msg = MagicMock()
        invalid_event = sample_signup_event.copy()
        invalid_event["event_type"] = "unknown_type"
        msg.value.return_value = json.dumps(invalid_event).encode("utf-8")

        db = MagicMock()

        success, should_retry_flag = process_message(msg, db)

        assert success is True
        assert should_retry_flag is False
        db.add.assert_called_once()
        db.commit.assert_called_once()


@pytest.mark.unit
class TestErrorClassification:
    def test_non_retryable_error_is_exception(self):
        with pytest.raises(NonRetryableError):
            raise NonRetryableError("Test error")

    def test_retryable_error_is_exception(self):
        with pytest.raises(RetryableError):
            raise RetryableError("Test error")
