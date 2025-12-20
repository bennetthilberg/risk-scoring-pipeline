import json
from unittest.mock import MagicMock

import pytest

from services.scorer.processor import process_message
from services.scorer.scoring import compute_dummy_score
from shared import RiskBand


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
class TestProcessMessage:
    def test_process_valid_message(self, sample_signup_event):
        msg = MagicMock()
        msg.value.return_value = json.dumps(sample_signup_event).encode("utf-8")

        db = MagicMock()

        result = process_message(msg, db)

        assert result is True
        db.add.assert_called_once()
        db.commit.assert_called_once()

    def test_process_login_event(self, sample_login_event):
        msg = MagicMock()
        msg.value.return_value = json.dumps(sample_login_event).encode("utf-8")

        db = MagicMock()

        result = process_message(msg, db)

        assert result is True
        db.add.assert_called_once()

    def test_process_transaction_event(self, sample_transaction_event):
        msg = MagicMock()
        msg.value.return_value = json.dumps(sample_transaction_event).encode("utf-8")

        db = MagicMock()

        result = process_message(msg, db)

        assert result is True

    def test_process_invalid_message(self):
        msg = MagicMock()
        msg.value.return_value = b"not valid json"

        db = MagicMock()

        result = process_message(msg, db)

        assert result is False
        db.add.assert_not_called()

    def test_process_creates_risk_score(self, sample_signup_event):
        msg = MagicMock()
        msg.value.return_value = json.dumps(sample_signup_event).encode("utf-8")

        db = MagicMock()

        process_message(msg, db)

        added_object = db.add.call_args[0][0]
        from shared.db import RiskScore

        assert isinstance(added_object, RiskScore)
        assert added_object.user_id == sample_signup_event["user_id"]
        assert 0.0 <= added_object.score <= 1.0
        assert added_object.band in ["low", "med", "high"]
        assert added_object.model_version == "dummy-v1"
