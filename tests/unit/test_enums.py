import pytest

from shared.enums import (
    CURRENT_SCHEMA_VERSION,
    RISK_BAND_THRESHOLDS,
    EventType,
    ProcessingStatus,
    RiskBand,
    score_to_band,
)


@pytest.mark.unit
class TestEventType:
    def test_signup_value(self):
        assert EventType.SIGNUP.value == "signup"

    def test_login_value(self):
        assert EventType.LOGIN.value == "login"

    def test_transaction_value(self):
        assert EventType.TRANSACTION.value == "transaction"

    def test_string_comparison(self):
        assert EventType.SIGNUP == "signup"
        assert EventType.LOGIN == "login"
        assert EventType.TRANSACTION == "transaction"


@pytest.mark.unit
class TestRiskBand:
    def test_low_value(self):
        assert RiskBand.LOW.value == "low"

    def test_medium_value(self):
        assert RiskBand.MEDIUM.value == "med"

    def test_high_value(self):
        assert RiskBand.HIGH.value == "high"


@pytest.mark.unit
class TestProcessingStatus:
    def test_success_value(self):
        assert ProcessingStatus.SUCCESS.value == "success"

    def test_failed_value(self):
        assert ProcessingStatus.FAILED.value == "failed"

    def test_skipped_value(self):
        assert ProcessingStatus.SKIPPED.value == "skipped"


@pytest.mark.unit
class TestScoreToBand:
    def test_zero_score_is_low(self):
        assert score_to_band(0.0) == RiskBand.LOW

    def test_score_below_low_threshold(self):
        assert score_to_band(0.1) == RiskBand.LOW
        assert score_to_band(0.32) == RiskBand.LOW

    def test_score_at_low_threshold_is_medium(self):
        assert score_to_band(0.33) == RiskBand.MEDIUM

    def test_score_in_medium_range(self):
        assert score_to_band(0.34) == RiskBand.MEDIUM
        assert score_to_band(0.5) == RiskBand.MEDIUM
        assert score_to_band(0.65) == RiskBand.MEDIUM

    def test_score_at_medium_threshold_is_high(self):
        assert score_to_band(0.66) == RiskBand.HIGH

    def test_score_above_medium_threshold(self):
        assert score_to_band(0.67) == RiskBand.HIGH
        assert score_to_band(0.9) == RiskBand.HIGH

    def test_max_score_is_high(self):
        assert score_to_band(1.0) == RiskBand.HIGH

    def test_boundary_precision(self):
        assert score_to_band(0.329999) == RiskBand.LOW
        assert score_to_band(0.330001) == RiskBand.MEDIUM
        assert score_to_band(0.659999) == RiskBand.MEDIUM
        assert score_to_band(0.660001) == RiskBand.HIGH


@pytest.mark.unit
class TestConstants:
    def test_current_schema_version(self):
        assert CURRENT_SCHEMA_VERSION == 1
        assert isinstance(CURRENT_SCHEMA_VERSION, int)

    def test_risk_band_thresholds(self):
        assert "low_max" in RISK_BAND_THRESHOLDS
        assert "med_max" in RISK_BAND_THRESHOLDS
        assert RISK_BAND_THRESHOLDS["low_max"] == 0.33
        assert RISK_BAND_THRESHOLDS["med_max"] == 0.66
