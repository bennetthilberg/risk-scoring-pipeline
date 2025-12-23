from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from services.scorer.features import (
    _account_age_days,
    _avg_txn_amount_window,
    _failed_logins_window,
    _txn_amount_sum_window,
    _txn_count_window,
    _unique_countries_window,
    compute_features,
    validate_feature_order,
)
from shared import utcnow
from shared.features import FEATURE_ORDER


@pytest.mark.unit
class TestComputeFeatures:
    def test_returns_all_features(self):
        db = MagicMock()
        db.execute.return_value.scalar.return_value = 0
        db.query.return_value.filter.return_value.all.return_value = []
        db.query.return_value.filter.return_value.order_by.return_value.first.return_value = None

        features = compute_features("user-001", db)

        for feature in FEATURE_ORDER:
            assert feature in features

    def test_uses_defaults_for_missing_data(self):
        db = MagicMock()
        db.execute.return_value.scalar.return_value = 0
        db.query.return_value.filter.return_value.all.return_value = []
        db.query.return_value.filter.return_value.order_by.return_value.first.return_value = None

        features = compute_features("new-user", db)

        assert features["txn_count_24h"] == 0
        assert features["failed_logins_1h"] == 0
        assert features["account_age_days"] == 0


@pytest.mark.unit
class TestTxnCountWindow:
    def test_returns_count_from_query(self):
        db = MagicMock()
        db.execute.return_value.scalar.return_value = 5

        now = utcnow()
        result = _txn_count_window("user-001", db, now, hours=24)

        assert result == 5
        db.execute.assert_called_once()

    def test_returns_zero_for_none(self):
        db = MagicMock()
        db.execute.return_value.scalar.return_value = None

        now = utcnow()
        result = _txn_count_window("user-001", db, now, hours=24)

        assert result == 0


@pytest.mark.unit
class TestTxnAmountSumWindow:
    def test_sums_transaction_amounts(self):
        db = MagicMock()
        event1 = MagicMock()
        event1.payload_json = {"amount": 100.0}
        event2 = MagicMock()
        event2.payload_json = {"amount": 50.0}
        db.query.return_value.filter.return_value.all.return_value = [event1, event2]

        now = utcnow()
        result = _txn_amount_sum_window("user-001", db, now, hours=24)

        assert result == 150.0

    def test_skips_events_without_amount(self):
        db = MagicMock()
        event1 = MagicMock()
        event1.payload_json = {"amount": 100.0}
        event2 = MagicMock()
        event2.payload_json = {}
        db.query.return_value.filter.return_value.all.return_value = [event1, event2]

        now = utcnow()
        result = _txn_amount_sum_window("user-001", db, now, hours=24)

        assert result == 100.0

    def test_returns_zero_for_no_events(self):
        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = []

        now = utcnow()
        result = _txn_amount_sum_window("user-001", db, now, hours=24)

        assert result == 0.0


@pytest.mark.unit
class TestFailedLoginsWindow:
    def test_counts_failed_logins(self):
        db = MagicMock()
        event1 = MagicMock()
        event1.payload_json = {"success": False}
        event2 = MagicMock()
        event2.payload_json = {"success": True}
        event3 = MagicMock()
        event3.payload_json = {"success": False}
        db.query.return_value.filter.return_value.all.return_value = [event1, event2, event3]

        now = utcnow()
        result = _failed_logins_window("user-001", db, now, hours=1)

        assert result == 2

    def test_returns_zero_for_no_failures(self):
        db = MagicMock()
        event1 = MagicMock()
        event1.payload_json = {"success": True}
        db.query.return_value.filter.return_value.all.return_value = [event1]

        now = utcnow()
        result = _failed_logins_window("user-001", db, now, hours=1)

        assert result == 0


@pytest.mark.unit
class TestAccountAgeDays:
    def test_calculates_age_from_first_event(self):
        db = MagicMock()
        now = utcnow()

        first_event = MagicMock()
        first_event.ts = now - timedelta(days=30)
        db.query.return_value.filter.return_value.order_by.return_value.first.return_value = (
            first_event
        )

        result = _account_age_days("user-001", db, now)

        assert result == 30

    def test_returns_zero_for_new_account(self):
        db = MagicMock()
        now = utcnow()

        first_event = MagicMock()
        first_event.ts = now
        db.query.return_value.filter.return_value.order_by.return_value.first.return_value = (
            first_event
        )

        result = _account_age_days("user-001", db, now)

        assert result == 0

    def test_returns_zero_for_no_events(self):
        db = MagicMock()
        db.query.return_value.filter.return_value.order_by.return_value.first.return_value = None

        now = utcnow()
        result = _account_age_days("user-001", db, now)

        assert result == 0


@pytest.mark.unit
class TestUniqueCountriesWindow:
    def test_counts_unique_countries(self):
        db = MagicMock()
        event1 = MagicMock()
        event1.payload_json = {"country": "US"}
        event2 = MagicMock()
        event2.payload_json = {"country": "UK"}
        event3 = MagicMock()
        event3.payload_json = {"country": "US"}
        db.query.return_value.filter.return_value.all.return_value = [event1, event2, event3]

        now = utcnow()
        result = _unique_countries_window("user-001", db, now, days=7)

        assert result == 2

    def test_skips_events_without_country(self):
        db = MagicMock()
        event1 = MagicMock()
        event1.payload_json = {"country": "US"}
        event2 = MagicMock()
        event2.payload_json = {}
        db.query.return_value.filter.return_value.all.return_value = [event1, event2]

        now = utcnow()
        result = _unique_countries_window("user-001", db, now, days=7)

        assert result == 1

    def test_returns_zero_for_no_events(self):
        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = []

        now = utcnow()
        result = _unique_countries_window("user-001", db, now, days=7)

        assert result == 0


@pytest.mark.unit
class TestAvgTxnAmountWindow:
    def test_calculates_average(self):
        db = MagicMock()
        event1 = MagicMock()
        event1.payload_json = {"amount": 100.0}
        event2 = MagicMock()
        event2.payload_json = {"amount": 200.0}
        db.query.return_value.filter.return_value.all.return_value = [event1, event2]

        now = utcnow()
        result = _avg_txn_amount_window("user-001", db, now, days=30)

        assert result == 150.0

    def test_returns_zero_for_no_events(self):
        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = []

        now = utcnow()
        result = _avg_txn_amount_window("user-001", db, now, days=30)

        assert result == 0.0

    def test_skips_events_without_amount(self):
        db = MagicMock()
        event1 = MagicMock()
        event1.payload_json = {"amount": 100.0}
        event2 = MagicMock()
        event2.payload_json = {}
        db.query.return_value.filter.return_value.all.return_value = [event1, event2]

        now = utcnow()
        result = _avg_txn_amount_window("user-001", db, now, days=30)

        assert result == 100.0


@pytest.mark.unit
class TestValidateFeatureOrder:
    def test_validates_successfully(self):
        assert validate_feature_order() is True
