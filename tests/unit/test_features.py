import pytest

from shared.features import FEATURE_DEFAULTS, FEATURE_ORDER


@pytest.mark.unit
class TestFeatureOrder:
    def test_feature_order_is_list(self):
        assert isinstance(FEATURE_ORDER, list)

    def test_feature_order_has_expected_features(self):
        expected = [
            "txn_count_24h",
            "txn_amount_sum_24h",
            "failed_logins_1h",
            "account_age_days",
            "unique_countries_7d",
            "avg_txn_amount_30d",
        ]
        assert expected == FEATURE_ORDER

    def test_feature_order_length(self):
        assert len(FEATURE_ORDER) == 6

    def test_feature_order_no_duplicates(self):
        assert len(FEATURE_ORDER) == len(set(FEATURE_ORDER))


@pytest.mark.unit
class TestFeatureDefaults:
    def test_defaults_match_order(self):
        for feature in FEATURE_ORDER:
            assert feature in FEATURE_DEFAULTS

    def test_all_defaults_have_values(self):
        for feature, default in FEATURE_DEFAULTS.items():
            assert default is not None
            assert feature in FEATURE_ORDER

    def test_count_features_default_to_zero(self):
        assert FEATURE_DEFAULTS["txn_count_24h"] == 0
        assert FEATURE_DEFAULTS["failed_logins_1h"] == 0
        assert FEATURE_DEFAULTS["unique_countries_7d"] == 0
        assert FEATURE_DEFAULTS["account_age_days"] == 0

    def test_amount_features_default_to_zero_float(self):
        assert FEATURE_DEFAULTS["txn_amount_sum_24h"] == 0.0
        assert FEATURE_DEFAULTS["avg_txn_amount_30d"] == 0.0


@pytest.mark.contract
class TestFeatureOrderContract:
    def test_feature_order_stability(self):
        expected_order = [
            "txn_count_24h",
            "txn_amount_sum_24h",
            "failed_logins_1h",
            "account_age_days",
            "unique_countries_7d",
            "avg_txn_amount_30d",
        ]
        assert expected_order == FEATURE_ORDER, (
            "Feature order has changed. This will break model inference. "
            "If intentional, retrain the model with the new feature order."
        )
