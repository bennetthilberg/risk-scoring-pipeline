import json
import pickle
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from services.scorer.scoring import (
    _get_model,
    compute_dummy_score,
    compute_score,
    reset_model,
)
from shared.enums import RiskBand
from shared.features import FEATURE_DEFAULTS, FEATURE_ORDER


@pytest.fixture
def sample_model_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        model_dir = Path(tmpdir)

        rng = np.random.default_rng(42)
        X = rng.random((100, len(FEATURE_ORDER)))
        y = rng.integers(0, 2, 100)

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        model = LogisticRegression(random_state=42)
        model.fit(X_scaled, y)

        with open(model_dir / "model.pkl", "wb") as f:
            pickle.dump({"model": model, "scaler": scaler}, f)

        metadata = {
            "model_version": "test-v1",
            "created_at": "2024-01-01T00:00:00Z",
            "feature_order": FEATURE_ORDER,
            "feature_defaults": FEATURE_DEFAULTS,
            "band_thresholds": {"low": 0.33, "med": 0.66},
            "params_hash": "abc123",
            "coefficients": dict(zip(FEATURE_ORDER, model.coef_[0].tolist(), strict=False)),
            "intercept": float(model.intercept_[0]),
            "scaler_mean": dict(zip(FEATURE_ORDER, scaler.mean_.tolist(), strict=False)),
            "scaler_scale": dict(zip(FEATURE_ORDER, scaler.scale_.tolist(), strict=False)),
            "metrics": {"test_accuracy": 0.85},
        }

        with open(model_dir / "metadata.json", "w") as f:
            json.dump(metadata, f)

        yield model_dir


@pytest.fixture(autouse=True)
def reset_model_state():
    reset_model()
    yield
    reset_model()


@pytest.mark.unit
class TestComputeDummyScore:
    def test_returns_tuple_of_three(self):
        result = compute_dummy_score("user-001", "signup")
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_score_is_float_in_range(self):
        score, _, _ = compute_dummy_score("user-001", "signup")
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0

    def test_band_is_risk_band(self):
        _, band, _ = compute_dummy_score("user-001", "signup")
        assert isinstance(band, RiskBand)

    def test_top_features_is_dict(self):
        _, _, top_features = compute_dummy_score("user-001", "signup")
        assert isinstance(top_features, dict)
        assert len(top_features) <= 3

    def test_deterministic_for_same_user(self):
        result1 = compute_dummy_score("user-fixed", "signup")
        result2 = compute_dummy_score("user-fixed", "signup")
        assert result1 == result2

    def test_different_users_different_scores(self):
        score1, _, _ = compute_dummy_score("user-a", "signup")
        score2, _, _ = compute_dummy_score("user-b", "signup")
        assert score1 != score2

    def test_transaction_type_affects_score(self):
        score_signup, _, _ = compute_dummy_score("user-001", "signup")
        score_txn, _, _ = compute_dummy_score("user-001", "transaction")
        assert score_txn >= score_signup


@pytest.mark.unit
class TestComputeScoreWithDummy:
    def test_use_dummy_returns_dummy_version(self):
        db = MagicMock()
        score, band, top_features, version = compute_score("user-001", db, use_dummy=True)

        assert version == "dummy-v1"
        assert isinstance(score, float)
        assert isinstance(band, RiskBand)
        assert isinstance(top_features, dict)


@pytest.mark.unit
class TestComputeScoreWithModel:
    @patch("services.scorer.features.compute_features")
    @patch("services.scorer.scoring._get_model")
    def test_uses_model_when_available(self, mock_get_model, mock_compute_features):
        mock_model = MagicMock()
        mock_model.predict.return_value = (0.75, RiskBand.HIGH, {"txn_count_24h": 0.5})
        mock_model.version = "model-v1"
        mock_get_model.return_value = mock_model
        mock_compute_features.return_value = dict.fromkeys(FEATURE_ORDER, 0.5)

        db = MagicMock()
        score, band, top_features, version = compute_score("user-001", db)

        assert score == 0.75
        assert band == RiskBand.HIGH
        assert version == "model-v1"
        mock_compute_features.assert_called_once()

    @patch("services.scorer.scoring._get_model")
    def test_falls_back_to_dummy_when_model_unavailable(self, mock_get_model):
        mock_get_model.return_value = None

        db = MagicMock()
        score, band, top_features, version = compute_score("user-001", db)

        assert version == "dummy-v1"
        assert isinstance(score, float)


@pytest.mark.unit
class TestGetModel:
    def test_returns_none_when_model_dir_missing(self):
        with patch("shared.config.get_settings") as mock_settings:
            mock_settings.return_value = type(
                "Settings", (), {"model_path": "/nonexistent/model.pkl"}
            )()

            reset_model()
            model = _get_model()

            assert model is None

    def test_returns_none_when_model_files_missing(self):
        with (
            tempfile.TemporaryDirectory() as tmpdir,
            patch("shared.config.get_settings") as mock_settings,
        ):
            mock_settings.return_value = type(
                "Settings", (), {"model_path": f"{tmpdir}/model.pkl"}
            )()

            reset_model()
            model = _get_model()

            assert model is None

    def test_caches_model_load_attempt(self):
        with patch("shared.config.get_settings") as mock_settings:
            mock_settings.return_value = type(
                "Settings", (), {"model_path": "/nonexistent/model.pkl"}
            )()

            reset_model()
            model1 = _get_model()
            model2 = _get_model()

            assert model1 is None
            assert model2 is None
            assert mock_settings.call_count == 1

    def test_loads_model_successfully(self, sample_model_dir):
        with patch("shared.config.get_settings") as mock_settings:
            mock_settings.return_value = type(
                "Settings", (), {"model_path": str(sample_model_dir / "model.pkl")}
            )()

            reset_model()
            model = _get_model()

            assert model is not None
            assert model.version == "test-v1"


@pytest.mark.unit
class TestResetModel:
    def test_reset_clears_model(self, sample_model_dir):
        import services.scorer.scoring as scoring_module

        with patch("shared.config.get_settings") as mock_settings:
            mock_settings.return_value = type(
                "Settings", (), {"model_path": str(sample_model_dir / "model.pkl")}
            )()

            reset_model()
            model = _get_model()
            assert model is not None

            reset_model()
            assert scoring_module._model is None
            assert scoring_module._model_load_attempted is False
