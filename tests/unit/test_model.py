import json
import pickle
import tempfile
from pathlib import Path

import numpy as np
import pytest
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from shared.enums import RiskBand
from shared.features import FEATURE_DEFAULTS, FEATURE_ORDER
from shared.model import ModelMetadata, RiskModel, get_model, reset_model


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


@pytest.mark.unit
class TestModelMetadata:
    def test_from_json(self, sample_model_dir):
        metadata = ModelMetadata.from_json(sample_model_dir / "metadata.json")

        assert metadata.model_version == "test-v1"
        assert metadata.feature_order == FEATURE_ORDER
        assert metadata.band_thresholds == {"low": 0.33, "med": 0.66}
        assert "test_accuracy" in metadata.metrics

    def test_feature_defaults_loaded(self, sample_model_dir):
        metadata = ModelMetadata.from_json(sample_model_dir / "metadata.json")

        for feature in FEATURE_ORDER:
            assert feature in metadata.feature_defaults

    def test_coefficients_loaded(self, sample_model_dir):
        metadata = ModelMetadata.from_json(sample_model_dir / "metadata.json")

        assert len(metadata.coefficients) == len(FEATURE_ORDER)
        for feature in FEATURE_ORDER:
            assert feature in metadata.coefficients
            assert isinstance(metadata.coefficients[feature], float)


@pytest.mark.unit
class TestRiskModel:
    def test_load_success(self, sample_model_dir):
        model = RiskModel(sample_model_dir)
        model.load()

        assert model._loaded is True
        assert model._model is not None
        assert model._scaler is not None

    def test_load_missing_model_file(self, sample_model_dir):
        (sample_model_dir / "model.pkl").unlink()

        model = RiskModel(sample_model_dir)
        with pytest.raises(FileNotFoundError):
            model.load()

    def test_load_missing_metadata_file(self, sample_model_dir):
        (sample_model_dir / "metadata.json").unlink()

        model = RiskModel(sample_model_dir)
        with pytest.raises(FileNotFoundError):
            model.load()

    def test_version_property(self, sample_model_dir):
        model = RiskModel(sample_model_dir)
        model.load()

        assert model.version == "test-v1"

    def test_version_before_load_raises(self, sample_model_dir):
        model = RiskModel(sample_model_dir)

        with pytest.raises(RuntimeError, match="Model not loaded"):
            _ = model.version


@pytest.mark.unit
class TestRiskModelScoring:
    def test_score_returns_float(self, sample_model_dir):
        model = RiskModel(sample_model_dir)
        model.load()

        features = dict.fromkeys(FEATURE_ORDER, 0.5)
        score = model.score(features)

        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0

    def test_score_before_load_raises(self, sample_model_dir):
        model = RiskModel(sample_model_dir)
        features = dict.fromkeys(FEATURE_ORDER, 0.5)

        with pytest.raises(RuntimeError, match="Model not loaded"):
            model.score(features)

    def test_score_uses_defaults_for_missing_features(self, sample_model_dir):
        model = RiskModel(sample_model_dir)
        model.load()

        partial_features = {"txn_count_24h": 5.0}
        score = model.score(partial_features)

        assert isinstance(score, float)

    def test_score_deterministic(self, sample_model_dir):
        model = RiskModel(sample_model_dir)
        model.load()

        features = dict.fromkeys(FEATURE_ORDER, 0.5)
        score1 = model.score(features)
        score2 = model.score(features)

        assert score1 == score2


@pytest.mark.unit
class TestRiskModelBanding:
    def test_score_to_band_low(self, sample_model_dir):
        model = RiskModel(sample_model_dir)
        model.load()

        band = model.score_to_band(0.1)
        assert band == RiskBand.LOW

    def test_score_to_band_medium(self, sample_model_dir):
        model = RiskModel(sample_model_dir)
        model.load()

        band = model.score_to_band(0.5)
        assert band == RiskBand.MEDIUM

    def test_score_to_band_high(self, sample_model_dir):
        model = RiskModel(sample_model_dir)
        model.load()

        band = model.score_to_band(0.8)
        assert band == RiskBand.HIGH

    def test_score_to_band_at_threshold(self, sample_model_dir):
        model = RiskModel(sample_model_dir)
        model.load()

        assert model.score_to_band(0.33) == RiskBand.MEDIUM
        assert model.score_to_band(0.66) == RiskBand.HIGH


@pytest.mark.unit
class TestRiskModelExplanation:
    def test_explain_returns_dict(self, sample_model_dir):
        model = RiskModel(sample_model_dir)
        model.load()

        features = dict.fromkeys(FEATURE_ORDER, 0.5)
        explanation = model.explain(features)

        assert isinstance(explanation, dict)

    def test_explain_respects_top_k(self, sample_model_dir):
        model = RiskModel(sample_model_dir)
        model.load()

        features = dict.fromkeys(FEATURE_ORDER, 0.5)

        explanation_3 = model.explain(features, top_k=3)
        assert len(explanation_3) <= 3

        explanation_2 = model.explain(features, top_k=2)
        assert len(explanation_2) <= 2

    def test_explain_values_are_floats(self, sample_model_dir):
        model = RiskModel(sample_model_dir)
        model.load()

        features = dict.fromkeys(FEATURE_ORDER, 0.5)
        explanation = model.explain(features)

        for feature, value in explanation.items():
            assert isinstance(value, float)
            assert feature in FEATURE_ORDER

    def test_explain_sorted_by_magnitude(self, sample_model_dir):
        model = RiskModel(sample_model_dir)
        model.load()

        features = dict.fromkeys(FEATURE_ORDER, 0.5)
        explanation = model.explain(features, top_k=6)

        values = list(explanation.values())
        magnitudes = [abs(v) for v in values]
        assert magnitudes == sorted(magnitudes, reverse=True)


@pytest.mark.unit
class TestRiskModelPredict:
    def test_predict_returns_tuple(self, sample_model_dir):
        model = RiskModel(sample_model_dir)
        model.load()

        features = dict.fromkeys(FEATURE_ORDER, 0.5)
        result = model.predict(features)

        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_predict_components(self, sample_model_dir):
        model = RiskModel(sample_model_dir)
        model.load()

        features = dict.fromkeys(FEATURE_ORDER, 0.5)
        score, band, top_features = model.predict(features)

        assert isinstance(score, float)
        assert isinstance(band, RiskBand)
        assert isinstance(top_features, dict)
        assert len(top_features) <= 3

    def test_predict_band_matches_score(self, sample_model_dir):
        model = RiskModel(sample_model_dir)
        model.load()

        features = dict.fromkeys(FEATURE_ORDER, 0.5)
        score, band, _ = model.predict(features)

        expected_band = model.score_to_band(score)
        assert band == expected_band


@pytest.mark.unit
class TestModelSingleton:
    def test_reset_model_clears_instance(self, sample_model_dir, monkeypatch):
        from shared import model as model_module

        reset_model()

        monkeypatch.setattr(
            "shared.config.get_settings",
            lambda: type("Settings", (), {"model_path": str(sample_model_dir / "model.pkl")})(),
        )

        instance = get_model(sample_model_dir)
        assert instance is not None

        reset_model()
        assert model_module._model_instance is None


@pytest.mark.unit
class TestRiskModelAPIContract:
    """Tests to verify correct API usage patterns and prevent common misuse."""

    def test_load_is_instance_method_not_class_method(self, sample_model_dir):
        """Verify that load() must be called on an instance, not the class.

        Calling RiskModel.load(path) instead of RiskModel(path).load() is a common
        mistake. This test ensures such misuse fails with a clear error.
        """
        with pytest.raises(AttributeError, match="model_dir"):
            RiskModel.load(sample_model_dir)

    def test_correct_instantiation_pattern(self, sample_model_dir):
        """Verify the correct pattern: create instance, then call load()."""
        model = RiskModel(sample_model_dir)
        assert model._loaded is False

        model.load()
        assert model._loaded is True
        assert model.version == "test-v1"

    def test_model_requires_path_argument(self):
        """Verify RiskModel requires a model_dir argument."""
        with pytest.raises(TypeError):
            RiskModel()

    def test_model_accepts_string_path(self, sample_model_dir):
        """Verify RiskModel accepts string path."""
        model = RiskModel(str(sample_model_dir))
        model.load()
        assert model._loaded is True

    def test_model_accepts_path_object(self, sample_model_dir):
        """Verify RiskModel accepts Path object."""
        model = RiskModel(sample_model_dir)
        model.load()
        assert model._loaded is True

    def test_operations_before_load_raise_runtime_error(self, sample_model_dir):
        """Verify all operations raise RuntimeError if load() not called."""
        model = RiskModel(sample_model_dir)
        features = {"txn_count_24h": 1.0}

        with pytest.raises(RuntimeError, match="Model not loaded"):
            model.score(features)

        with pytest.raises(RuntimeError, match="Model not loaded"):
            model.score_to_band(0.5)

        with pytest.raises(RuntimeError, match="Model not loaded"):
            model.explain(features)

        with pytest.raises(RuntimeError, match="Model not loaded"):
            model.predict(features)

        with pytest.raises(RuntimeError, match="Model not loaded"):
            _ = model.version

        with pytest.raises(RuntimeError, match="Model not loaded"):
            _ = model.metadata

    def test_double_load_is_safe(self, sample_model_dir):
        """Verify calling load() twice doesn't cause issues."""
        model = RiskModel(sample_model_dir)
        model.load()
        model.load()
        assert model._loaded is True

    def test_get_model_helper_returns_loaded_model(self, sample_model_dir, monkeypatch):
        """Verify get_model() helper returns a fully loaded model."""
        reset_model()

        monkeypatch.setattr(
            "shared.config.get_settings",
            lambda: type("Settings", (), {"model_path": str(sample_model_dir / "model.pkl")})(),
        )

        model = get_model(sample_model_dir)
        assert model._loaded is True
        assert model.version == "test-v1"

        features = {"txn_count_24h": 1.0}
        score = model.score(features)
        assert isinstance(score, float)

        reset_model()
