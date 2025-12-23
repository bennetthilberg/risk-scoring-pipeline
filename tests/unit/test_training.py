import json
import pickle
import tempfile
from pathlib import Path

import numpy as np
import pytest
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from scripts.train import (
    compute_params_hash,
    generate_synthetic_data,
    save_model,
    train_model,
)
from shared.features import FEATURE_ORDER


@pytest.mark.unit
class TestGenerateSyntheticData:
    def test_returns_correct_shape(self):
        X, y = generate_synthetic_data(n_samples=100, seed=42)
        assert X.shape == (100, len(FEATURE_ORDER))
        assert y.shape == (100,)

    def test_default_fraud_rate(self):
        X, y = generate_synthetic_data(n_samples=1000, fraud_rate=0.15, seed=42)
        actual_rate = y.mean()
        assert 0.12 <= actual_rate <= 0.18

    def test_custom_fraud_rate(self):
        X, y = generate_synthetic_data(n_samples=1000, fraud_rate=0.30, seed=42)
        actual_rate = y.mean()
        assert 0.27 <= actual_rate <= 0.33

    def test_deterministic_with_seed(self):
        X1, y1 = generate_synthetic_data(n_samples=100, seed=42)
        X2, y2 = generate_synthetic_data(n_samples=100, seed=42)
        np.testing.assert_array_equal(X1, X2)
        np.testing.assert_array_equal(y1, y2)

    def test_different_seeds_different_data(self):
        X1, y1 = generate_synthetic_data(n_samples=100, seed=42)
        X2, y2 = generate_synthetic_data(n_samples=100, seed=123)
        assert not np.allclose(X1, X2)

    def test_features_are_non_negative(self):
        X, y = generate_synthetic_data(n_samples=500, seed=42)
        assert np.all(X >= 0)

    def test_fraud_users_have_higher_txn_count(self):
        X, y = generate_synthetic_data(n_samples=1000, seed=42)
        fraud_txn_count = X[y == 1, 0].mean()
        legit_txn_count = X[y == 0, 0].mean()
        assert fraud_txn_count > legit_txn_count


@pytest.mark.unit
class TestTrainModel:
    def test_returns_model_scaler_metrics(self):
        X, y = generate_synthetic_data(n_samples=500, seed=42)
        model, scaler, metrics = train_model(X, y, seed=42)

        assert isinstance(model, LogisticRegression)
        assert isinstance(scaler, StandardScaler)
        assert isinstance(metrics, dict)

    def test_metrics_contain_expected_keys(self):
        X, y = generate_synthetic_data(n_samples=500, seed=42)
        _, _, metrics = train_model(X, y, seed=42)

        expected_keys = [
            "train_accuracy",
            "test_accuracy",
            "train_samples",
            "test_samples",
            "fraud_rate",
            "mean_score",
            "std_score",
        ]
        for key in expected_keys:
            assert key in metrics

    def test_model_accuracy_reasonable(self):
        X, y = generate_synthetic_data(n_samples=2000, seed=42)
        _, _, metrics = train_model(X, y, seed=42)

        assert metrics["train_accuracy"] > 0.7
        assert metrics["test_accuracy"] > 0.7

    def test_model_coefficients_correct_shape(self):
        X, y = generate_synthetic_data(n_samples=500, seed=42)
        model, _, _ = train_model(X, y, seed=42)

        assert model.coef_.shape == (1, len(FEATURE_ORDER))
        assert model.intercept_.shape == (1,)

    def test_scaler_fitted(self):
        X, y = generate_synthetic_data(n_samples=500, seed=42)
        _, scaler, _ = train_model(X, y, seed=42)

        assert hasattr(scaler, "mean_")
        assert hasattr(scaler, "scale_")
        assert len(scaler.mean_) == len(FEATURE_ORDER)


@pytest.mark.unit
class TestComputeParamsHash:
    def test_returns_string(self):
        X, y = generate_synthetic_data(n_samples=100, seed=42)
        model, scaler, _ = train_model(X, y, seed=42)
        hash_val = compute_params_hash(model, scaler)

        assert isinstance(hash_val, str)
        assert len(hash_val) == 16

    def test_deterministic(self):
        X, y = generate_synthetic_data(n_samples=100, seed=42)
        model, scaler, _ = train_model(X, y, seed=42)

        hash1 = compute_params_hash(model, scaler)
        hash2 = compute_params_hash(model, scaler)
        assert hash1 == hash2

    def test_different_models_different_hash(self):
        X1, y1 = generate_synthetic_data(n_samples=100, seed=42)
        X2, y2 = generate_synthetic_data(n_samples=100, seed=123)

        model1, scaler1, _ = train_model(X1, y1, seed=42)
        model2, scaler2, _ = train_model(X2, y2, seed=123)

        hash1 = compute_params_hash(model1, scaler1)
        hash2 = compute_params_hash(model2, scaler2)
        assert hash1 != hash2


@pytest.mark.unit
class TestSaveModel:
    def test_creates_model_files(self):
        X, y = generate_synthetic_data(n_samples=100, seed=42)
        model, scaler, metrics = train_model(X, y, seed=42)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            model_path, metadata_path = save_model(model, scaler, metrics, output_dir)

            assert model_path.exists()
            assert metadata_path.exists()
            assert model_path.name == "model.pkl"
            assert metadata_path.name == "metadata.json"

    def test_model_pickle_loadable(self):
        X, y = generate_synthetic_data(n_samples=100, seed=42)
        model, scaler, metrics = train_model(X, y, seed=42)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            model_path, _ = save_model(model, scaler, metrics, output_dir)

            with open(model_path, "rb") as f:
                artifacts = pickle.load(f)

            assert "model" in artifacts
            assert "scaler" in artifacts
            assert isinstance(artifacts["model"], LogisticRegression)
            assert isinstance(artifacts["scaler"], StandardScaler)

    def test_metadata_json_valid(self):
        X, y = generate_synthetic_data(n_samples=100, seed=42)
        model, scaler, metrics = train_model(X, y, seed=42)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            _, metadata_path = save_model(model, scaler, metrics, output_dir)

            with open(metadata_path) as f:
                metadata = json.load(f)

            assert "model_version" in metadata
            assert "feature_order" in metadata
            assert "band_thresholds" in metadata
            assert "coefficients" in metadata
            assert "intercept" in metadata
            assert "scaler_mean" in metadata
            assert "scaler_scale" in metadata
            assert metadata["feature_order"] == FEATURE_ORDER

    def test_custom_version(self):
        X, y = generate_synthetic_data(n_samples=100, seed=42)
        model, scaler, metrics = train_model(X, y, seed=42)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            _, metadata_path = save_model(
                model, scaler, metrics, output_dir, model_version="custom-v2"
            )

            with open(metadata_path) as f:
                metadata = json.load(f)

            assert metadata["model_version"] == "custom-v2"

    def test_band_thresholds_correct(self):
        X, y = generate_synthetic_data(n_samples=100, seed=42)
        model, scaler, metrics = train_model(X, y, seed=42)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            _, metadata_path = save_model(model, scaler, metrics, output_dir)

            with open(metadata_path) as f:
                metadata = json.load(f)

            assert metadata["band_thresholds"]["low"] == 0.33
            assert metadata["band_thresholds"]["med"] == 0.66
