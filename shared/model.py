"""Model loading and inference for risk scoring."""

import json
import logging
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from shared.enums import RiskBand

logger = logging.getLogger(__name__)


@dataclass
class ModelMetadata:
    model_version: str
    created_at: str
    feature_order: list[str]
    feature_defaults: dict[str, float]
    band_thresholds: dict[str, float]
    params_hash: str
    coefficients: dict[str, float]
    intercept: float
    scaler_mean: dict[str, float]
    scaler_scale: dict[str, float]
    metrics: dict[str, Any]

    @classmethod
    def from_json(cls, path: Path) -> "ModelMetadata":
        with open(path) as f:
            data = json.load(f)
        return cls(
            model_version=data["model_version"],
            created_at=data["created_at"],
            feature_order=data["feature_order"],
            feature_defaults=data["feature_defaults"],
            band_thresholds=data["band_thresholds"],
            params_hash=data["params_hash"],
            coefficients=data["coefficients"],
            intercept=data["intercept"],
            scaler_mean=data["scaler_mean"],
            scaler_scale=data["scaler_scale"],
            metrics=data.get("metrics", {}),
        )


class RiskModel:
    """Risk scoring model wrapper with inference and explanation."""

    def __init__(self, model_dir: Path | str):
        self.model_dir = Path(model_dir)
        self._model: Any = None
        self._scaler: Any = None
        self._metadata: ModelMetadata | None = None
        self._loaded = False

    def load(self) -> None:
        """Load model and metadata from disk."""
        model_path = self.model_dir / "model.pkl"
        metadata_path = self.model_dir / "metadata.json"

        if not model_path.exists():
            raise FileNotFoundError(f"Model file not found: {model_path}")
        if not metadata_path.exists():
            raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

        with open(model_path, "rb") as f:
            artifacts = pickle.load(f)
            self._model = artifacts["model"]
            self._scaler = artifacts["scaler"]

        self._metadata = ModelMetadata.from_json(metadata_path)
        self._loaded = True

        logger.info(
            f"Loaded model {self._metadata.model_version} (hash: {self._metadata.params_hash})"
        )

    def _ensure_loaded(self) -> None:
        if not self._loaded or self._metadata is None:
            raise RuntimeError("Model not loaded. Call load() first.")

    @property
    def metadata(self) -> ModelMetadata:
        self._ensure_loaded()
        assert self._metadata is not None
        return self._metadata

    @property
    def version(self) -> str:
        return self.metadata.model_version

    def score(self, features: dict[str, float]) -> float:
        """Score a feature vector and return probability of high risk."""
        self._ensure_loaded()
        assert self._scaler is not None
        assert self._model is not None

        feature_vector = self._prepare_features(features)
        scaled = self._scaler.transform(feature_vector.reshape(1, -1))
        proba = self._model.predict_proba(scaled)[0, 1]
        return float(proba)

    def score_to_band(self, score: float) -> RiskBand:
        """Convert score to risk band using metadata thresholds."""
        self._ensure_loaded()
        assert self._metadata is not None

        thresholds = self._metadata.band_thresholds
        if score < thresholds["low"]:
            return RiskBand.LOW
        elif score < thresholds["med"]:
            return RiskBand.MEDIUM
        else:
            return RiskBand.HIGH

    def explain(self, features: dict[str, float], top_k: int = 3) -> dict[str, float]:
        """Compute feature contributions to the score."""
        self._ensure_loaded()
        assert self._scaler is not None
        assert self._model is not None
        assert self._metadata is not None

        feature_vector = self._prepare_features(features)
        scaled = self._scaler.transform(feature_vector.reshape(1, -1))[0]

        contributions = {}
        coefs = self._model.coef_[0]
        for i, feature_name in enumerate(self._metadata.feature_order):
            contributions[feature_name] = float(coefs[i] * scaled[i])

        sorted_features = sorted(contributions.items(), key=lambda x: abs(x[1]), reverse=True)
        return {k: round(v, 4) for k, v in sorted_features[:top_k]}

    def predict(
        self, features: dict[str, float], top_k: int = 3
    ) -> tuple[float, RiskBand, dict[str, float]]:
        """Full prediction with score, band, and explanation."""
        score = self.score(features)
        band = self.score_to_band(score)
        top_features = self.explain(features, top_k)
        return score, band, top_features

    def _prepare_features(self, features: dict[str, float]) -> np.ndarray:
        """Convert feature dict to ordered numpy array."""
        assert self._metadata is not None
        vector = []
        for feature_name in self._metadata.feature_order:
            default = self._metadata.feature_defaults.get(feature_name, 0.0)
            value = features.get(feature_name, default)
            vector.append(float(value))
        return np.array(vector)


_model_instance: RiskModel | None = None


def get_model(model_dir: Path | str | None = None) -> RiskModel:
    """Get or create the global model instance."""
    global _model_instance
    if _model_instance is None:
        if model_dir is None:
            from shared.config import get_settings

            model_dir = Path(get_settings().model_path).parent
        _model_instance = RiskModel(model_dir)
        _model_instance.load()
    return _model_instance


def reset_model() -> None:
    """Reset the global model instance (for testing)."""
    global _model_instance
    _model_instance = None
