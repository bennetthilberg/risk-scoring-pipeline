"""Scoring module for risk assessment.

Provides both dummy scoring (for testing/development) and real model-based
scoring using a trained logistic regression model.
"""

import logging
import random
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

from shared import RiskBand, score_to_band
from shared.features import FEATURE_ORDER

if TYPE_CHECKING:
    from shared.model import RiskModel

logger = logging.getLogger(__name__)

_model = None
_model_load_attempted = False


def compute_dummy_score(user_id: str, event_type: str) -> tuple[float, RiskBand, dict[str, float]]:
    """Compute a deterministic dummy score for testing.

    This provides reproducible scores based on user_id hash,
    useful for testing when a real model isn't available.
    """
    random.seed(hash(user_id) % (2**32))

    base_score = random.uniform(0.1, 0.5)

    if event_type == "transaction":
        base_score += random.uniform(0.0, 0.3)
    elif event_type == "login":
        base_score += random.uniform(0.0, 0.1)

    score = min(max(base_score, 0.0), 1.0)
    band = score_to_band(score)

    top_features = {}
    feature_contributions = []
    for feature in FEATURE_ORDER:
        contribution = random.uniform(-0.1, 0.2)
        feature_contributions.append((feature, contribution))

    feature_contributions.sort(key=lambda x: abs(x[1]), reverse=True)
    for feature, contribution in feature_contributions[:3]:
        top_features[feature] = round(contribution, 4)

    return score, band, top_features


def compute_score(
    user_id: str,
    db: Session,
    use_dummy: bool = False,
) -> tuple[float, RiskBand, dict[str, float], str]:
    """Compute risk score using real model or dummy scoring.

    Args:
        user_id: User to score
        db: Database session for feature computation
        use_dummy: Force dummy scoring even if model is available

    Returns:
        Tuple of (score, band, top_features, model_version)
    """
    if use_dummy:
        score, band, top_features = compute_dummy_score(user_id, "transaction")
        return score, band, top_features, "dummy-v1"

    model = _get_model()
    if model is None:
        logger.warning("Model not available, falling back to dummy scoring")
        score, band, top_features = compute_dummy_score(user_id, "transaction")
        return score, band, top_features, "dummy-v1"

    from services.scorer.features import compute_features

    features = compute_features(user_id, db)
    score, band, top_features = model.predict(features)

    return score, band, top_features, model.version


def _get_model() -> "RiskModel | None":
    """Get or load the model singleton."""
    global _model, _model_load_attempted

    if _model is not None:
        return _model

    if _model_load_attempted:
        return None

    _model_load_attempted = True

    try:
        from shared.config import get_settings
        from shared.model import RiskModel

        settings = get_settings()
        model_dir = Path(settings.model_path).parent

        if not model_dir.exists():
            logger.info(f"Model directory not found: {model_dir}")
            return None

        model_path = model_dir / "model.pkl"
        metadata_path = model_dir / "metadata.json"

        if not model_path.exists() or not metadata_path.exists():
            logger.info("Model files not found, will use dummy scoring")
            return None

        _model = RiskModel(model_dir)
        _model.load()
        logger.info(f"Loaded model: {_model.version}")
        return _model

    except Exception as e:
        logger.warning(f"Failed to load model: {e}")
        return None


def reset_model() -> None:
    """Reset the model singleton (for testing)."""
    global _model, _model_load_attempted
    _model = None
    _model_load_attempted = False
