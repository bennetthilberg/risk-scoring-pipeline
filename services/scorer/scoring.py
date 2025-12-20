import random

from shared import RiskBand, score_to_band
from shared.features import FEATURE_ORDER


def compute_dummy_score(user_id: str, event_type: str) -> tuple[float, RiskBand, dict[str, float]]:
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
