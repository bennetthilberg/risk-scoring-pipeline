#!/usr/bin/env python3
"""Train a logistic regression model on synthetic risk scoring data.

This script generates synthetic user behavior data and trains a simple
logistic regression model for risk scoring. The model and metadata are
saved to the models/ directory.

Usage:
    python scripts/train.py
    python scripts/train.py --output-dir models/ --seed 42
"""

import argparse
import hashlib
import json
import pickle
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from shared.features import FEATURE_DEFAULTS, FEATURE_ORDER


def generate_synthetic_data(
    n_samples: int = 10000,
    fraud_rate: float = 0.15,
    seed: int = 42,
) -> tuple[np.ndarray, np.ndarray]:
    """Generate synthetic risk scoring training data.

    Creates realistic feature distributions where higher risk users have:
    - More transactions in short windows
    - Higher transaction amounts
    - More failed logins
    - Newer accounts
    - More unique countries
    """
    rng = np.random.default_rng(seed)

    n_fraud = int(n_samples * fraud_rate)
    n_legit = n_samples - n_fraud

    legit_features = _generate_legit_users(n_legit, rng)
    fraud_features = _generate_fraud_users(n_fraud, rng)

    X = np.vstack([legit_features, fraud_features])
    y = np.array([0] * n_legit + [1] * n_fraud)

    shuffle_idx = rng.permutation(n_samples)
    return X[shuffle_idx], y[shuffle_idx]


def _generate_legit_users(n: int, rng: np.random.Generator) -> np.ndarray:
    """Generate feature vectors for legitimate users."""
    features = np.zeros((n, len(FEATURE_ORDER)))

    features[:, 0] = rng.poisson(2, n)
    features[:, 1] = rng.exponential(100, n)
    features[:, 2] = rng.choice([0, 0, 0, 0, 1], n)
    features[:, 3] = rng.exponential(180, n) + 30
    features[:, 4] = rng.choice([1, 1, 1, 2], n)
    features[:, 5] = rng.exponential(75, n) + 25

    return features


def _generate_fraud_users(n: int, rng: np.random.Generator) -> np.ndarray:
    """Generate feature vectors for high-risk/fraudulent users."""
    features = np.zeros((n, len(FEATURE_ORDER)))

    features[:, 0] = rng.poisson(8, n) + 3
    features[:, 1] = rng.exponential(500, n) + 200
    features[:, 2] = rng.poisson(2, n)
    features[:, 3] = rng.exponential(10, n) + 1
    features[:, 4] = rng.poisson(3, n) + 2
    features[:, 5] = rng.exponential(300, n) + 100

    return features


def train_model(
    X: np.ndarray,
    y: np.ndarray,
    seed: int = 42,
) -> tuple[LogisticRegression, StandardScaler, dict]:
    """Train logistic regression model with scaling."""
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=seed, stratify=y
    )

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    model = LogisticRegression(
        random_state=seed,
        max_iter=1000,
        class_weight="balanced",
    )
    model.fit(X_train_scaled, y_train)

    train_score = model.score(X_train_scaled, y_train)
    test_score = model.score(X_test_scaled, y_test)

    y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]

    metrics = {
        "train_accuracy": float(train_score),
        "test_accuracy": float(test_score),
        "train_samples": len(y_train),
        "test_samples": len(y_test),
        "fraud_rate": float(y.mean()),
        "mean_score": float(y_pred_proba.mean()),
        "std_score": float(y_pred_proba.std()),
    }

    return model, scaler, metrics


def compute_params_hash(model: LogisticRegression, scaler: StandardScaler) -> str:
    """Compute a hash of model parameters for versioning."""
    params_bytes = pickle.dumps(
        {
            "coef": model.coef_.tolist(),
            "intercept": model.intercept_.tolist(),
            "scaler_mean": scaler.mean_.tolist(),
            "scaler_scale": scaler.scale_.tolist(),
        }
    )
    return hashlib.sha256(params_bytes).hexdigest()[:16]


def save_model(
    model: LogisticRegression,
    scaler: StandardScaler,
    metrics: dict,
    output_dir: Path,
    model_version: str | None = None,
) -> tuple[Path, Path]:
    """Save model artifact and metadata."""
    output_dir.mkdir(parents=True, exist_ok=True)

    params_hash = compute_params_hash(model, scaler)
    if model_version is None:
        model_version = f"v1-{params_hash[:8]}"

    model_path = output_dir / "model.pkl"
    metadata_path = output_dir / "metadata.json"

    with open(model_path, "wb") as f:
        pickle.dump({"model": model, "scaler": scaler}, f)

    coefficients = dict(zip(FEATURE_ORDER, model.coef_[0].tolist(), strict=False))

    metadata = {
        "model_version": model_version,
        "created_at": datetime.now(UTC).isoformat(),
        "feature_order": FEATURE_ORDER,
        "feature_defaults": FEATURE_DEFAULTS,
        "band_thresholds": {"low": 0.33, "med": 0.66},
        "params_hash": params_hash,
        "coefficients": coefficients,
        "intercept": float(model.intercept_[0]),
        "scaler_mean": dict(zip(FEATURE_ORDER, scaler.mean_.tolist(), strict=False)),
        "scaler_scale": dict(zip(FEATURE_ORDER, scaler.scale_.tolist(), strict=False)),
        "metrics": metrics,
    }

    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    return model_path, metadata_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Train risk scoring model")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("models"),
        help="Directory to save model artifacts",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility",
    )
    parser.add_argument(
        "--n-samples",
        type=int,
        default=10000,
        help="Number of training samples to generate",
    )
    parser.add_argument(
        "--fraud-rate",
        type=float,
        default=0.15,
        help="Proportion of high-risk samples",
    )
    args = parser.parse_args()

    print(f"Generating {args.n_samples} synthetic samples...")
    X, y = generate_synthetic_data(
        n_samples=args.n_samples,
        fraud_rate=args.fraud_rate,
        seed=args.seed,
    )
    print(f"  Features shape: {X.shape}")
    print(f"  Fraud rate: {y.mean():.2%}")

    print("\nTraining logistic regression model...")
    model, scaler, metrics = train_model(X, y, seed=args.seed)
    print(f"  Train accuracy: {metrics['train_accuracy']:.4f}")
    print(f"  Test accuracy: {metrics['test_accuracy']:.4f}")

    print(f"\nSaving model to {args.output_dir}/...")
    model_path, metadata_path = save_model(model, scaler, metrics, args.output_dir)
    print(f"  Model: {model_path}")
    print(f"  Metadata: {metadata_path}")

    print("\nFeature coefficients (standardized):")
    for feature, coef in zip(FEATURE_ORDER, model.coef_[0], strict=False):
        print(f"  {feature}: {coef:+.4f}")

    print("\nTraining complete!")


if __name__ == "__main__":
    main()
