"""Feature computation for risk scoring.

Computes rolling window features from the events table for a given user.
Features align with FEATURE_ORDER to ensure training/inference consistency.
"""

import logging
from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from shared import utcnow
from shared.db import Event
from shared.features import FEATURE_DEFAULTS, FEATURE_ORDER

logger = logging.getLogger(__name__)


def compute_features(
    user_id: str, db: Session, as_of: "datetime | None" = None
) -> dict[str, float]:
    """Compute all features for a user at a given point in time.

    Args:
        user_id: The user to compute features for
        db: Database session
        as_of: Point in time to compute features (defaults to now)

    Returns:
        Dictionary mapping feature names to values
    """
    if as_of is None:
        as_of = utcnow()

    features = dict(FEATURE_DEFAULTS)

    features["txn_count_24h"] = _txn_count_window(user_id, db, as_of, hours=24)
    features["txn_amount_sum_24h"] = _txn_amount_sum_window(user_id, db, as_of, hours=24)
    features["failed_logins_1h"] = _failed_logins_window(user_id, db, as_of, hours=1)
    features["account_age_days"] = _account_age_days(user_id, db, as_of)
    features["unique_countries_7d"] = _unique_countries_window(user_id, db, as_of, days=7)
    features["avg_txn_amount_30d"] = _avg_txn_amount_window(user_id, db, as_of, days=30)

    return features


def _txn_count_window(user_id: str, db: Session, as_of: "datetime", hours: int) -> int:
    """Count transactions in the last N hours."""
    window_start = as_of - timedelta(hours=hours)

    result = db.execute(
        select(func.count())
        .select_from(Event)
        .where(
            Event.user_id == user_id,
            Event.event_type == "transaction",
            Event.ts >= window_start,
            Event.ts <= as_of,
        )
    ).scalar()

    return int(result or 0)


def _txn_amount_sum_window(user_id: str, db: Session, as_of: "datetime", hours: int) -> float:
    """Sum transaction amounts in the last N hours."""
    window_start = as_of - timedelta(hours=hours)

    events = (
        db.query(Event)
        .filter(
            Event.user_id == user_id,
            Event.event_type == "transaction",
            Event.ts >= window_start,
            Event.ts <= as_of,
        )
        .all()
    )

    total = 0.0
    for event in events:
        payload = event.payload_json
        if isinstance(payload, dict) and "amount" in payload:
            total += float(payload["amount"])

    return total


def _failed_logins_window(user_id: str, db: Session, as_of: "datetime", hours: int) -> int:
    """Count failed logins in the last N hours."""
    window_start = as_of - timedelta(hours=hours)

    events = (
        db.query(Event)
        .filter(
            Event.user_id == user_id,
            Event.event_type == "login",
            Event.ts >= window_start,
            Event.ts <= as_of,
        )
        .all()
    )

    failed_count = 0
    for event in events:
        payload = event.payload_json
        if isinstance(payload, dict) and payload.get("success") is False:
            failed_count += 1

    return failed_count


def _account_age_days(user_id: str, db: Session, as_of: "datetime") -> int:
    """Days since first event (signup) for user."""
    first_event = db.query(Event).filter(Event.user_id == user_id).order_by(Event.ts.asc()).first()

    if first_event is None:
        return 0

    delta = as_of - first_event.ts.replace(tzinfo=as_of.tzinfo)
    return max(0, delta.days)


def _unique_countries_window(user_id: str, db: Session, as_of: "datetime", days: int) -> int:
    """Count unique countries from transactions and signups in last N days."""
    window_start = as_of - timedelta(days=days)

    events = (
        db.query(Event)
        .filter(
            Event.user_id == user_id,
            Event.event_type.in_(["transaction", "signup"]),
            Event.ts >= window_start,
            Event.ts <= as_of,
        )
        .all()
    )

    countries = set()
    for event in events:
        payload = event.payload_json
        if isinstance(payload, dict) and "country" in payload:
            countries.add(payload["country"])

    return len(countries)


def _avg_txn_amount_window(user_id: str, db: Session, as_of: "datetime", days: int) -> float:
    """Average transaction amount in last N days."""
    window_start = as_of - timedelta(days=days)

    events = (
        db.query(Event)
        .filter(
            Event.user_id == user_id,
            Event.event_type == "transaction",
            Event.ts >= window_start,
            Event.ts <= as_of,
        )
        .all()
    )

    if not events:
        return 0.0

    total = 0.0
    count = 0
    for event in events:
        payload = event.payload_json
        if isinstance(payload, dict) and "amount" in payload:
            total += float(payload["amount"])
            count += 1

    return total / count if count > 0 else 0.0


def validate_feature_order() -> bool:
    """Validate that computed features match FEATURE_ORDER."""
    computed_features = list(FEATURE_DEFAULTS.keys())
    return computed_features == FEATURE_ORDER
