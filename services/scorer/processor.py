import logging
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

from services.scorer.scoring import compute_dummy_score
from shared import deserialize_event, utcnow
from shared.db import RiskScore

if TYPE_CHECKING:
    from confluent_kafka import Message

logger = logging.getLogger(__name__)


def process_message(
    msg: "Message",
    db: Session,
) -> bool:
    try:
        event = deserialize_event(msg.value())
    except Exception as e:
        logger.error(f"Failed to deserialize message: {e}")
        return False

    logger.info(f"Processing event {event.event_id} for user {event.user_id}")

    score, band, top_features = compute_dummy_score(
        user_id=event.user_id,
        event_type=event.event_type.value,
    )

    risk_score = RiskScore(
        user_id=event.user_id,
        score=score,
        band=band.value,
        computed_at=utcnow(),
        top_features_json=top_features,
        model_version="dummy-v1",
    )

    db.add(risk_score)
    db.commit()

    logger.info(
        f"Scored user {event.user_id}: score={score:.3f}, band={band.value}"
    )

    return True
