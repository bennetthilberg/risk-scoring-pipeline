import logging
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from services.scorer.scoring import compute_dummy_score
from shared import ProcessingStatus, deserialize_event, utcnow
from shared.db import ProcessedEvent, RiskScore

if TYPE_CHECKING:
    from confluent_kafka import Message

logger = logging.getLogger(__name__)


def is_already_processed(event_id: UUID, db: Session) -> bool:
    result = db.execute(
        select(ProcessedEvent.event_id).where(ProcessedEvent.event_id == event_id)
    ).first()
    return result is not None


def mark_processed(event_id: UUID, status: ProcessingStatus, db: Session) -> bool:
    stmt = (
        insert(ProcessedEvent)
        .values(
            event_id=event_id,
            processed_at=utcnow(),
            status=status.value,
        )
        .on_conflict_do_nothing(index_elements=["event_id"])
    )

    result = db.execute(stmt)
    return result.rowcount > 0  # type: ignore[attr-defined, no-any-return]


def process_message(
    msg: "Message",
    db: Session,
) -> bool:
    try:
        event = deserialize_event(msg.value())
    except Exception as e:
        logger.error(f"Failed to deserialize message: {e}")
        return False

    if is_already_processed(event.event_id, db):
        logger.info(f"Event {event.event_id} already processed, skipping")
        return True

    logger.info(f"Processing event {event.event_id} for user {event.user_id}")

    try:
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

        if not mark_processed(event.event_id, ProcessingStatus.SUCCESS, db):
            logger.warning(f"Event {event.event_id} was processed by another worker")
            db.rollback()
            return True

        db.commit()

        logger.info(f"Scored user {event.user_id}: score={score:.3f}, band={band.value}")
        return True

    except Exception as e:
        logger.error(f"Failed to process event {event.event_id}: {e}")
        db.rollback()
        mark_processed(event.event_id, ProcessingStatus.FAILED, db)
        db.commit()
        return False
