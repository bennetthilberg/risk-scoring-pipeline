import logging
import time
from collections.abc import Callable
from typing import TYPE_CHECKING
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from services.scorer.retry import NonRetryableError, RetryableError, send_to_dlq
from services.scorer.scoring import compute_score
from shared import ProcessingStatus, deserialize_event, utcnow
from shared.config import Settings
from shared.db import ProcessedEvent, RiskScore
from shared.metrics import (
    DLQ_EVENTS_TOTAL,
    EVENTS_PROCESSED_TOTAL,
    RETRY_ATTEMPTS_TOTAL,
    SCORING_DURATION,
)

if TYPE_CHECKING:
    from confluent_kafka import Message

    from shared import EventEnvelope

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
    retry_count: int = 0,
    settings: Settings | None = None,
) -> tuple[bool, bool]:
    """Process a Kafka message and return (success, should_retry).

    Returns:
        tuple[bool, bool]: (success, should_retry)
            - success: True if message processed successfully or should be skipped
            - should_retry: True if message should be retried (only when success=False)
    """
    raw_payload = msg.value()

    try:
        event = deserialize_event(raw_payload)
    except (ValidationError, ValueError, TypeError) as e:
        logger.error(f"Schema validation failed (non-retryable): {e}")
        send_to_dlq(
            raw_payload=raw_payload,
            failure_reason=f"Schema validation failed: {e}",
            db=db,
            event_id=None,
            retry_count=retry_count,
        )
        DLQ_EVENTS_TOTAL.labels(reason="schema_validation").inc()
        EVENTS_PROCESSED_TOTAL.labels(event_type="unknown", status="dlq").inc()
        return (True, False)
    except Exception as e:
        logger.error(f"Failed to deserialize message: {e}")
        send_to_dlq(
            raw_payload=raw_payload,
            failure_reason=f"Deserialization failed: {e}",
            db=db,
            event_id=None,
            retry_count=retry_count,
        )
        DLQ_EVENTS_TOTAL.labels(reason="deserialization").inc()
        EVENTS_PROCESSED_TOTAL.labels(event_type="unknown", status="dlq").inc()
        return (True, False)

    event_type = event.event_type.value

    if is_already_processed(event.event_id, db):
        logger.info(f"Event {event.event_id} already processed, skipping")
        EVENTS_PROCESSED_TOTAL.labels(event_type=event_type, status="skipped").inc()
        return (True, False)

    logger.info(f"Processing event {event.event_id} for user {event.user_id}")

    try:
        _process_event(event, db)
        EVENTS_PROCESSED_TOTAL.labels(event_type=event_type, status="success").inc()
        return (True, False)

    except NonRetryableError as e:
        logger.error(f"Non-retryable error for event {event.event_id}: {e}")
        db.rollback()
        send_to_dlq(
            raw_payload=raw_payload,
            failure_reason=str(e),
            db=db,
            event_id=event.event_id,
            retry_count=retry_count,
        )
        mark_processed(event.event_id, ProcessingStatus.FAILED, db)
        db.commit()
        DLQ_EVENTS_TOTAL.labels(reason="non_retryable").inc()
        EVENTS_PROCESSED_TOTAL.labels(event_type=event_type, status="dlq").inc()
        return (True, False)

    except RetryableError as e:
        logger.warning(f"Retryable error for event {event.event_id}: {e}")
        db.rollback()
        return (False, True)

    except Exception as e:
        logger.error(f"Unexpected error processing event {event.event_id}: {e}")
        db.rollback()
        return (False, True)


def _process_event(event: "EventEnvelope", db: Session) -> None:
    start_time = time.perf_counter()
    score, band, top_features, model_version = compute_score(
        user_id=event.user_id,
        db=db,
    )
    scoring_duration = time.perf_counter() - start_time
    SCORING_DURATION.labels(model_version=model_version).observe(scoring_duration)

    risk_score = RiskScore(
        user_id=event.user_id,
        score=score,
        band=band.value,
        computed_at=utcnow(),
        top_features_json=top_features,
        model_version=model_version,
    )

    db.add(risk_score)

    if not mark_processed(event.event_id, ProcessingStatus.SUCCESS, db):
        logger.warning(f"Event {event.event_id} was processed by another worker")
        db.rollback()
        return

    db.commit()
    logger.info(f"Scored user {event.user_id}: score={score:.3f}, band={band.value}")


def process_message_with_retries(
    msg: "Message",
    db_factory: Callable[[], Session],
    settings: Settings,
) -> bool:
    """Process a message with retry logic.

    Returns True if message was processed successfully (or sent to DLQ).
    """
    from services.scorer.retry import send_to_dlq, should_retry, sleep_with_backoff

    retry_count = 0
    raw_payload = msg.value()

    while True:
        db = db_factory()
        try:
            success, should_retry_flag = process_message(msg, db, retry_count, settings)

            if success:
                return True

            if not should_retry_flag:
                return True

            if not should_retry(retry_count, settings.max_retries):
                logger.error(f"Max retries ({settings.max_retries}) exceeded, sending to DLQ")
                try:
                    event = deserialize_event(raw_payload)
                    event_id = event.event_id
                    event_type = event.event_type.value
                except Exception:
                    event_id = None
                    event_type = "unknown"

                send_to_dlq(
                    raw_payload=raw_payload,
                    failure_reason=f"Max retries ({settings.max_retries}) exceeded",
                    db=db,
                    event_id=event_id,
                    retry_count=retry_count,
                )
                DLQ_EVENTS_TOTAL.labels(reason="max_retries").inc()
                EVENTS_PROCESSED_TOTAL.labels(event_type=event_type, status="dlq").inc()
                return True

            retry_count += 1
            RETRY_ATTEMPTS_TOTAL.labels(attempt_number=str(retry_count)).inc()
            sleep_with_backoff(retry_count - 1, settings)

        finally:
            db.close()
