import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from services.api.dependencies import get_db, get_producer
from shared import (
    EventAcceptedResponse,
    compute_payload_hash,
    parse_event,
    utcnow,
)
from shared.config import get_settings
from shared.db import Event
from shared.metrics import EVENTS_INGESTED_TOTAL

logger = logging.getLogger(__name__)

router = APIRouter()


def _publish_event(event: Event, db: Session) -> bool:
    producer = get_producer()
    settings = get_settings()

    try:
        from shared.schemas import parse_event as parse

        parsed = parse(
            {
                "event_id": str(event.event_id),
                "event_type": event.event_type,
                "user_id": event.user_id,
                "ts": event.ts.isoformat(),
                "schema_version": event.schema_version,
                "payload": event.payload_json,
            }
        )

        message_value = parsed.model_dump_json().encode("utf-8")
        message_key = event.user_id.encode("utf-8")

        producer.produce(
            topic=settings.kafka_topic,
            key=message_key,
            value=message_value,
        )
        producer.poll(0)

        event.published_at = utcnow()
        db.commit()

        logger.info(f"Event {event.event_id} published to Kafka")
        return True

    except Exception as e:
        logger.error(f"Failed to publish event {event.event_id}: {e}")
        db.rollback()
        return False


@router.post(
    "",
    response_model=EventAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def ingest_event(
    event_data: dict,
    db: Annotated[Session, Depends(get_db)],
) -> EventAcceptedResponse:
    try:
        event = parse_event(event_data)
    except ValueError as e:
        EVENTS_INGESTED_TOTAL.labels(event_type="unknown", status="invalid").inc()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid event: {e}",
        ) from None
    except Exception as e:
        EVENTS_INGESTED_TOTAL.labels(event_type="unknown", status="invalid").inc()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Event validation failed: {e}",
        ) from None

    event_type = event.event_type.value
    raw_hash = compute_payload_hash(event_data)
    now = utcnow()

    stmt = (
        insert(Event)
        .values(
            event_id=event.event_id,
            user_id=event.user_id,
            event_type=event_type,
            ts=event.ts,
            schema_version=event.schema_version,
            payload_json=event.payload.model_dump(),
            raw_payload_hash=raw_hash,
            accepted_at=now,
            published_at=None,
        )
        .on_conflict_do_nothing(index_elements=["event_id"])
    )

    result = db.execute(stmt)
    db.commit()

    if result.rowcount == 0:  # type: ignore[attr-defined]
        existing = db.execute(select(Event).where(Event.event_id == event.event_id)).scalar_one()

        if existing.published_at is None:
            logger.info(
                f"Duplicate event {event.event_id} with unpublished status, retrying publish"
            )
            _publish_event(existing, db)

        EVENTS_INGESTED_TOTAL.labels(event_type=event_type, status="duplicate").inc()
        return EventAcceptedResponse(event_id=event.event_id, status="accepted")

    db_event = db.execute(select(Event).where(Event.event_id == event.event_id)).scalar_one()

    _publish_event(db_event, db)

    EVENTS_INGESTED_TOTAL.labels(event_type=event_type, status="accepted").inc()
    return EventAcceptedResponse(event_id=event.event_id, status="accepted")
