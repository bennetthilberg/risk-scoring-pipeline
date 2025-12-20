import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from services.api.dependencies import get_db, get_producer
from shared import (
    EventAcceptedResponse,
    compute_payload_hash,
    parse_event,
    utcnow,
)
from shared.db import Event

logger = logging.getLogger(__name__)

router = APIRouter()


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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid event: {e}",
        ) from None
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Event validation failed: {e}",
        ) from None

    raw_hash = compute_payload_hash(event_data)
    now = utcnow()

    db_event = Event(
        event_id=event.event_id,
        user_id=event.user_id,
        event_type=event.event_type.value,
        ts=event.ts,
        schema_version=event.schema_version,
        payload_json=event.payload.model_dump(),
        raw_payload_hash=raw_hash,
        accepted_at=now,
        published_at=None,
    )

    db.add(db_event)
    db.commit()

    producer = get_producer()
    try:
        message_value = event.model_dump_json().encode("utf-8")
        message_key = event.user_id.encode("utf-8")

        from shared.config import get_settings

        settings = get_settings()
        producer.produce(
            topic=settings.kafka_topic,
            key=message_key,
            value=message_value,
        )
        producer.poll(0)

        db_event.published_at = utcnow()
        db.commit()

        logger.info(f"Event {event.event_id} published to Kafka")
    except Exception as e:
        logger.error(f"Failed to publish event {event.event_id}: {e}")

    return EventAcceptedResponse(event_id=event.event_id, status="accepted")
