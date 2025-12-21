import logging
import time
from uuid import UUID

from sqlalchemy.orm import Session

from shared import utcnow
from shared.config import Settings
from shared.db import DLQEvent

logger = logging.getLogger(__name__)


class NonRetryableError(Exception):
    pass


class RetryableError(Exception):
    pass


def calculate_backoff_ms(retry_count: int, base_delay_ms: int) -> int:
    return int(base_delay_ms * (2**retry_count))


def sleep_with_backoff(retry_count: int, settings: Settings) -> None:
    delay_ms = calculate_backoff_ms(retry_count, settings.retry_base_delay_ms)
    delay_sec = delay_ms / 1000.0
    logger.debug(f"Sleeping {delay_ms}ms before retry {retry_count + 1}")
    time.sleep(delay_sec)


def send_to_dlq(
    raw_payload: bytes,
    failure_reason: str,
    db: Session,
    event_id: UUID | None = None,
    retry_count: int = 0,
) -> None:
    dlq_entry = DLQEvent(
        event_id=event_id,
        raw_payload=raw_payload.decode("utf-8", errors="replace"),
        failure_reason=failure_reason,
        created_at=utcnow(),
        retry_count=retry_count,
    )
    db.add(dlq_entry)
    db.commit()
    logger.warning(
        f"Event sent to DLQ: event_id={event_id}, reason={failure_reason}, retries={retry_count}"
    )


def should_retry(retry_count: int, max_retries: int) -> bool:
    return retry_count < max_retries
