from collections.abc import Callable, Generator
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from confluent_kafka import Producer

_db_session_factory: Callable[[], Session] | None = None
_kafka_producer: "Producer | None" = None


def set_db_session_factory(factory: Callable[[], Session]) -> None:
    global _db_session_factory
    _db_session_factory = factory


def set_kafka_producer(producer: "Producer") -> None:
    global _kafka_producer
    _kafka_producer = producer


def get_db() -> Generator[Session, None, None]:
    if _db_session_factory is None:
        raise RuntimeError("Database session factory not initialized")
    session = _db_session_factory()
    try:
        yield session
    finally:
        session.close()


def get_producer() -> "Producer":
    if _kafka_producer is None:
        raise RuntimeError("Kafka producer not initialized")
    return _kafka_producer
