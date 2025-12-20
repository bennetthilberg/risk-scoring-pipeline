import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from fastapi import FastAPI
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from services.api.dependencies import set_db_session_factory, set_kafka_producer
from services.api.routes import events, health, scores
from shared.config import get_settings

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> "AsyncGenerator[None, None]":
    settings = get_settings()

    engine = create_engine(settings.database_url, pool_pre_ping=True)
    session_factory = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    set_db_session_factory(session_factory)
    logger.info("Database connection established")

    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": settings.kafka_brokers})
    set_kafka_producer(producer)
    logger.info("Kafka producer initialized")

    yield

    producer.flush(timeout=5)
    logger.info("Kafka producer flushed")
    engine.dispose()
    logger.info("Database connection closed")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Risk Scoring API",
        description="Real-time event ingestion and risk score queries",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.include_router(health.router, tags=["health"])
    app.include_router(events.router, prefix="/events", tags=["events"])
    app.include_router(scores.router, prefix="/score", tags=["scores"])

    return app


app = create_app()
