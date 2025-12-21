import logging
import signal
from types import FrameType

from confluent_kafka import Consumer, KafkaError
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from services.scorer.processor import process_message_with_retries
from shared.config import Settings, get_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

_shutdown_requested = False


def signal_handler(signum: int, frame: FrameType | None) -> None:
    global _shutdown_requested
    logger.info(f"Received signal {signum}, initiating shutdown...")
    _shutdown_requested = True


def create_consumer(settings: Settings) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_brokers,
            "group.id": settings.consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def run_worker(max_messages: int | None = None) -> int:
    global _shutdown_requested
    _shutdown_requested = False

    settings = get_settings()

    engine = create_engine(settings.database_url, pool_pre_ping=True)
    SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

    def db_factory() -> "Session":
        return SessionLocal()

    consumer = create_consumer(settings)
    consumer.subscribe([settings.kafka_topic])

    logger.info(f"Worker started, consuming from {settings.kafka_topic}")
    logger.info(
        f"Retry policy: max_retries={settings.max_retries}, "
        f"base_delay={settings.retry_base_delay_ms}ms"
    )

    messages_processed = 0

    try:
        while not _shutdown_requested:
            if max_messages is not None and messages_processed >= max_messages:
                logger.info(f"Reached max messages limit ({max_messages})")
                break

            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                continue

            success = process_message_with_retries(msg, db_factory, settings)
            if success:
                consumer.commit(msg)
                messages_processed += 1

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    finally:
        logger.info("Closing consumer...")
        consumer.close()
        engine.dispose()
        logger.info(f"Worker stopped. Processed {messages_processed} messages.")

    return messages_processed


def main() -> None:
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    run_worker()


if __name__ == "__main__":
    main()
