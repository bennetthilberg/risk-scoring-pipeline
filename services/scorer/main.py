import logging
import signal
from types import FrameType

from confluent_kafka import Consumer, KafkaError
from prometheus_client import start_http_server
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from services.scorer.processor import process_message_with_retries
from shared.config import Settings, get_settings
from shared.metrics import ACTIVE_MODEL_INFO, CONSUMER_LAG

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


def update_consumer_lag(consumer: Consumer, topic: str) -> None:
    try:
        assignment = consumer.assignment()
        if not assignment:
            return

        for tp in assignment:
            low, high = consumer.get_watermark_offsets(tp, timeout=1.0)
            position = consumer.position([tp])[0].offset
            if position >= 0 and high >= 0:
                lag = max(0, high - position)
                CONSUMER_LAG.labels(topic=topic, partition=str(tp.partition)).set(lag)
    except Exception:
        pass


def update_model_info() -> None:
    try:
        from services.scorer.scoring import _get_model

        model = _get_model()
        if model is not None:
            ACTIVE_MODEL_INFO.labels(
                model_version=model.version,
                params_hash=model.metadata.params_hash,
            ).set(1)
    except Exception:
        pass


def run_worker(
    max_messages: int | None = None,
    start_metrics_server: bool = True,
    metrics_port: int = 9100,
) -> int:
    global _shutdown_requested
    _shutdown_requested = False

    settings = get_settings()

    if start_metrics_server:
        try:
            start_http_server(metrics_port)
            logger.info(f"Prometheus metrics server started on port {metrics_port}")
        except Exception as e:
            logger.warning(f"Failed to start metrics server: {e}")

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

    update_model_info()

    messages_processed = 0
    lag_update_counter = 0

    try:
        while not _shutdown_requested:
            if max_messages is not None and messages_processed >= max_messages:
                logger.info(f"Reached max messages limit ({max_messages})")
                break

            msg = consumer.poll(timeout=1.0)

            lag_update_counter += 1
            if lag_update_counter >= 10:
                update_consumer_lag(consumer, settings.kafka_topic)
                lag_update_counter = 0

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
