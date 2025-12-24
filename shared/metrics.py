"""Prometheus metrics definitions for API and worker services."""

from prometheus_client import Counter, Gauge, Histogram

HTTP_REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint", "status"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
)

EVENTS_INGESTED_TOTAL = Counter(
    "events_ingested_total",
    "Total events ingested via API",
    ["event_type", "status"],
)

EVENTS_PROCESSED_TOTAL = Counter(
    "events_processed_total",
    "Total events processed by worker",
    ["event_type", "status"],
)

SCORING_DURATION = Histogram(
    "scoring_duration_seconds",
    "Time to compute risk score",
    ["model_version"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5],
)

DLQ_EVENTS_TOTAL = Counter(
    "dlq_events_total",
    "Total events sent to dead letter queue",
    ["reason"],
)

CONSUMER_LAG = Gauge(
    "consumer_lag",
    "Kafka consumer lag (messages behind)",
    ["topic", "partition"],
)

ACTIVE_MODEL_INFO = Gauge(
    "active_model_info",
    "Currently loaded model information",
    ["model_version", "params_hash"],
)

RETRY_ATTEMPTS_TOTAL = Counter(
    "retry_attempts_total",
    "Total retry attempts",
    ["attempt_number"],
)

DB_QUERY_DURATION = Histogram(
    "db_query_duration_seconds",
    "Database query duration in seconds",
    ["operation"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)
