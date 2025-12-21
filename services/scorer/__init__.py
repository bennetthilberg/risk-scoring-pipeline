"""Scoring worker service for the risk scoring pipeline."""

from services.scorer.main import main, run_worker
from services.scorer.processor import (
    is_already_processed,
    mark_processed,
    process_message,
    process_message_with_retries,
)
from services.scorer.retry import (
    NonRetryableError,
    RetryableError,
    calculate_backoff_ms,
    send_to_dlq,
    should_retry,
    sleep_with_backoff,
)
from services.scorer.scoring import compute_dummy_score

__all__ = [
    "NonRetryableError",
    "RetryableError",
    "calculate_backoff_ms",
    "compute_dummy_score",
    "is_already_processed",
    "main",
    "mark_processed",
    "process_message",
    "process_message_with_retries",
    "run_worker",
    "send_to_dlq",
    "should_retry",
    "sleep_with_backoff",
]
