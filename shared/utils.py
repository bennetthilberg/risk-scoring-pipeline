import hashlib
import json
from datetime import UTC, datetime


def compute_payload_hash(payload: dict) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def utcnow() -> datetime:
    return datetime.now(UTC)
