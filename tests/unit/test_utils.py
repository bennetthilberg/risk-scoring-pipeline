from datetime import UTC, datetime

import pytest

from shared.utils import compute_payload_hash, utcnow


@pytest.mark.unit
class TestComputePayloadHash:
    def test_hash_is_sha256_hex(self):
        payload = {"key": "value"}
        h = compute_payload_hash(payload)
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_same_payload_same_hash(self):
        payload = {"amount": 100.0, "currency": "USD"}
        h1 = compute_payload_hash(payload)
        h2 = compute_payload_hash(payload)
        assert h1 == h2

    def test_different_payload_different_hash(self):
        h1 = compute_payload_hash({"amount": 100.0})
        h2 = compute_payload_hash({"amount": 200.0})
        assert h1 != h2

    def test_key_order_does_not_affect_hash(self):
        h1 = compute_payload_hash({"a": 1, "b": 2, "c": 3})
        h2 = compute_payload_hash({"c": 3, "b": 2, "a": 1})
        assert h1 == h2

    def test_nested_dict_order_independent(self):
        h1 = compute_payload_hash({"outer": {"z": 1, "a": 2}})
        h2 = compute_payload_hash({"outer": {"a": 2, "z": 1}})
        assert h1 == h2

    def test_empty_payload(self):
        h = compute_payload_hash({})
        assert len(h) == 64

    def test_complex_payload(self):
        payload = {
            "event_id": "12345678-1234-5678-1234-567812345678",
            "user_id": "user-001",
            "ts": "2024-01-15T12:00:00+00:00",
            "payload": {
                "amount": 150.0,
                "currency": "USD",
                "merchant": "Test Store",
                "country": "US",
            },
        }
        h = compute_payload_hash(payload)
        assert len(h) == 64


@pytest.mark.unit
class TestUtcnow:
    def test_returns_datetime(self):
        result = utcnow()
        assert isinstance(result, datetime)

    def test_has_utc_timezone(self):
        result = utcnow()
        assert result.tzinfo == UTC

    def test_is_recent(self):
        before = datetime.now(UTC)
        result = utcnow()
        after = datetime.now(UTC)
        assert before <= result <= after
