"""Unit tests for the demo script utilities."""

import random
from datetime import UTC, datetime, timedelta

import pytest

from scripts.demo import (
    generate_event_id,
    generate_login_event,
    generate_signup_event,
    generate_timestamp,
    generate_transaction_event,
    generate_user_event_sequence,
    generate_user_id,
)


@pytest.mark.unit
class TestUserIdGeneration:
    def test_generate_user_id_format(self) -> None:
        user_id = generate_user_id(0)
        assert user_id == "user-demo-0000"

    def test_generate_user_id_padding(self) -> None:
        assert generate_user_id(1) == "user-demo-0001"
        assert generate_user_id(99) == "user-demo-0099"
        assert generate_user_id(999) == "user-demo-0999"
        assert generate_user_id(9999) == "user-demo-9999"

    def test_generate_user_id_unique(self) -> None:
        user_ids = [generate_user_id(i) for i in range(100)]
        assert len(set(user_ids)) == 100


@pytest.mark.unit
class TestEventIdGeneration:
    def test_generate_event_id_is_valid_uuid(self) -> None:
        import uuid

        event_id = generate_event_id()
        uuid.UUID(event_id)

    def test_generate_event_id_unique(self) -> None:
        event_ids = [generate_event_id() for _ in range(100)]
        assert len(set(event_ids)) == 100


@pytest.mark.unit
class TestTimestampGeneration:
    def test_generate_timestamp_format(self) -> None:
        base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        ts = generate_timestamp(base_time, 0)
        parsed = datetime.fromisoformat(ts)
        assert parsed == base_time

    def test_generate_timestamp_with_offset(self) -> None:
        base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        ts = generate_timestamp(base_time, 30)
        parsed = datetime.fromisoformat(ts)
        assert parsed == base_time + timedelta(minutes=30)


@pytest.mark.unit
class TestSignupEventGeneration:
    def test_generates_valid_signup_event(self) -> None:
        rng = random.Random(42)
        ts = datetime.now(UTC).isoformat()

        event = generate_signup_event("user-001", ts, rng)

        assert event["event_type"] == "signup"
        assert event["user_id"] == "user-001"
        assert event["ts"] == ts
        assert event["schema_version"] == 1
        assert "event_id" in event
        assert "payload" in event

    def test_signup_payload_has_required_fields(self) -> None:
        rng = random.Random(42)
        ts = datetime.now(UTC).isoformat()

        event = generate_signup_event("user-001", ts, rng)
        payload = event["payload"]

        assert "email_domain" in payload
        assert "country" in payload
        assert "device_id" in payload

    def test_risky_signup_uses_suspicious_domains(self) -> None:
        ts = datetime.now(UTC).isoformat()

        risky_domains = {"temp-mail.org", "mail.ru", "disposable.com"}
        found_risky = False

        for _ in range(20):
            event = generate_signup_event("user-001", ts, random.Random(), is_risky=True)
            if event["payload"]["email_domain"] in risky_domains:
                found_risky = True
                break

        assert found_risky, "Expected risky signup to use suspicious email domains"

    def test_deterministic_with_same_seed(self) -> None:
        ts = datetime.now(UTC).isoformat()

        event1 = generate_signup_event("user-001", ts, random.Random(42))
        event2 = generate_signup_event("user-001", ts, random.Random(42))

        assert event1["payload"]["email_domain"] == event2["payload"]["email_domain"]
        assert event1["payload"]["country"] == event2["payload"]["country"]


@pytest.mark.unit
class TestLoginEventGeneration:
    def test_generates_valid_login_event(self) -> None:
        rng = random.Random(42)
        ts = datetime.now(UTC).isoformat()

        event = generate_login_event("user-001", ts, rng)

        assert event["event_type"] == "login"
        assert event["user_id"] == "user-001"
        assert event["schema_version"] == 1

    def test_login_payload_has_required_fields(self) -> None:
        rng = random.Random(42)
        ts = datetime.now(UTC).isoformat()

        event = generate_login_event("user-001", ts, rng)
        payload = event["payload"]

        assert "ip" in payload
        assert "success" in payload
        assert "device_id" in payload
        assert isinstance(payload["success"], bool)

    def test_ip_address_format(self) -> None:
        rng = random.Random(42)
        ts = datetime.now(UTC).isoformat()

        event = generate_login_event("user-001", ts, rng)
        ip = event["payload"]["ip"]

        parts = ip.split(".")
        assert len(parts) == 4
        for part in parts:
            assert 0 <= int(part) <= 255


@pytest.mark.unit
class TestTransactionEventGeneration:
    def test_generates_valid_transaction_event(self) -> None:
        rng = random.Random(42)
        ts = datetime.now(UTC).isoformat()

        event = generate_transaction_event("user-001", ts, rng)

        assert event["event_type"] == "transaction"
        assert event["user_id"] == "user-001"
        assert event["schema_version"] == 1

    def test_transaction_payload_has_required_fields(self) -> None:
        rng = random.Random(42)
        ts = datetime.now(UTC).isoformat()

        event = generate_transaction_event("user-001", ts, rng)
        payload = event["payload"]

        assert "amount" in payload
        assert "currency" in payload
        assert "merchant" in payload
        assert "country" in payload

    def test_transaction_amount_positive(self) -> None:
        ts = datetime.now(UTC).isoformat()

        for _ in range(20):
            event = generate_transaction_event("user-001", ts, random.Random())
            assert event["payload"]["amount"] > 0

    def test_risky_transaction_has_higher_amounts(self) -> None:
        ts = datetime.now(UTC).isoformat()

        risky_amounts = []
        normal_amounts = []

        for i in range(50):
            risky_event = generate_transaction_event(
                "user-001", ts, random.Random(i), is_risky=True
            )
            normal_event = generate_transaction_event(
                "user-001", ts, random.Random(i), is_risky=False
            )
            risky_amounts.append(risky_event["payload"]["amount"])
            normal_amounts.append(normal_event["payload"]["amount"])

        assert sum(risky_amounts) / len(risky_amounts) > sum(normal_amounts) / len(normal_amounts)


@pytest.mark.unit
class TestUserEventSequenceGeneration:
    def test_generates_correct_number_of_events(self) -> None:
        rng = random.Random(42)
        base_time = datetime.now(UTC)

        events = generate_user_event_sequence("user-001", 5, rng, base_time)

        assert len(events) == 5

    def test_first_event_is_signup(self) -> None:
        rng = random.Random(42)
        base_time = datetime.now(UTC)

        events = generate_user_event_sequence("user-001", 10, rng, base_time)

        assert events[0]["event_type"] == "signup"

    def test_all_events_have_same_user_id(self) -> None:
        rng = random.Random(42)
        base_time = datetime.now(UTC)

        events = generate_user_event_sequence("user-001", 10, rng, base_time)

        for event in events:
            assert event["user_id"] == "user-001"

    def test_events_have_increasing_timestamps(self) -> None:
        rng = random.Random(42)
        base_time = datetime.now(UTC)

        events = generate_user_event_sequence("user-001", 10, rng, base_time)

        timestamps = [datetime.fromisoformat(e["ts"]) for e in events]
        for i in range(1, len(timestamps)):
            assert timestamps[i] > timestamps[i - 1]

    def test_subsequent_events_are_login_or_transaction(self) -> None:
        rng = random.Random(42)
        base_time = datetime.now(UTC)

        events = generate_user_event_sequence("user-001", 10, rng, base_time)

        for event in events[1:]:
            assert event["event_type"] in ("login", "transaction")

    def test_deterministic_with_same_seed(self) -> None:
        base_time = datetime.now(UTC)

        events1 = generate_user_event_sequence("user-001", 10, random.Random(42), base_time)
        events2 = generate_user_event_sequence("user-001", 10, random.Random(42), base_time)

        for e1, e2 in zip(events1, events2, strict=True):
            assert e1["event_type"] == e2["event_type"]
            assert e1["ts"] == e2["ts"]
