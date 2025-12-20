import json
from unittest.mock import MagicMock

import pytest

from shared.kafka import (
    deserialize_event,
    deserialize_message,
    get_message_key,
    serialize_event,
)
from shared.schemas import LoginEvent, LoginPayload, SignupEvent, SignupPayload


@pytest.mark.unit
class TestSerializeEvent:
    def test_serialize_signup_event(self, sample_signup_event):
        from shared.schemas import parse_event

        event = parse_event(sample_signup_event)
        data = serialize_event(event)

        assert isinstance(data, bytes)
        parsed = json.loads(data.decode("utf-8"))
        assert parsed["event_type"] == "signup"
        assert parsed["user_id"] == event.user_id

    def test_serialize_produces_valid_json(self, sample_login_event):
        from shared.schemas import parse_event

        event = parse_event(sample_login_event)
        data = serialize_event(event)

        parsed = json.loads(data.decode("utf-8"))
        assert "event_id" in parsed
        assert "payload" in parsed


@pytest.mark.unit
class TestDeserializeEvent:
    def test_deserialize_signup_event(self, sample_signup_event):
        data = json.dumps(sample_signup_event).encode("utf-8")
        event = deserialize_event(data)

        assert isinstance(event, SignupEvent)
        assert event.user_id == sample_signup_event["user_id"]

    def test_deserialize_login_event(self, sample_login_event):
        data = json.dumps(sample_login_event).encode("utf-8")
        event = deserialize_event(data)

        assert isinstance(event, LoginEvent)
        assert event.payload.success is True

    def test_deserialize_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            deserialize_event(b"not valid json")

    def test_deserialize_unknown_type_raises(self, fixed_uuid, fixed_timestamp):
        data = json.dumps({
            "event_id": str(fixed_uuid),
            "event_type": "unknown",
            "user_id": "user-001",
            "ts": fixed_timestamp.isoformat(),
            "schema_version": 1,
            "payload": {},
        }).encode("utf-8")

        with pytest.raises(ValueError, match="Unknown event type"):
            deserialize_event(data)


@pytest.mark.unit
class TestDeserializeMessage:
    def test_deserialize_kafka_message(self, sample_signup_event):
        msg = MagicMock()
        msg.value.return_value = json.dumps(sample_signup_event).encode("utf-8")

        event = deserialize_message(msg)

        assert isinstance(event, SignupEvent)
        msg.value.assert_called_once()


@pytest.mark.unit
class TestGetMessageKey:
    def test_key_is_user_id_bytes(self, sample_signup_event):
        from shared.schemas import parse_event

        event = parse_event(sample_signup_event)
        key = get_message_key(event)

        assert isinstance(key, bytes)
        assert key == event.user_id.encode("utf-8")

    def test_key_consistency(self, fixed_uuid, fixed_timestamp):
        event1 = SignupEvent(
            event_id=fixed_uuid,
            user_id="user-123",
            ts=fixed_timestamp,
            payload=SignupPayload(email_domain="ex.com", country="US", device_id="d1"),
        )
        event2 = LoginEvent(
            event_id=fixed_uuid,
            user_id="user-123",
            ts=fixed_timestamp,
            payload=LoginPayload(ip="1.2.3.4", success=True, device_id="d1"),
        )

        assert get_message_key(event1) == get_message_key(event2)
        assert get_message_key(event1) == b"user-123"


@pytest.mark.unit
class TestRoundtrip:
    def test_serialize_deserialize_roundtrip(self, sample_signup_event):
        from shared.schemas import parse_event

        original = parse_event(sample_signup_event)
        serialized = serialize_event(original)
        deserialized = deserialize_event(serialized)

        assert deserialized.event_id == original.event_id
        assert deserialized.user_id == original.user_id
        assert deserialized.event_type == original.event_type
        assert deserialized.payload.email_domain == original.payload.email_domain
