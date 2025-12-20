import pytest
from pydantic import ValidationError

from shared.schemas import (
    LoginEvent,
    LoginPayload,
    SignupEvent,
    SignupPayload,
    TransactionEvent,
    TransactionPayload,
    parse_event,
)


@pytest.mark.unit
class TestSignupPayload:
    def test_valid_payload(self):
        payload = SignupPayload(
            email_domain="example.com",
            country="US",
            device_id="device-123",
        )
        assert payload.email_domain == "example.com"
        assert payload.country == "US"
        assert payload.device_id == "device-123"

    def test_missing_required_field(self):
        with pytest.raises(ValidationError) as exc:
            SignupPayload(email_domain="example.com", country="US")
        errors = exc.value.errors()
        assert any(e["loc"] == ("device_id",) for e in errors)

    def test_empty_email_domain_rejected(self):
        with pytest.raises(ValidationError) as exc:
            SignupPayload(email_domain="", country="US", device_id="d1")
        errors = exc.value.errors()
        assert any(e["loc"] == ("email_domain",) for e in errors)

    def test_invalid_country_length(self):
        with pytest.raises(ValidationError) as exc:
            SignupPayload(email_domain="ex.com", country="USA", device_id="d1")
        errors = exc.value.errors()
        assert any(e["loc"] == ("country",) for e in errors)

    def test_extra_fields_forbidden(self):
        with pytest.raises(ValidationError) as exc:
            SignupPayload(
                email_domain="ex.com",
                country="US",
                device_id="d1",
                extra_field="not allowed",
            )
        errors = exc.value.errors()
        assert any("extra" in str(e) for e in errors)


@pytest.mark.unit
class TestLoginPayload:
    def test_valid_payload(self):
        payload = LoginPayload(ip="192.168.1.1", success=True, device_id="dev-1")
        assert payload.ip == "192.168.1.1"
        assert payload.success is True
        assert payload.device_id == "dev-1"

    def test_valid_ipv6(self):
        payload = LoginPayload(
            ip="2001:0db8:85a3:0000:0000:8a2e:0370:7334",
            success=False,
            device_id="dev-1",
        )
        assert payload.success is False

    def test_missing_success_field(self):
        with pytest.raises(ValidationError) as exc:
            LoginPayload(ip="192.168.1.1", device_id="d1")
        errors = exc.value.errors()
        assert any(e["loc"] == ("success",) for e in errors)

    def test_invalid_ip_too_short(self):
        with pytest.raises(ValidationError) as exc:
            LoginPayload(ip="1.1", success=True, device_id="d1")
        errors = exc.value.errors()
        assert any(e["loc"] == ("ip",) for e in errors)


@pytest.mark.unit
class TestTransactionPayload:
    def test_valid_payload(self):
        payload = TransactionPayload(
            amount=100.50,
            currency="USD",
            merchant="Test Store",
            country="US",
        )
        assert payload.amount == 100.50
        assert payload.currency == "USD"
        assert payload.merchant == "Test Store"
        assert payload.country == "US"

    def test_zero_amount_rejected(self):
        with pytest.raises(ValidationError) as exc:
            TransactionPayload(
                amount=0,
                currency="USD",
                merchant="Store",
                country="US",
            )
        errors = exc.value.errors()
        assert any(e["loc"] == ("amount",) for e in errors)

    def test_negative_amount_rejected(self):
        with pytest.raises(ValidationError) as exc:
            TransactionPayload(
                amount=-50.0,
                currency="USD",
                merchant="Store",
                country="US",
            )
        errors = exc.value.errors()
        assert any(e["loc"] == ("amount",) for e in errors)

    def test_invalid_currency_length(self):
        with pytest.raises(ValidationError) as exc:
            TransactionPayload(
                amount=100.0,
                currency="US",
                merchant="Store",
                country="US",
            )
        errors = exc.value.errors()
        assert any(e["loc"] == ("currency",) for e in errors)


@pytest.mark.unit
class TestSignupEvent:
    def test_valid_event(self, fixed_uuid, fixed_timestamp):
        event = SignupEvent(
            event_id=fixed_uuid,
            user_id="user-001",
            ts=fixed_timestamp,
            schema_version=1,
            payload=SignupPayload(
                email_domain="example.com",
                country="US",
                device_id="d1",
            ),
        )
        assert event.event_type.value == "signup"
        assert event.event_id == fixed_uuid
        assert event.user_id == "user-001"

    def test_iso_timestamp_parsing(self, fixed_uuid):
        event = SignupEvent(
            event_id=fixed_uuid,
            user_id="user-001",
            ts="2024-01-15T12:00:00+00:00",
            schema_version=1,
            payload=SignupPayload(
                email_domain="example.com",
                country="US",
                device_id="d1",
            ),
        )
        assert event.ts.year == 2024
        assert event.ts.month == 1
        assert event.ts.day == 15

    def test_z_suffix_timestamp_parsing(self, fixed_uuid):
        event = SignupEvent(
            event_id=fixed_uuid,
            user_id="user-001",
            ts="2024-01-15T12:00:00Z",
            schema_version=1,
            payload=SignupPayload(
                email_domain="example.com",
                country="US",
                device_id="d1",
            ),
        )
        assert event.ts.tzinfo is not None

    def test_missing_user_id(self, fixed_uuid, fixed_timestamp):
        with pytest.raises(ValidationError) as exc:
            SignupEvent(
                event_id=fixed_uuid,
                ts=fixed_timestamp,
                schema_version=1,
                payload=SignupPayload(
                    email_domain="example.com",
                    country="US",
                    device_id="d1",
                ),
            )
        errors = exc.value.errors()
        assert any(e["loc"] == ("user_id",) for e in errors)

    def test_empty_user_id_rejected(self, fixed_uuid, fixed_timestamp):
        with pytest.raises(ValidationError) as exc:
            SignupEvent(
                event_id=fixed_uuid,
                user_id="",
                ts=fixed_timestamp,
                schema_version=1,
                payload=SignupPayload(
                    email_domain="example.com",
                    country="US",
                    device_id="d1",
                ),
            )
        errors = exc.value.errors()
        assert any(e["loc"] == ("user_id",) for e in errors)

    def test_invalid_schema_version(self, fixed_uuid, fixed_timestamp):
        with pytest.raises(ValidationError) as exc:
            SignupEvent(
                event_id=fixed_uuid,
                user_id="user-001",
                ts=fixed_timestamp,
                schema_version=0,
                payload=SignupPayload(
                    email_domain="example.com",
                    country="US",
                    device_id="d1",
                ),
            )
        errors = exc.value.errors()
        assert any(e["loc"] == ("schema_version",) for e in errors)


@pytest.mark.unit
class TestLoginEvent:
    def test_valid_event(self, fixed_uuid, fixed_timestamp):
        event = LoginEvent(
            event_id=fixed_uuid,
            user_id="user-001",
            ts=fixed_timestamp,
            schema_version=1,
            payload=LoginPayload(ip="192.168.1.1", success=True, device_id="d1"),
        )
        assert event.event_type.value == "login"
        assert event.payload.success is True

    def test_failed_login(self, fixed_uuid, fixed_timestamp):
        event = LoginEvent(
            event_id=fixed_uuid,
            user_id="user-001",
            ts=fixed_timestamp,
            schema_version=1,
            payload=LoginPayload(ip="10.0.0.1", success=False, device_id="d1"),
        )
        assert event.payload.success is False


@pytest.mark.unit
class TestTransactionEvent:
    def test_valid_event(self, fixed_uuid, fixed_timestamp):
        event = TransactionEvent(
            event_id=fixed_uuid,
            user_id="user-001",
            ts=fixed_timestamp,
            schema_version=1,
            payload=TransactionPayload(
                amount=250.00,
                currency="EUR",
                merchant="Shop",
                country="DE",
            ),
        )
        assert event.event_type.value == "transaction"
        assert event.payload.amount == 250.00
        assert event.payload.currency == "EUR"


@pytest.mark.unit
class TestParseEvent:
    def test_parse_signup_event(self, sample_signup_event):
        event = parse_event(sample_signup_event)
        assert isinstance(event, SignupEvent)
        assert event.event_type.value == "signup"

    def test_parse_login_event(self, sample_login_event):
        event = parse_event(sample_login_event)
        assert isinstance(event, LoginEvent)
        assert event.event_type.value == "login"

    def test_parse_transaction_event(self, sample_transaction_event):
        event = parse_event(sample_transaction_event)
        assert isinstance(event, TransactionEvent)
        assert event.event_type.value == "transaction"

    def test_unknown_event_type_raises(self, fixed_uuid, fixed_timestamp):
        data = {
            "event_id": str(fixed_uuid),
            "event_type": "unknown",
            "user_id": "user-001",
            "ts": fixed_timestamp.isoformat(),
            "schema_version": 1,
            "payload": {},
        }
        with pytest.raises(ValueError, match="Unknown event type"):
            parse_event(data)

    def test_missing_event_type_raises(self, fixed_uuid, fixed_timestamp):
        data = {
            "event_id": str(fixed_uuid),
            "user_id": "user-001",
            "ts": fixed_timestamp.isoformat(),
            "schema_version": 1,
            "payload": {},
        }
        with pytest.raises(ValueError, match="Unknown event type"):
            parse_event(data)


@pytest.mark.unit
class TestEventSerialization:
    def test_signup_event_to_dict(self, fixed_uuid, fixed_timestamp):
        event = SignupEvent(
            event_id=fixed_uuid,
            user_id="user-001",
            ts=fixed_timestamp,
            schema_version=1,
            payload=SignupPayload(
                email_domain="example.com",
                country="US",
                device_id="d1",
            ),
        )
        data = event.model_dump(mode="json")
        assert data["event_id"] == str(fixed_uuid)
        assert data["event_type"] == "signup"
        assert data["payload"]["email_domain"] == "example.com"

    def test_roundtrip_serialization(self, sample_signup_event):
        event = parse_event(sample_signup_event)
        data = event.model_dump(mode="json")
        reparsed = parse_event(data)
        assert reparsed.event_id == event.event_id
        assert reparsed.user_id == event.user_id
        assert reparsed.payload.email_domain == event.payload.email_domain
