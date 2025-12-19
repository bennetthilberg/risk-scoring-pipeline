"""
Placeholder unit tests to verify pytest configuration works.

These tests will be replaced with real tests as we build out the codebase.
"""

import pytest


@pytest.mark.unit
def test_placeholder_passes() -> None:
    """Basic test to verify pytest runs correctly."""
    assert True


@pytest.mark.unit
def test_fixed_uuid_fixture(fixed_uuid) -> None:
    """Verify the fixed_uuid fixture works."""
    import uuid

    assert isinstance(fixed_uuid, uuid.UUID)
    assert str(fixed_uuid) == "12345678-1234-5678-1234-567812345678"


@pytest.mark.unit
def test_fixed_timestamp_fixture(fixed_timestamp) -> None:
    """Verify the fixed_timestamp fixture provides UTC datetime."""
    from datetime import UTC, datetime

    assert isinstance(fixed_timestamp, datetime)
    assert fixed_timestamp.tzinfo == UTC


@pytest.mark.unit
def test_sample_signup_event_fixture(sample_signup_event) -> None:
    """Verify the sample_signup_event fixture has expected structure."""
    assert "event_id" in sample_signup_event
    assert "event_type" in sample_signup_event
    assert sample_signup_event["event_type"] == "signup"
    assert "payload" in sample_signup_event
    assert "email_domain" in sample_signup_event["payload"]
