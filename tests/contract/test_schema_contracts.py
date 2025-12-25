import json
from pathlib import Path

import pytest

from shared.features import FEATURE_DEFAULTS, FEATURE_ORDER
from shared.schemas import (
    LoginEvent,
    LoginPayload,
    SignupEvent,
    SignupPayload,
    TransactionEvent,
    TransactionPayload,
)

SCHEMAS_DIR = Path(__file__).parent.parent.parent / "shared" / "schemas"


def load_schema_snapshot(name: str) -> dict:
    path = SCHEMAS_DIR / f"{name}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


def save_schema_snapshot(name: str, schema: dict) -> None:
    path = SCHEMAS_DIR / f"{name}.json"
    path.write_text(json.dumps(schema, indent=2, sort_keys=True) + "\n")


@pytest.mark.contract
class TestSignupPayloadContract:
    def test_schema_matches_snapshot(self):
        schema = SignupPayload.model_json_schema()
        snapshot = load_schema_snapshot("signup_payload")

        if snapshot is None:
            save_schema_snapshot("signup_payload", schema)
            pytest.skip("Snapshot created, re-run test to verify")

        assert schema == snapshot, (
            "SignupPayload schema has changed. "
            "If intentional, delete shared/schemas/signup_payload.json and re-run."
        )

    def test_required_fields(self):
        schema = SignupPayload.model_json_schema()
        assert "email_domain" in schema["required"]
        assert "country" in schema["required"]
        assert "device_id" in schema["required"]


@pytest.mark.contract
class TestLoginPayloadContract:
    def test_schema_matches_snapshot(self):
        schema = LoginPayload.model_json_schema()
        snapshot = load_schema_snapshot("login_payload")

        if snapshot is None:
            save_schema_snapshot("login_payload", schema)
            pytest.skip("Snapshot created, re-run test to verify")

        assert schema == snapshot, (
            "LoginPayload schema has changed. "
            "If intentional, delete shared/schemas/login_payload.json and re-run."
        )

    def test_required_fields(self):
        schema = LoginPayload.model_json_schema()
        assert "ip" in schema["required"]
        assert "success" in schema["required"]
        assert "device_id" in schema["required"]


@pytest.mark.contract
class TestTransactionPayloadContract:
    def test_schema_matches_snapshot(self):
        schema = TransactionPayload.model_json_schema()
        snapshot = load_schema_snapshot("transaction_payload")

        if snapshot is None:
            save_schema_snapshot("transaction_payload", schema)
            pytest.skip("Snapshot created, re-run test to verify")

        assert schema == snapshot, (
            "TransactionPayload schema has changed. "
            "If intentional, delete shared/schemas/transaction_payload.json and re-run."
        )

    def test_required_fields(self):
        schema = TransactionPayload.model_json_schema()
        assert "amount" in schema["required"]
        assert "currency" in schema["required"]
        assert "merchant" in schema["required"]
        assert "country" in schema["required"]


@pytest.mark.contract
class TestSignupEventContract:
    def test_schema_matches_snapshot(self):
        schema = SignupEvent.model_json_schema()
        snapshot = load_schema_snapshot("signup_event")

        if snapshot is None:
            save_schema_snapshot("signup_event", schema)
            pytest.skip("Snapshot created, re-run test to verify")

        assert schema == snapshot, (
            "SignupEvent schema has changed. "
            "If intentional, delete shared/schemas/signup_event.json and re-run."
        )


@pytest.mark.contract
class TestLoginEventContract:
    def test_schema_matches_snapshot(self):
        schema = LoginEvent.model_json_schema()
        snapshot = load_schema_snapshot("login_event")

        if snapshot is None:
            save_schema_snapshot("login_event", schema)
            pytest.skip("Snapshot created, re-run test to verify")

        assert schema == snapshot, (
            "LoginEvent schema has changed. "
            "If intentional, delete shared/schemas/login_event.json and re-run."
        )


@pytest.mark.contract
class TestTransactionEventContract:
    def test_schema_matches_snapshot(self):
        schema = TransactionEvent.model_json_schema()
        snapshot = load_schema_snapshot("transaction_event")

        if snapshot is None:
            save_schema_snapshot("transaction_event", schema)
            pytest.skip("Snapshot created, re-run test to verify")

        assert schema == snapshot, (
            "TransactionEvent schema has changed. "
            "If intentional, delete shared/schemas/transaction_event.json and re-run."
        )


@pytest.mark.contract
class TestFeatureOrderContract:
    def test_feature_order_matches_model_metadata(self):
        model_metadata_path = Path(__file__).parent.parent.parent / "models" / "metadata.json"

        if not model_metadata_path.exists():
            pytest.skip("Model metadata not found (run training first)")

        with open(model_metadata_path) as f:
            metadata = json.load(f)

        model_feature_order = metadata["feature_order"]
        assert model_feature_order == FEATURE_ORDER, (
            f"Feature order mismatch between model metadata and shared/features.py.\n"
            f"Model: {model_feature_order}\n"
            f"Code: {FEATURE_ORDER}\n"
            f"If intentional, re-train the model or update FEATURE_ORDER."
        )

    def test_feature_defaults_match_model_metadata(self):
        model_metadata_path = Path(__file__).parent.parent.parent / "models" / "metadata.json"

        if not model_metadata_path.exists():
            pytest.skip("Model metadata not found (run training first)")

        with open(model_metadata_path) as f:
            metadata = json.load(f)

        model_defaults = metadata["feature_defaults"]
        for feature in FEATURE_ORDER:
            assert feature in model_defaults, f"Feature {feature} missing from model defaults"
            assert feature in FEATURE_DEFAULTS, f"Feature {feature} missing from code defaults"

    def test_feature_order_length_consistent(self):
        assert len(FEATURE_ORDER) == len(FEATURE_DEFAULTS), (
            f"FEATURE_ORDER has {len(FEATURE_ORDER)} items, "
            f"but FEATURE_DEFAULTS has {len(FEATURE_DEFAULTS)} items"
        )

    def test_all_features_have_defaults(self):
        for feature in FEATURE_ORDER:
            assert feature in FEATURE_DEFAULTS, (
                f"Feature '{feature}' is in FEATURE_ORDER but not in FEATURE_DEFAULTS"
            )
