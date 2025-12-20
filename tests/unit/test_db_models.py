import pytest

from shared.db import Base, DLQEvent, Event, ModelVersion, ProcessedEvent, RiskScore


@pytest.mark.unit
class TestEventModel:
    def test_tablename(self):
        assert Event.__tablename__ == "events"

    def test_primary_key(self):
        pk_cols = [col.name for col in Event.__table__.primary_key.columns]
        assert pk_cols == ["event_id"]

    def test_required_columns(self):
        columns = {col.name for col in Event.__table__.columns}
        expected = {
            "event_id",
            "user_id",
            "event_type",
            "ts",
            "schema_version",
            "payload_json",
            "raw_payload_hash",
            "accepted_at",
            "published_at",
        }
        assert expected.issubset(columns)

    def test_user_id_ts_index_exists(self):
        index_names = {idx.name for idx in Event.__table__.indexes}
        assert "ix_events_user_id_ts" in index_names


@pytest.mark.unit
class TestRiskScoreModel:
    def test_tablename(self):
        assert RiskScore.__tablename__ == "risk_scores"

    def test_primary_key(self):
        pk_cols = [col.name for col in RiskScore.__table__.primary_key.columns]
        assert pk_cols == ["id"]

    def test_required_columns(self):
        columns = {col.name for col in RiskScore.__table__.columns}
        expected = {"id", "user_id", "score", "band", "computed_at"}
        assert expected.issubset(columns)

    def test_user_id_computed_at_index_exists(self):
        index_names = {idx.name for idx in RiskScore.__table__.indexes}
        assert "ix_risk_scores_user_id_computed_at" in index_names


@pytest.mark.unit
class TestProcessedEventModel:
    def test_tablename(self):
        assert ProcessedEvent.__tablename__ == "processed_events"

    def test_primary_key(self):
        pk_cols = [col.name for col in ProcessedEvent.__table__.primary_key.columns]
        assert pk_cols == ["event_id"]

    def test_required_columns(self):
        columns = {col.name for col in ProcessedEvent.__table__.columns}
        expected = {"event_id", "processed_at", "status"}
        assert expected.issubset(columns)


@pytest.mark.unit
class TestModelVersionModel:
    def test_tablename(self):
        assert ModelVersion.__tablename__ == "model_versions"

    def test_primary_key(self):
        pk_cols = [col.name for col in ModelVersion.__table__.primary_key.columns]
        assert pk_cols == ["model_version"]

    def test_required_columns(self):
        columns = {col.name for col in ModelVersion.__table__.columns}
        expected = {"model_version", "created_at", "params_hash"}
        assert expected.issubset(columns)


@pytest.mark.unit
class TestDLQEventModel:
    def test_tablename(self):
        assert DLQEvent.__tablename__ == "dlq_events"

    def test_primary_key(self):
        pk_cols = [col.name for col in DLQEvent.__table__.primary_key.columns]
        assert pk_cols == ["id"]

    def test_required_columns(self):
        columns = {col.name for col in DLQEvent.__table__.columns}
        expected = {"id", "raw_payload", "failure_reason", "created_at", "retry_count"}
        assert expected.issubset(columns)


@pytest.mark.unit
class TestBaseModel:
    def test_base_has_naming_convention(self):
        assert "ix" in Base.metadata.naming_convention
        assert "uq" in Base.metadata.naming_convention
        assert "fk" in Base.metadata.naming_convention
        assert "pk" in Base.metadata.naming_convention

    def test_all_models_registered(self):
        table_names = set(Base.metadata.tables.keys())
        expected = {"events", "risk_scores", "processed_events", "model_versions", "dlq_events"}
        assert expected == table_names
