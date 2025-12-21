"""Initial schema with events, risk_scores, processed_events, model_versions, dlq_events tables

Revision ID: 001
Revises:
Create Date: 2024-01-15

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "events",
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.String(length=255), nullable=False),
        sa.Column("event_type", sa.String(length=50), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("raw_payload_hash", sa.String(length=64), nullable=False),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("event_id", name=op.f("pk_events")),
    )
    op.create_index("ix_events_user_id_ts", "events", ["user_id", "ts"], unique=False)

    op.create_table(
        "risk_scores",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.String(length=255), nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("band", sa.String(length=10), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("top_features_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("model_version", sa.String(length=50), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_risk_scores")),
    )
    op.create_index(
        "ix_risk_scores_user_id_computed_at",
        "risk_scores",
        ["user_id", "computed_at"],
        unique=False,
    )

    op.create_table(
        "processed_events",
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.PrimaryKeyConstraint("event_id", name=op.f("pk_processed_events")),
    )

    op.create_table(
        "model_versions",
        sa.Column("model_version", sa.String(length=50), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("params_hash", sa.String(length=64), nullable=False),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("model_version", name=op.f("pk_model_versions")),
    )

    op.create_table(
        "dlq_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=True),
        sa.Column("raw_payload", sa.Text(), nullable=False),
        sa.Column("failure_reason", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_dlq_events")),
    )


def downgrade() -> None:
    op.drop_table("dlq_events")
    op.drop_table("model_versions")
    op.drop_table("processed_events")
    op.drop_index("ix_risk_scores_user_id_computed_at", table_name="risk_scores")
    op.drop_table("risk_scores")
    op.drop_index("ix_events_user_id_ts", table_name="events")
    op.drop_table("events")
