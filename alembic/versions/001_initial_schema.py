"""Initial schema — all core tables.

Revision ID: 001_initial
Revises:
Create Date: 2026-04-29

Creates:
- pipeline_events
- diagnoses
- actions
- audit_log
- cost_baselines
"""

from alembic import op
import sqlalchemy as sa

revision = "001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- pipeline_events ---
    op.create_table(
        "pipeline_events",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("external_run_id", sa.String(255), nullable=False),
        sa.Column("pipeline_name", sa.String(255), nullable=False),
        sa.Column("platform", sa.String(50), nullable=False),
        sa.Column("status", sa.String(50), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("error_code", sa.String(100), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_seconds", sa.Float(), nullable=True),
        sa.Column("metadata_json", sa.Text(), nullable=True),
    )
    op.create_index("ix_pipeline_events_external_run_id", "pipeline_events", ["external_run_id"])
    op.create_index("ix_pipeline_events_pipeline_name", "pipeline_events", ["pipeline_name"])
    op.create_index("ix_events_platform_status", "pipeline_events", ["platform", "status"])
    op.create_index("ix_events_pipeline_started", "pipeline_events", ["pipeline_name", "started_at"])

    # --- diagnoses ---
    op.create_table(
        "diagnoses",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("event_id", sa.Integer(), sa.ForeignKey("pipeline_events.id", ondelete="CASCADE"), nullable=False),
        sa.Column("category", sa.String(50), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("details_json", sa.Text(), nullable=True),
        sa.Column("agent_name", sa.String(100), nullable=False),
    )
    op.create_index("ix_diagnoses_event_id", "diagnoses", ["event_id"])

    # --- actions ---
    op.create_table(
        "actions",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("diagnosis_id", sa.Integer(), sa.ForeignKey("diagnoses.id", ondelete="CASCADE"), nullable=False),
        sa.Column("action_type", sa.String(50), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("parameters_json", sa.Text(), nullable=True),
        sa.Column("status", sa.String(50), nullable=False, server_default="proposed"),
        sa.Column("result_json", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("approved_by", sa.String(255), nullable=True),
        sa.Column("executed_by", sa.String(100), nullable=False, server_default="system"),
    )
    op.create_index("ix_actions_diagnosis_id", "actions", ["diagnosis_id"])

    # --- audit_log ---
    op.create_table(
        "audit_log",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("actor", sa.String(255), nullable=False),
        sa.Column("resource_type", sa.String(100), nullable=False),
        sa.Column("resource_id", sa.Integer(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("details_json", sa.Text(), nullable=True),
        sa.Column("correlation_id", sa.String(255), nullable=True),
    )
    op.create_index("ix_audit_log_event_type", "audit_log", ["event_type"])
    op.create_index("ix_audit_log_resource_id", "audit_log", ["resource_id"])
    op.create_index("ix_audit_log_correlation_id", "audit_log", ["correlation_id"])

    # --- cost_baselines ---
    op.create_table(
        "cost_baselines",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("pipeline_name", sa.String(255), nullable=False, unique=True),
        sa.Column("avg_duration", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("min_duration", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("max_duration", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("stddev_duration", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("total_runs", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_failures", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_successes", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_duration_seconds", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("last_duration", sa.Float(), nullable=True),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("consecutive_anomalies", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_index("ix_cost_baselines_pipeline_name", "cost_baselines", ["pipeline_name"])
    op.create_index("ix_baselines_last_run", "cost_baselines", ["last_run_at"])


def downgrade() -> None:
    op.drop_table("cost_baselines")
    op.drop_table("audit_log")
    op.drop_table("actions")
    op.drop_table("diagnoses")
    op.drop_table("pipeline_events")
