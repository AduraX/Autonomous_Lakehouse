"""Pipeline Event model — the canonical representation of a pipeline run."""

from datetime import datetime
from enum import StrEnum

from sqlalchemy import DateTime, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from lakehouse.models.base import Base


class PipelineStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


class Platform(StrEnum):
    ADF = "adf"
    FABRIC = "fabric"
    DATABRICKS = "databricks"
    KUBEFLOW = "kubeflow"
    MOCK = "mock"


class PipelineEvent(Base):
    """A single pipeline run event ingested from a lakehouse platform.

    This is the entry point for the entire system — every agent reacts
    to pipeline events.
    """

    __tablename__ = "pipeline_events"

    # Identity
    external_run_id: Mapped[str] = mapped_column(
        String(255), nullable=False, index=True, comment="Run ID from the source platform"
    )
    pipeline_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    platform: Mapped[Platform] = mapped_column(String(50), nullable=False)

    # Status
    status: Mapped[PipelineStatus] = mapped_column(String(50), nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)

    # Timing
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_seconds: Mapped[float | None] = mapped_column(nullable=True)

    # Context
    metadata_json: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="Arbitrary JSON metadata from the platform"
    )

    __table_args__ = (
        Index("ix_events_platform_status", "platform", "status"),
        Index("ix_events_pipeline_started", "pipeline_name", "started_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<PipelineEvent(id={self.id}, pipeline={self.pipeline_name}, "
            f"status={self.status}, platform={self.platform})>"
        )
