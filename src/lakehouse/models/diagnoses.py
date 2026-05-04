"""Diagnosis model — root-cause classification for pipeline failures."""

from enum import StrEnum

from sqlalchemy import ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from lakehouse.models.base import Base


class FailureCategory(StrEnum):
    SCHEMA_MISMATCH = "schema_mismatch"
    SOURCE_UNAVAILABLE = "source_unavailable"
    SINK_UNAVAILABLE = "sink_unavailable"
    TIMEOUT = "timeout"
    PERMISSION_DENIED = "permission_denied"
    DATA_QUALITY = "data_quality"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    CONFIGURATION_ERROR = "configuration_error"
    DEPENDENCY_FAILURE = "dependency_failure"
    UNKNOWN = "unknown"


class Diagnosis(Base):
    """Root-cause diagnosis produced by an agent for a failed pipeline event."""

    __tablename__ = "diagnoses"

    # Link to the event
    event_id: Mapped[int] = mapped_column(
        ForeignKey("pipeline_events.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Classification
    category: Mapped[FailureCategory] = mapped_column(String(50), nullable=False)
    confidence: Mapped[float] = mapped_column(nullable=False, comment="0.0 to 1.0 confidence score")
    summary: Mapped[str] = mapped_column(
        Text, nullable=False, comment="Human-readable diagnosis summary"
    )
    details_json: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="Structured diagnostic details as JSON"
    )

    # Agent that produced this diagnosis
    agent_name: Mapped[str] = mapped_column(
        String(100), nullable=False, comment="Name of the agent that produced this diagnosis"
    )

    # Relationship
    event: Mapped["PipelineEvent"] = relationship(lazy="selectin")  # type: ignore[name-defined]  # noqa: F821

    def __repr__(self) -> str:
        return (
            f"<Diagnosis(id={self.id}, event_id={self.event_id}, "
            f"category={self.category}, confidence={self.confidence})>"
        )
