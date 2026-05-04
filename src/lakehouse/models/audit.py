"""Audit log model — immutable record of every system decision."""

from sqlalchemy import String, Text
from sqlalchemy.orm import Mapped, mapped_column

from lakehouse.models.base import Base


class AuditLog(Base):
    """Immutable audit trail entry.

    Every significant system action (diagnosis, action proposal, execution,
    approval, rejection) is logged here for compliance and debugging.
    """

    __tablename__ = "audit_log"

    # What happened
    event_type: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        index=True,
        comment="e.g. event_ingested, diagnosis_created, action_proposed, action_executed",
    )

    # Who did it
    actor: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Agent name, user email, or 'system'",
    )

    # What it relates to
    resource_type: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        comment="e.g. pipeline_event, diagnosis, action",
    )
    resource_id: Mapped[int] = mapped_column(
        nullable=False,
        index=True,
    )

    # Details
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    details_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Correlation
    correlation_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        index=True,
        comment="Groups related audit entries across a single workflow",
    )

    def __repr__(self) -> str:
        return (
            f"<AuditLog(id={self.id}, event_type={self.event_type}, "
            f"resource={self.resource_type}:{self.resource_id})>"
        )
