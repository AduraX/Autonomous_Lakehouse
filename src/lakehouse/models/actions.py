"""Action model — proposed or executed remediation actions."""

from enum import StrEnum

from sqlalchemy import ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from lakehouse.models.base import Base


class ActionType(StrEnum):
    RETRY_PIPELINE = "retry_pipeline"
    BLOCK_DOWNSTREAM = "block_downstream"
    SCALE_COMPUTE = "scale_compute"
    REFRESH_CREDENTIALS = "refresh_credentials"
    NOTIFY_OWNER = "notify_owner"
    REQUEST_APPROVAL = "request_approval"
    ADJUST_SCHEDULE = "adjust_schedule"
    CUSTOM = "custom"


class ActionStatus(StrEnum):
    PROPOSED = "proposed"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXECUTING = "executing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class Action(Base):
    """A remediation action proposed or executed in response to a diagnosis."""

    __tablename__ = "actions"

    # Link to diagnosis
    diagnosis_id: Mapped[int] = mapped_column(
        ForeignKey("diagnoses.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Action definition
    action_type: Mapped[ActionType] = mapped_column(String(50), nullable=False)
    description: Mapped[str] = mapped_column(
        Text, nullable=False, comment="Human-readable description of the action"
    )
    parameters_json: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="Action parameters as JSON"
    )

    # Execution state
    status: Mapped[ActionStatus] = mapped_column(
        String(50), nullable=False, default=ActionStatus.PROPOSED
    )
    result_json: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="Execution result as JSON"
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Who/what approved or executed
    approved_by: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="User or system that approved"
    )
    executed_by: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        default="system",
        comment="Agent or user that executed the action",
    )

    # Relationship
    diagnosis: Mapped["Diagnosis"] = relationship(lazy="selectin")  # type: ignore[name-defined]  # noqa: F821

    def __repr__(self) -> str:
        return f"<Action(id={self.id}, type={self.action_type}, status={self.status})>"
