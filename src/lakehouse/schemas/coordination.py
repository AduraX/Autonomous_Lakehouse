"""Pydantic schemas for the Coordinator Agent."""

from datetime import datetime

from pydantic import BaseModel, Field

from lakehouse.models.actions import ActionType
from lakehouse.models.diagnoses import FailureCategory
from lakehouse.policies import PolicyClassification


class CoordinationRequest(BaseModel):
    """Request to coordinate diagnosis and remediation for a pipeline event."""

    event_id: int


class AgentFinding(BaseModel):
    """Result from a specialist agent invoked by the coordinator."""

    agent_name: str = Field(..., examples=["failure_diagnosis"])
    category: FailureCategory
    confidence: float = Field(..., ge=0.0, le=1.0, examples=[0.91])
    summary: str
    details: dict[str, object] = Field(default_factory=dict)


class ProposedAction(BaseModel):
    """An action the coordinator proposes based on agent findings."""

    action_type: ActionType
    description: str
    policy: PolicyClassification


class CoordinationResult(BaseModel):
    """Aggregate output from the coordinator agent."""

    event_id: int
    findings: list[AgentFinding]
    proposed_actions: list[ProposedAction]
    summary: str
    coordinated_at: datetime
