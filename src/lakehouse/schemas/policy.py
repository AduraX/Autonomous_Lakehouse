"""Pydantic schemas for Adaptive Policy Learning API."""

from pydantic import BaseModel, Field


class ActionStatsResponse(BaseModel):
    """Approval/rejection statistics for an (action_type, failure_category) pair."""

    action_type: str
    failure_category: str
    total: int
    approved: int
    rejected: int
    succeeded: int
    failed: int
    approval_rate: float


class PolicySuggestionResponse(BaseModel):
    """A suggested policy change based on historical action outcomes."""

    action_type: str
    failure_category: str
    current_policy: str
    suggested_policy: str
    confidence: float
    reason: str
    stats: ActionStatsResponse


class PolicyAnalysisResponse(BaseModel):
    """Full policy analysis with suggestions and summary."""

    suggestions: list[PolicySuggestionResponse] = Field(default_factory=list)
    total_groups_analyzed: int
    suggestions_count: int
    summary: str


class PolicyConfigEntry(BaseModel):
    """A single entry in the current policy configuration."""

    action_type: str
    policy: str


class PolicyConfigResponse(BaseModel):
    """Current policy configuration for all action types."""

    policies: list[PolicyConfigEntry]
    auto_approved: list[str]
    requires_approval: list[str]
    forbidden: list[str]
