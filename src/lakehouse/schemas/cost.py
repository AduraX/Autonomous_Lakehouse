"""Pydantic schemas for Cost Optimization API."""

from datetime import datetime

from pydantic import BaseModel, Field


class CostBaselineResponse(BaseModel):
    """Cost baseline statistics for a single pipeline."""

    model_config = {"from_attributes": True}

    id: int
    pipeline_name: str
    avg_duration: float
    min_duration: float
    max_duration: float
    stddev_duration: float
    total_runs: int
    total_failures: int
    total_successes: int
    total_duration_seconds: float
    last_duration: float | None
    last_run_at: datetime | None
    consecutive_anomalies: int
    failure_rate: float
    avg_cost_per_run: float


class CostFindingResponse(BaseModel):
    """A cost anomaly or optimization opportunity."""

    anomaly_type: str
    pipeline_name: str
    severity: str
    message: str
    details: dict[str, object] = Field(default_factory=dict)
    recommendations: list[str] = Field(default_factory=list)


class CostAnalysisRequest(BaseModel):
    """Request to analyze cost for a specific pipeline event."""

    event_id: int


class CostAnalysisResponse(BaseModel):
    """Cost analysis result for a single event."""

    event_id: int
    pipeline_name: str
    duration_seconds: float
    findings: list[CostFindingResponse]
    actions_proposed: int
    analyzed_at: datetime


class CostSummaryResponse(BaseModel):
    """Overall cost summary across all pipelines."""

    total_pipelines: int
    total_runs: int
    total_duration_hours: float
    top_cost_pipelines: list[CostBaselineResponse]
    high_failure_pipelines: list[CostBaselineResponse]
    anomaly_pipelines: list[CostBaselineResponse]
