"""Cost Optimization API routes — baselines, anomaly detection, recommendations."""

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from lakehouse.agents.cost_optimizer import CostOptimizer
from lakehouse.api.dependencies import get_db
from lakehouse.logging import get_logger
from lakehouse.models.events import PipelineEvent
from lakehouse.schemas.cost import (
    CostAnalysisRequest,
    CostAnalysisResponse,
    CostBaselineResponse,
    CostFindingResponse,
    CostSummaryResponse,
)

router = APIRouter()
logger = get_logger(__name__)


@router.get("/cost/baselines", response_model=list[CostBaselineResponse])
async def list_baselines(
    session: AsyncSession = Depends(get_db),
) -> list[CostBaselineResponse]:
    """List all pipeline cost baselines, ordered by total cost."""
    optimizer = CostOptimizer(session=session)
    return await optimizer.get_all_baselines()  # type: ignore[return-value]


@router.get("/cost/baselines/{pipeline_name}", response_model=CostBaselineResponse)
async def get_baseline(
    pipeline_name: str,
    session: AsyncSession = Depends(get_db),
) -> object:
    """Get the cost baseline for a specific pipeline."""
    optimizer = CostOptimizer(session=session)
    baseline = await optimizer.get_baseline(pipeline_name)
    if not baseline:
        raise HTTPException(status_code=404, detail=f"No baseline found for '{pipeline_name}'")
    return baseline


@router.post("/cost/analyze", response_model=CostAnalysisResponse)
async def analyze_event(
    payload: CostAnalysisRequest,
    session: AsyncSession = Depends(get_db),
) -> CostAnalysisResponse:
    """Analyze a pipeline event for cost anomalies.

    Updates the baseline and checks for duration spikes and high failure rates.
    Anomalies auto-create diagnoses and proposed actions.
    """
    event = await session.get(PipelineEvent, payload.event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")

    optimizer = CostOptimizer(session=session)
    findings, actions_proposed = await optimizer.analyze_and_coordinate(event)

    return CostAnalysisResponse(
        event_id=event.id,
        pipeline_name=event.pipeline_name,
        duration_seconds=event.duration_seconds or 0.0,
        findings=[
            CostFindingResponse(
                anomaly_type=f.anomaly_type.value,
                pipeline_name=f.pipeline_name,
                severity=f.severity,
                message=f.message,
                details=f.details,
                recommendations=f.recommendations,
            )
            for f in findings
        ],
        actions_proposed=actions_proposed,
        analyzed_at=datetime.now(UTC),
    )


@router.get("/cost/summary", response_model=CostSummaryResponse)
async def cost_summary(
    top_n: int = Query(10, ge=1, le=50),
    session: AsyncSession = Depends(get_db),
) -> CostSummaryResponse:
    """Get an overall cost summary across all pipelines."""
    optimizer = CostOptimizer(session=session)
    all_baselines = await optimizer.get_all_baselines()

    total_runs = sum(b.total_runs for b in all_baselines)
    total_duration = sum(b.total_duration_seconds for b in all_baselines)

    # Top cost pipelines
    top_cost = all_baselines[:top_n]

    # High failure rate pipelines (>30%)
    high_failure = [b for b in all_baselines if b.failure_rate > 0.3]

    # Pipelines with active anomalies
    anomaly = [b for b in all_baselines if b.consecutive_anomalies > 0]

    return CostSummaryResponse(
        total_pipelines=len(all_baselines),
        total_runs=total_runs,
        total_duration_hours=round(total_duration / 3600, 2),
        top_cost_pipelines=top_cost,
        high_failure_pipelines=high_failure,
        anomaly_pipelines=anomaly,
    )
