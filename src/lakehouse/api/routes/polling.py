"""Event polling API route — fetch events from the platform and coordinate failures."""

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from lakehouse.adapters import get_adapter
from lakehouse.agents.coordinator import CoordinatorAgent
from lakehouse.agents.cost_optimizer import CostOptimizer
from lakehouse.api.dependencies import get_db
from lakehouse.logging import get_logger
from lakehouse.models.events import PipelineEvent, PipelineStatus

router = APIRouter()
logger = get_logger(__name__)


@router.post("/poll")
async def poll_and_coordinate(
    limit: int = Query(10, ge=1, le=100),
    session: AsyncSession = Depends(get_db),
) -> dict[str, object]:
    """Poll the platform adapter for recent events, ingest, and coordinate failures.

    This is the synchronous version — for production use, the arq worker's
    poll_events task handles this on a schedule.
    """
    adapter = get_adapter()

    # Fetch events from platform
    events_data = await adapter.fetch_recent_events(limit=limit)

    ingested = 0
    coordinated = 0
    cost_anomalies = 0
    coordination_results = []

    coordinator = CoordinatorAgent(adapter=adapter, session=session)
    cost_optimizer = CostOptimizer(session=session)

    for event_data in events_data:
        # Ingest
        event = PipelineEvent(**event_data.model_dump())
        session.add(event)
        await session.flush()
        await session.refresh(event)
        ingested += 1

        # Update cost baseline for every completed event
        status_val = (
            event.status.value if isinstance(event.status, PipelineStatus) else event.status
        )
        if status_val != PipelineStatus.RUNNING.value and event.duration_seconds:
            findings, _ = await cost_optimizer.analyze_and_coordinate(event)
            cost_anomalies += len(findings)

        # Coordinate failures
        if status_val in (PipelineStatus.FAILED.value, PipelineStatus.TIMED_OUT.value):
            result = await coordinator.coordinate(event.id)
            coordinated += 1
            coordination_results.append(
                {
                    "event_id": event.id,
                    "pipeline": event.pipeline_name,
                    "category": result.findings[0].category.value if result.findings else "none",
                    "actions": len(result.proposed_actions),
                }
            )

    logger.info(
        "poll_completed",
        ingested=ingested,
        coordinated=coordinated,
        cost_anomalies=cost_anomalies,
    )

    return {
        "ingested": ingested,
        "coordinated": coordinated,
        "cost_anomalies": cost_anomalies,
        "polled_at": datetime.now(UTC).isoformat(),
        "coordination_results": coordination_results,
    }
