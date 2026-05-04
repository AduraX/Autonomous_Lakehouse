"""Coordination API route — trigger the coordinator agent for a pipeline event."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from lakehouse.adapters import get_adapter
from lakehouse.agents.coordinator import CoordinatorAgent
from lakehouse.api.dependencies import get_db
from lakehouse.logging import get_logger
from lakehouse.schemas.coordination import CoordinationRequest, CoordinationResult

router = APIRouter()
logger = get_logger(__name__)


@router.post("/coordinate", response_model=CoordinationResult)
async def coordinate_event(
    payload: CoordinationRequest,
    session: AsyncSession = Depends(get_db),
) -> CoordinationResult:
    """Trigger the coordinator agent for a failed pipeline event."""
    adapter = get_adapter()
    coordinator = CoordinatorAgent(adapter=adapter, session=session)

    try:
        result = await coordinator.coordinate(payload.event_id)
    except ValueError as err:
        raise HTTPException(status_code=404, detail=str(err)) from err

    logger.info(
        "coordination_requested",
        event_id=payload.event_id,
        findings=len(result.findings),
        actions=len(result.proposed_actions),
    )
    return result
