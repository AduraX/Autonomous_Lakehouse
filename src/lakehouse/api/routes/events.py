"""Pipeline Event API routes — ingestion and retrieval."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from lakehouse.api.dependencies import get_db
from lakehouse.logging import get_logger
from lakehouse.models.events import PipelineEvent, PipelineStatus
from lakehouse.schemas.events import (
    PipelineEventCreate,
    PipelineEventList,
    PipelineEventResponse,
)

router = APIRouter()
logger = get_logger(__name__)


@router.post("/events", response_model=PipelineEventResponse, status_code=201)
async def ingest_event(
    payload: PipelineEventCreate,
    session: AsyncSession = Depends(get_db),
) -> PipelineEvent:
    """Ingest a pipeline event from a lakehouse platform.

    This is the main entry point — every pipeline run (success or failure)
    should be reported here.
    """
    event = PipelineEvent(**payload.model_dump())
    session.add(event)
    await session.flush()
    await session.refresh(event)

    logger.info(
        "event_ingested",
        event_id=event.id,
        pipeline=event.pipeline_name,
        platform=event.platform,
        status=event.status,
    )
    return event


@router.get("/events/{event_id}", response_model=PipelineEventResponse)
async def get_event(
    event_id: int,
    session: AsyncSession = Depends(get_db),
) -> PipelineEvent:
    """Retrieve a single pipeline event by ID."""
    event = await session.get(PipelineEvent, event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    return event


@router.get("/events", response_model=PipelineEventList)
async def list_events(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    status: PipelineStatus | None = None,
    pipeline_name: str | None = None,
    session: AsyncSession = Depends(get_db),
) -> dict[str, object]:
    """List pipeline events with filtering and pagination."""
    query = select(PipelineEvent).order_by(PipelineEvent.created_at.desc())
    count_query = select(func.count(PipelineEvent.id))

    if status:
        query = query.where(PipelineEvent.status == status)
        count_query = count_query.where(PipelineEvent.status == status)
    if pipeline_name:
        query = query.where(PipelineEvent.pipeline_name.ilike(f"%{pipeline_name}%"))
        count_query = count_query.where(PipelineEvent.pipeline_name.ilike(f"%{pipeline_name}%"))

    total = (await session.execute(count_query)).scalar_one()
    offset = (page - 1) * page_size
    result = await session.execute(query.offset(offset).limit(page_size))
    items = list(result.scalars().all())

    return {"items": items, "total": total, "page": page, "page_size": page_size}
