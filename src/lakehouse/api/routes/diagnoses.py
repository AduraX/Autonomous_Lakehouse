"""Diagnosis API routes."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from lakehouse.api.dependencies import get_db
from lakehouse.logging import get_logger
from lakehouse.models.diagnoses import Diagnosis
from lakehouse.schemas.diagnoses import DiagnosisCreate, DiagnosisResponse

router = APIRouter()
logger = get_logger(__name__)


@router.post("/diagnoses", response_model=DiagnosisResponse, status_code=201)
async def create_diagnosis(
    payload: DiagnosisCreate,
    session: AsyncSession = Depends(get_db),
) -> Diagnosis:
    """Record a root-cause diagnosis for a pipeline event."""
    diagnosis = Diagnosis(**payload.model_dump())
    session.add(diagnosis)
    await session.flush()
    await session.refresh(diagnosis)

    logger.info(
        "diagnosis_created",
        diagnosis_id=diagnosis.id,
        event_id=diagnosis.event_id,
        category=diagnosis.category,
        confidence=diagnosis.confidence,
    )
    return diagnosis


@router.get("/diagnoses/{diagnosis_id}", response_model=DiagnosisResponse)
async def get_diagnosis(
    diagnosis_id: int,
    session: AsyncSession = Depends(get_db),
) -> Diagnosis:
    """Retrieve a diagnosis by ID."""
    diagnosis = await session.get(Diagnosis, diagnosis_id)
    if not diagnosis:
        raise HTTPException(status_code=404, detail="Diagnosis not found")
    return diagnosis


@router.get("/events/{event_id}/diagnoses", response_model=list[DiagnosisResponse])
async def list_event_diagnoses(
    event_id: int,
    session: AsyncSession = Depends(get_db),
) -> list[Diagnosis]:
    """List all diagnoses for a specific pipeline event."""
    result = await session.execute(
        select(Diagnosis)
        .where(Diagnosis.event_id == event_id)
        .order_by(Diagnosis.confidence.desc())
    )
    return list(result.scalars().all())
