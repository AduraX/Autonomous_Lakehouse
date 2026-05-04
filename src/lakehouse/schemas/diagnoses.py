"""Pydantic schemas for Diagnoses."""

from datetime import datetime

from pydantic import BaseModel, Field

from lakehouse.models.diagnoses import FailureCategory


class DiagnosisCreate(BaseModel):
    """Schema for creating a new diagnosis."""

    event_id: int
    category: FailureCategory
    confidence: float = Field(..., ge=0.0, le=1.0, examples=[0.92])
    summary: str = Field(..., examples=["Source schema changed: column 'amount' type mismatch"])
    details_json: str | None = None
    agent_name: str = Field(..., max_length=100, examples=["pipeline_recovery_agent"])


class DiagnosisResponse(BaseModel):
    """Schema returned when reading a diagnosis."""

    model_config = {"from_attributes": True}

    id: int
    event_id: int
    category: FailureCategory
    confidence: float
    summary: str
    details_json: str | None
    agent_name: str
    created_at: datetime
    updated_at: datetime
