"""Pydantic schemas for Audit Log."""

from datetime import datetime

from pydantic import BaseModel, Field


class AuditLogCreate(BaseModel):
    """Schema for creating an audit log entry."""

    event_type: str = Field(..., max_length=100, examples=["event_ingested"])
    actor: str = Field(..., max_length=255, examples=["pipeline_recovery_agent"])
    resource_type: str = Field(..., max_length=100, examples=["pipeline_event"])
    resource_id: int
    summary: str
    details_json: str | None = None
    correlation_id: str | None = Field(None, max_length=255)


class AuditLogResponse(BaseModel):
    """Schema returned when reading an audit log entry."""

    model_config = {"from_attributes": True}

    id: int
    event_type: str
    actor: str
    resource_type: str
    resource_id: int
    summary: str
    details_json: str | None
    correlation_id: str | None
    created_at: datetime
