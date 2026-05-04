"""Pydantic schemas for Pipeline Events."""

from datetime import datetime

from pydantic import BaseModel, Field

from lakehouse.models.events import PipelineStatus, Platform


class PipelineEventCreate(BaseModel):
    """Schema for ingesting a new pipeline event."""

    external_run_id: str = Field(..., max_length=255, examples=["adf-run-abc123"])
    pipeline_name: str = Field(..., max_length=255, examples=["ingest_sales_data"])
    platform: Platform = Field(..., examples=[Platform.ADF])
    status: PipelineStatus = Field(..., examples=[PipelineStatus.FAILED])
    error_message: str | None = Field(None, examples=["Schema mismatch on column 'amount'"])
    error_code: str | None = Field(None, max_length=100, examples=["SCHEMA_ERROR_4012"])
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_seconds: float | None = Field(None, ge=0)
    metadata_json: str | None = None


class PipelineEventResponse(BaseModel):
    """Schema returned when reading a pipeline event."""

    model_config = {"from_attributes": True}

    id: int
    external_run_id: str
    pipeline_name: str
    platform: Platform
    status: PipelineStatus
    error_message: str | None
    error_code: str | None
    started_at: datetime | None
    finished_at: datetime | None
    duration_seconds: float | None
    metadata_json: str | None
    created_at: datetime
    updated_at: datetime


class PipelineEventList(BaseModel):
    """Paginated list of pipeline events."""

    items: list[PipelineEventResponse]
    total: int
    page: int
    page_size: int
