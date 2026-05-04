"""Pydantic schemas for Data Quality API."""

from datetime import datetime

from pydantic import BaseModel, Field


class QualityCheckRequest(BaseModel):
    """Request to run quality checks on a single Iceberg table."""

    namespace: str = Field(..., examples=["lakehouse"])
    table: str = Field(..., examples=["sales"])
    expected_columns: list[str] | None = Field(
        None,
        examples=[["id", "name", "amount"]],
        description="If provided, schema drift is checked against these columns",
    )
    max_age_hours: float = Field(
        24.0,
        ge=0.1,
        description="Maximum hours since last snapshot before freshness fails",
    )


class QualityScanRequest(BaseModel):
    """Request to scan all tables in a namespace."""

    namespace: str = Field(..., examples=["lakehouse"])
    max_age_hours: float = Field(24.0, ge=0.1)


class QualityCheckResultResponse(BaseModel):
    """Result of a single quality check."""

    check_name: str
    status: str
    table: str
    message: str
    details: dict[str, object] = Field(default_factory=dict)
    timestamp: datetime


class QualityTableReport(BaseModel):
    """Quality report for a single table — all checks."""

    table: str
    checks: list[QualityCheckResultResponse]
    passed: int
    warnings: int
    failed: int
    errors: int


class QualityScanResponse(BaseModel):
    """Quality report for an entire namespace scan."""

    namespace: str
    tables_scanned: int
    total_checks: int
    total_passed: int
    total_failed: int
    tables: list[QualityTableReport]
    actions_proposed: int = 0
    scanned_at: datetime


class QualityCheckResponse(BaseModel):
    """Response for a single-table quality check."""

    table: str
    checks: list[QualityCheckResultResponse]
    passed: int
    warnings: int
    failed: int
    errors: int
    actions_proposed: int = 0
    checked_at: datetime
