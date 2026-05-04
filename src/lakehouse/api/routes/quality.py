"""Data Quality API routes — run Iceberg quality checks and coordinate failures."""

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from lakehouse.agents.iceberg_quality import CheckStatus, IcebergQualityAgent
from lakehouse.agents.quality_coordinator import (
    run_quality_checks_and_coordinate,
    scan_namespace_and_coordinate,
)
from lakehouse.api.dependencies import get_db
from lakehouse.config import get_settings
from lakehouse.logging import get_logger
from lakehouse.schemas.quality import (
    QualityCheckRequest,
    QualityCheckResponse,
    QualityCheckResultResponse,
    QualityScanRequest,
    QualityScanResponse,
    QualityTableReport,
)

router = APIRouter()
logger = get_logger(__name__)


def _get_agent() -> IcebergQualityAgent:
    """Get an IcebergQualityAgent, checking that Iceberg is configured."""
    settings = get_settings()
    if not settings.iceberg_rest_catalog_url:
        raise HTTPException(
            status_code=503,
            detail="Iceberg REST Catalog not configured (set ICEBERG_REST_CATALOG_URL)",
        )
    return IcebergQualityAgent()


def _to_response(r: Any) -> QualityCheckResultResponse:
    """Convert a dataclass QualityCheckResult to a Pydantic response."""
    return QualityCheckResultResponse(
        check_name=r.check_name,
        status=r.status.value,
        table=r.table,
        message=r.message,
        details=r.details,
        timestamp=r.timestamp,
    )


@router.get("/quality/namespaces")
async def list_namespaces() -> dict[str, list[str]]:
    """List all namespaces in the Iceberg catalog."""
    agent = _get_agent()
    try:
        namespaces = await agent.list_namespaces()
    except Exception as err:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to reach Iceberg catalog: {err}",
        ) from err
    return {"namespaces": namespaces}


@router.get("/quality/namespaces/{namespace}/tables")
async def list_tables(namespace: str) -> dict[str, str | list[str]]:
    """List all tables in a namespace."""
    agent = _get_agent()
    try:
        tables = await agent.list_tables(namespace)
    except Exception as err:
        raise HTTPException(status_code=502, detail=f"Failed to list tables: {err}") from err
    return {"namespace": namespace, "tables": tables}


@router.post("/quality/check", response_model=QualityCheckResponse)
async def check_table(
    payload: QualityCheckRequest,
    session: AsyncSession = Depends(get_db),
) -> QualityCheckResponse:
    """Run quality checks on a single Iceberg table.

    If any checks fail, automatically creates a pipeline event,
    diagnosis, and proposed actions in the remediation pipeline.
    """
    agent = _get_agent()

    try:
        results, actions_proposed = await run_quality_checks_and_coordinate(
            agent,
            session,
            namespace=payload.namespace,
            table=payload.table,
            expected_columns=payload.expected_columns,
            max_age_hours=payload.max_age_hours,
        )
    except Exception as e:
        logger.error(
            "quality_check_error", table=f"{payload.namespace}.{payload.table}", error=str(e)
        )
        raise HTTPException(status_code=502, detail=f"Quality check failed: {e}") from e

    check_responses = [_to_response(r) for r in results]
    full_table = f"{payload.namespace}.{payload.table}"

    return QualityCheckResponse(
        table=full_table,
        checks=check_responses,
        passed=sum(1 for r in results if r.status == CheckStatus.PASSED),
        warnings=sum(1 for r in results if r.status == CheckStatus.WARNING),
        failed=sum(1 for r in results if r.status == CheckStatus.FAILED),
        errors=sum(1 for r in results if r.status == CheckStatus.ERROR),
        actions_proposed=actions_proposed,
        checked_at=datetime.now(UTC),
    )


@router.post("/quality/scan", response_model=QualityScanResponse)
async def scan_namespace(
    payload: QualityScanRequest,
    session: AsyncSession = Depends(get_db),
) -> QualityScanResponse:
    """Scan all tables in a namespace and coordinate any failures.

    Runs schema drift, freshness, and volume checks on every table.
    Failed checks automatically create events, diagnoses, and proposed actions.
    """
    agent = _get_agent()

    try:
        all_results, total_actions = await scan_namespace_and_coordinate(
            agent,
            session,
            namespace=payload.namespace,
            max_age_hours=payload.max_age_hours,
        )
    except Exception as e:
        logger.error("quality_scan_error", namespace=payload.namespace, error=str(e))
        raise HTTPException(status_code=502, detail=f"Namespace scan failed: {e}") from e

    table_reports: list[QualityTableReport] = []
    total_checks = 0
    total_passed = 0
    total_failed = 0

    for table_name, results in all_results.items():
        checks = [_to_response(r) for r in results]
        passed = sum(1 for r in results if r.status == CheckStatus.PASSED)
        warnings = sum(1 for r in results if r.status == CheckStatus.WARNING)
        failed = sum(1 for r in results if r.status == CheckStatus.FAILED)
        errors = sum(1 for r in results if r.status == CheckStatus.ERROR)

        table_reports.append(
            QualityTableReport(
                table=table_name,
                checks=checks,
                passed=passed,
                warnings=warnings,
                failed=failed,
                errors=errors,
            )
        )
        total_checks += len(results)
        total_passed += passed
        total_failed += failed

    return QualityScanResponse(
        namespace=payload.namespace,
        tables_scanned=len(all_results),
        total_checks=total_checks,
        total_passed=total_passed,
        total_failed=total_failed,
        tables=table_reports,
        actions_proposed=total_actions,
        scanned_at=datetime.now(UTC),
    )
