"""Mock platform adapter for local development and testing.

Generates realistic fake pipeline events so you can develop and test
the entire agent pipeline without needing Azure credentials.
"""

import random
from datetime import UTC, datetime, timedelta

from lakehouse.logging import get_logger
from lakehouse.models.events import PipelineStatus, Platform
from lakehouse.schemas.events import PipelineEventCreate

logger = get_logger(__name__)

MOCK_PIPELINES = [
    "ingest_sales_data",
    "ingest_customer_profiles",
    "transform_daily_revenue",
    "load_dimension_products",
    "refresh_ml_features",
    "sync_crm_contacts",
    "aggregate_clickstream",
    "validate_inventory_feed",
]

MOCK_ERRORS = [
    ("Schema mismatch on column 'amount': expected DECIMAL, got STRING", "SCHEMA_ERROR_4012"),
    ("Connection timeout to source database after 300s", "TIMEOUT_5001"),
    ("Source blob container 'raw-data' not found", "SOURCE_UNAVAIL_3001"),
    ("Permission denied: service principal lacks Reader role", "AUTH_ERROR_4030"),
    ("Out of memory: executor exceeded 8GB limit", "RESOURCE_OOM_5002"),
    ("Duplicate primary key violation on 'customer_id'", "DQ_DUPLICATE_6001"),
    ("Upstream pipeline 'ingest_sales_data' has not completed", "DEP_FAIL_7001"),
    ("Partition column 'date' contains null values", "DQ_NULL_6002"),
]


class MockAdapter:
    """Generates fake pipeline events for local development."""

    async def fetch_recent_events(self, limit: int = 50) -> list[PipelineEventCreate]:
        """Generate a batch of mock pipeline events."""
        events: list[PipelineEventCreate] = []
        now = datetime.now(UTC)

        for i in range(min(limit, 10)):
            pipeline = random.choice(MOCK_PIPELINES)
            status = random.choices(
                [PipelineStatus.SUCCEEDED, PipelineStatus.FAILED, PipelineStatus.TIMED_OUT],
                weights=[0.6, 0.3, 0.1],
                k=1,
            )[0]

            duration = random.uniform(30, 600)
            started = now - timedelta(seconds=duration + random.uniform(0, 3600))
            finished = started + timedelta(seconds=duration)

            error_msg = None
            error_code = None
            if status != PipelineStatus.SUCCEEDED:
                error_msg, error_code = random.choice(MOCK_ERRORS)

            events.append(
                PipelineEventCreate(
                    external_run_id=f"mock-run-{i}-{int(now.timestamp())}",
                    pipeline_name=pipeline,
                    platform=Platform.MOCK,
                    status=status,
                    error_message=error_msg,
                    error_code=error_code,
                    started_at=started,
                    finished_at=finished,
                    duration_seconds=round(duration, 2),
                )
            )

        logger.info("mock_events_generated", count=len(events))
        return events

    async def retry_pipeline(self, external_run_id: str) -> dict[str, str]:
        """Simulate a pipeline retry."""
        logger.info("mock_pipeline_retry", run_id=external_run_id)
        return {"status": "triggered", "new_run_id": f"mock-retry-{external_run_id}"}

    async def cancel_pipeline(self, external_run_id: str) -> dict[str, str]:
        """Simulate cancelling a pipeline."""
        logger.info("mock_pipeline_cancel", run_id=external_run_id)
        return {"status": "cancelled", "run_id": external_run_id}

    async def get_pipeline_logs(self, external_run_id: str) -> list[str]:
        """Return simulated log lines."""
        return [
            f"[INFO] Starting pipeline run {external_run_id}",
            "[INFO] Connecting to source: Azure Blob Storage",
            "[INFO] Reading partition: date=2026-04-28",
            "[WARN] Schema validation: 2 warnings detected",
            "[ERROR] Column 'amount' type mismatch — expected DECIMAL(18,2), got VARCHAR(50)",
            "[ERROR] Pipeline failed at stage: schema_validation",
        ]

    async def check_source_availability(self, source_name: str) -> bool:
        """Simulate source availability check — 90% available."""
        available = random.random() > 0.1
        logger.info("mock_source_check", source=source_name, available=available)
        return available

    async def get_pipeline_metadata(self, pipeline_name: str) -> dict[str, object]:
        """Return mock pipeline metadata."""
        return {
            "pipeline_name": pipeline_name,
            "owner": "data-engineering@company.com",
            "schedule": "0 */4 * * *",
            "cluster_size": "Standard_DS3_v2",
            "max_retries": 3,
            "timeout_minutes": 60,
        }
