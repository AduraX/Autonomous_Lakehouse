"""Azure Data Factory platform adapter.

Implements the PlatformAdapter protocol for ADF pipelines.
Currently returns mock data; structured so real Azure REST API calls
can be enabled by providing credentials via adf_* settings.
"""

from datetime import UTC, datetime, timedelta

import httpx

from lakehouse.config import get_settings
from lakehouse.logging import get_logger
from lakehouse.models.events import PipelineStatus, Platform
from lakehouse.schemas.events import PipelineEventCreate

logger = get_logger(__name__)

_ADF_STATUS_MAP: dict[str, PipelineStatus] = {
    "Succeeded": PipelineStatus.SUCCEEDED,
    "Failed": PipelineStatus.FAILED,
    "InProgress": PipelineStatus.RUNNING,
    "Cancelled": PipelineStatus.CANCELLED,
    "Queued": PipelineStatus.RUNNING,
}


class AzureDataFactoryAdapter:
    """Azure Data Factory adapter implementing PlatformAdapter protocol."""

    def __init__(self) -> None:
        settings = get_settings()
        self._subscription_id = settings.adf_subscription_id
        self._resource_group = settings.adf_resource_group
        self._factory_name = settings.adf_factory_name
        self._client_secret = settings.adf_client_secret
        self._base_url = (
            f"https://management.azure.com/subscriptions/{self._subscription_id}"
            f"/resourceGroups/{self._resource_group}"
            f"/providers/Microsoft.DataFactory/factories/{self._factory_name}"
        )

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            headers={"Authorization": "Bearer <token>"},
            timeout=30.0,
        )

    async def fetch_recent_events(self, limit: int = 50) -> list[PipelineEventCreate]:
        """Fetch recent pipeline runs from ADF."""
        now = datetime.now(UTC)
        # Mock data — replace with real API call:
        # POST {base_url}/queryPipelineRuns?api-version=2018-06-01
        events = [
            PipelineEventCreate(
                external_run_id=f"adf-run-{i}-{int(now.timestamp())}",
                pipeline_name="ingest_sales_data" if i % 2 == 0 else "transform_daily_revenue",
                platform=Platform.ADF,
                status=PipelineStatus.FAILED if i % 3 == 0 else PipelineStatus.SUCCEEDED,
                error_message="Column 'customer_id' not found in source schema"
                if i % 3 == 0
                else None,
                error_code="SCHEMA_ERROR_4012" if i % 3 == 0 else None,
                started_at=now - timedelta(hours=i, minutes=30),
                finished_at=now - timedelta(hours=i),
                duration_seconds=1800.0,
            )
            for i in range(min(limit, 5))
        ]
        logger.info("adf_events_fetched", count=len(events))
        return events

    async def retry_pipeline(self, external_run_id: str) -> dict[str, str]:
        """Trigger a retry of a failed ADF pipeline run."""
        # Real call: POST {base_url}/pipelines/{name}/createRun?api-version=2018-06-01
        logger.info("adf_pipeline_retry", run_id=external_run_id)
        return {"status": "triggered", "new_run_id": f"adf-retry-{external_run_id}"}

    async def cancel_pipeline(self, external_run_id: str) -> dict[str, str]:
        """Cancel a running ADF pipeline."""
        # Real call: POST {base_url}/pipelineruns/{runId}/cancel?api-version=2018-06-01
        logger.info("adf_pipeline_cancel", run_id=external_run_id)
        return {"status": "cancelled", "run_id": external_run_id}

    async def get_pipeline_logs(self, external_run_id: str) -> list[str]:
        """Fetch activity run logs for an ADF pipeline run."""
        # Real call: POST {base_url}/pipelineruns/{runId}/queryActivityruns?api-version=2018-06-01
        return [
            f"[INFO] ADF pipeline run {external_run_id} started",
            "[INFO] Activity 'CopySalesData' started",
            "[INFO] Reading from Azure SQL source",
            "[ERROR] Column 'customer_id' not found in source schema",
            "[ERROR] Activity 'CopySalesData' failed with error code SCHEMA_ERROR_4012",
        ]

    async def check_source_availability(self, source_name: str) -> bool:
        """Verify that an ADF linked service or dataset is reachable."""
        # Real call: GET {base_url}/linkedservices/{name}?api-version=2018-06-01
        logger.info("adf_source_check", source=source_name)
        return True

    async def get_pipeline_metadata(self, pipeline_name: str) -> dict[str, object]:
        """Fetch ADF pipeline definition metadata."""
        # Real call: GET {base_url}/pipelines/{name}?api-version=2018-06-01
        return {
            "pipeline_name": pipeline_name,
            "factory_name": self._factory_name,
            "resource_group": self._resource_group,
            "owner": "data-engineering@company.com",
            "activities": ["CopySalesData", "ValidateSchema", "LoadToWarehouse"],
            "timeout_minutes": 120,
        }
