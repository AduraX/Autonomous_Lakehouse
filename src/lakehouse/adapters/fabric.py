"""Microsoft Fabric platform adapter.

Implements the PlatformAdapter protocol for Fabric pipelines.
Currently returns mock data; structured so real Fabric REST API calls
can be enabled by providing credentials via fabric_* settings.
"""

from datetime import UTC, datetime, timedelta

import httpx

from lakehouse.config import get_settings
from lakehouse.logging import get_logger
from lakehouse.models.events import PipelineStatus, Platform
from lakehouse.schemas.events import PipelineEventCreate

logger = get_logger(__name__)

_FABRIC_STATUS_MAP: dict[str, PipelineStatus] = {
    "Completed": PipelineStatus.SUCCEEDED,
    "Failed": PipelineStatus.FAILED,
    "InProgress": PipelineStatus.RUNNING,
    "Cancelled": PipelineStatus.CANCELLED,
    "Deduped": PipelineStatus.CANCELLED,
    "NotStarted": PipelineStatus.RUNNING,
}


class FabricAdapter:
    """Microsoft Fabric adapter implementing PlatformAdapter protocol."""

    def __init__(self) -> None:
        settings = get_settings()
        self._workspace_id = settings.fabric_workspace_id
        self._client_secret = settings.fabric_client_secret
        self._base_url = f"https://api.fabric.microsoft.com/v1/workspaces/{self._workspace_id}"

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            headers={"Authorization": "Bearer <token>"},
            timeout=30.0,
        )

    async def fetch_recent_events(self, limit: int = 50) -> list[PipelineEventCreate]:
        """Fetch recent pipeline runs from Fabric."""
        now = datetime.now(UTC)
        # Mock data — replace with real API call:
        # GET {base_url}/items/{itemId}/jobs/instances
        events = [
            PipelineEventCreate(
                external_run_id=f"fabric-run-{i}-{int(now.timestamp())}",
                pipeline_name="lakehouse_refresh" if i % 2 == 0 else "notebook_transform",
                platform=Platform.FABRIC,
                status=PipelineStatus.FAILED if i % 3 == 0 else PipelineStatus.SUCCEEDED,
                error_message="Lakehouse table refresh failed: partition not found"
                if i % 3 == 0
                else None,
                error_code="FABRIC_REFRESH_ERR_001" if i % 3 == 0 else None,
                started_at=now - timedelta(hours=i, minutes=45),
                finished_at=now - timedelta(hours=i),
                duration_seconds=2700.0,
            )
            for i in range(min(limit, 5))
        ]
        logger.info("fabric_events_fetched", count=len(events))
        return events

    async def retry_pipeline(self, external_run_id: str) -> dict[str, str]:
        """Trigger a retry of a failed Fabric pipeline run."""
        # Real call: POST {base_url}/items/{itemId}/jobs/instances?jobType=Pipeline
        logger.info("fabric_pipeline_retry", run_id=external_run_id)
        return {"status": "triggered", "new_run_id": f"fabric-retry-{external_run_id}"}

    async def cancel_pipeline(self, external_run_id: str) -> dict[str, str]:
        """Cancel a running Fabric pipeline."""
        # Real call: POST {base_url}/items/{itemId}/jobs/instances/{jobInstanceId}/cancel
        logger.info("fabric_pipeline_cancel", run_id=external_run_id)
        return {"status": "cancelled", "run_id": external_run_id}

    async def get_pipeline_logs(self, external_run_id: str) -> list[str]:
        """Fetch execution logs for a Fabric pipeline run."""
        return [
            f"[INFO] Fabric pipeline run {external_run_id} started",
            "[INFO] Lakehouse table refresh initiated",
            "[INFO] Reading from OneLake storage",
            "[ERROR] Partition 'date=2026-04-28' not found in source",
            "[ERROR] Lakehouse refresh failed at stage: data_load",
        ]

    async def check_source_availability(self, source_name: str) -> bool:
        """Verify that a Fabric data source is reachable."""
        # Real call: GET {base_url}/items to check workspace connectivity
        logger.info("fabric_source_check", source=source_name)
        return True

    async def get_pipeline_metadata(self, pipeline_name: str) -> dict[str, object]:
        """Fetch Fabric pipeline item metadata."""
        # Real call: GET {base_url}/items?type=DataPipeline
        return {
            "pipeline_name": pipeline_name,
            "workspace_id": self._workspace_id,
            "owner": "data-engineering@company.com",
            "item_type": "DataPipeline",
            "capacity": "F64",
            "timeout_minutes": 90,
        }
