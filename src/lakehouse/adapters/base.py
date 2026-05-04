"""Platform adapter protocol — the contract all adapters must implement."""

from typing import Protocol

from lakehouse.schemas.events import PipelineEventCreate


class PlatformAdapter(Protocol):
    """Interface for lakehouse platform integrations.

    Every adapter (ADF, Fabric, Databricks, Mock) implements this protocol.
    The coordinator calls these methods without knowing which platform is behind them.
    """

    async def fetch_recent_events(self, limit: int = 50) -> list[PipelineEventCreate]:
        """Poll the platform for recent pipeline run events."""
        ...

    async def retry_pipeline(self, external_run_id: str) -> dict[str, str]:
        """Trigger a retry of a failed pipeline run."""
        ...

    async def cancel_pipeline(self, external_run_id: str) -> dict[str, str]:
        """Cancel a running pipeline."""
        ...

    async def get_pipeline_logs(self, external_run_id: str) -> list[str]:
        """Fetch execution logs for a pipeline run."""
        ...

    async def check_source_availability(self, source_name: str) -> bool:
        """Verify that a data source is reachable."""
        ...

    async def get_pipeline_metadata(self, pipeline_name: str) -> dict[str, object]:
        """Fetch metadata (schedule, owner, config) for a pipeline."""
        ...
