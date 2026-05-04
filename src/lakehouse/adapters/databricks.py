"""Databricks platform adapter.

Implements the PlatformAdapter protocol for Databricks Jobs API v2.1.
Uses real HTTP calls via httpx with Bearer token authentication.

Requires:
  - DATABRICKS_WORKSPACE_URL: base URL of the Databricks workspace
  - DATABRICKS_TOKEN: personal access token or service principal token
"""

import json
from datetime import UTC, datetime

import httpx

from lakehouse.config import get_settings
from lakehouse.logging import get_logger
from lakehouse.models.events import PipelineStatus, Platform
from lakehouse.schemas.events import PipelineEventCreate

logger = get_logger(__name__)

# Map Databricks run life_cycle_state + result_state to canonical PipelineStatus.
# Databricks runs have a life_cycle_state (PENDING, RUNNING, TERMINATING,
# TERMINATED, SKIPPED, INTERNAL_ERROR) and a result_state (SUCCESS, FAILED,
# TIMEDOUT, CANCELED) that is only set when life_cycle_state is TERMINATED.
_RESULT_STATE_MAP: dict[str, PipelineStatus] = {
    "SUCCESS": PipelineStatus.SUCCEEDED,
    "FAILED": PipelineStatus.FAILED,
    "TIMEDOUT": PipelineStatus.FAILED,
    "CANCELED": PipelineStatus.CANCELLED,
}

_LIFECYCLE_STATE_MAP: dict[str, PipelineStatus] = {
    "RUNNING": PipelineStatus.RUNNING,
    "PENDING": PipelineStatus.RUNNING,
    "TERMINATING": PipelineStatus.CANCELLED,
    "SKIPPED": PipelineStatus.CANCELLED,
    "INTERNAL_ERROR": PipelineStatus.FAILED,
}


def _map_status(life_cycle_state: str, result_state: str | None) -> PipelineStatus:
    """Map Databricks run states to a canonical PipelineStatus."""
    if life_cycle_state == "TERMINATED" and result_state:
        return _RESULT_STATE_MAP.get(result_state, PipelineStatus.FAILED)
    return _LIFECYCLE_STATE_MAP.get(life_cycle_state, PipelineStatus.FAILED)


class DatabricksAdapter:
    """Databricks adapter implementing PlatformAdapter protocol.

    Talks to the Databricks Jobs API v2.1 over HTTP using a personal
    access token (PAT) or service-principal token for authentication.
    """

    def __init__(self) -> None:
        settings = get_settings()
        self._workspace_url = settings.databricks_workspace_url.rstrip("/")
        self._token = settings.databricks_token
        self._headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    def _client(self) -> httpx.AsyncClient:
        """Create an HTTP client with auth headers."""
        return httpx.AsyncClient(
            headers=self._headers,
            timeout=30.0,
        )

    @staticmethod
    def _epoch_ms_to_datetime(epoch_ms: int | None) -> datetime | None:
        """Convert Databricks epoch-millisecond timestamp to datetime."""
        if not epoch_ms:
            return None
        try:
            return datetime.fromtimestamp(epoch_ms / 1000.0, tz=UTC)
        except (ValueError, OSError):
            return None

    async def fetch_recent_events(self, limit: int = 50) -> list[PipelineEventCreate]:
        """Fetch recent job runs from the Databricks workspace.

        Calls GET /api/2.1/jobs/runs/list with the requested limit and
        maps each run to a PipelineEventCreate.
        """
        events: list[PipelineEventCreate] = []

        async with self._client() as client:
            params: dict[str, str | int] = {"limit": min(limit, 100), "expand_tasks": "false"}
            resp = await client.get(
                f"{self._workspace_url}/api/2.1/jobs/runs/list",
                params=params,
            )
            resp.raise_for_status()
            data = resp.json()

        for run in data.get("runs", []):
            run_id = str(run.get("run_id", ""))
            run_name = run.get("run_name", "unknown")
            state = run.get("state", {})
            life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
            result_state = state.get("result_state")
            status = _map_status(life_cycle_state, result_state)

            started = self._epoch_ms_to_datetime(run.get("start_time"))
            finished = self._epoch_ms_to_datetime(run.get("end_time"))
            duration_ms = run.get("execution_duration")
            duration_s = round(duration_ms / 1000.0, 2) if duration_ms else None

            error_msg = state.get("state_message") if status == PipelineStatus.FAILED else None

            events.append(
                PipelineEventCreate(
                    external_run_id=run_id,
                    pipeline_name=run_name,
                    platform=Platform.DATABRICKS,
                    status=status,
                    error_message=error_msg,
                    error_code=result_state if status == PipelineStatus.FAILED else None,
                    started_at=started,
                    finished_at=finished,
                    duration_seconds=duration_s,
                    metadata_json=json.dumps(
                        {
                            "job_id": run.get("job_id"),
                            "cluster_id": run.get("cluster_instance", {}).get("cluster_id"),
                            "run_page_url": run.get("run_page_url"),
                        }
                    ),
                )
            )

        logger.info("databricks_events_fetched", count=len(events))
        return events

    async def retry_pipeline(self, external_run_id: str) -> dict[str, str]:
        """Retry a failed Databricks job run.

        Fetches the original run to get the job_id, then submits a new
        run via POST /api/2.1/jobs/run-now.
        """
        async with self._client() as client:
            # Get original run to find the job_id
            resp = await client.get(
                f"{self._workspace_url}/api/2.1/jobs/runs/get",
                params={"run_id": external_run_id},
            )
            resp.raise_for_status()
            original_run = resp.json()
            job_id = original_run.get("job_id")

            # Trigger a new run of the same job
            resp = await client.post(
                f"{self._workspace_url}/api/2.1/jobs/run-now",
                json={"job_id": job_id},
            )
            resp.raise_for_status()
            new_run = resp.json()

        new_run_id = str(new_run.get("run_id", "unknown"))
        logger.info(
            "databricks_pipeline_retry",
            original_run_id=external_run_id,
            new_run_id=new_run_id,
        )
        return {"status": "triggered", "new_run_id": new_run_id}

    async def cancel_pipeline(self, external_run_id: str) -> dict[str, str]:
        """Cancel a running Databricks job run."""
        async with self._client() as client:
            resp = await client.post(
                f"{self._workspace_url}/api/2.1/jobs/runs/cancel",
                json={"run_id": int(external_run_id)},
            )
            resp.raise_for_status()

        logger.info("databricks_pipeline_cancelled", run_id=external_run_id)
        return {"status": "cancelled", "run_id": external_run_id}

    async def get_pipeline_logs(self, external_run_id: str) -> list[str]:
        """Fetch output/logs for a Databricks job run.

        Uses GET /api/2.1/jobs/runs/get-output to retrieve the
        notebook or task output associated with a run.
        """
        logs: list[str] = []

        async with self._client() as client:
            resp = await client.get(
                f"{self._workspace_url}/api/2.1/jobs/runs/get-output",
                params={"run_id": external_run_id},
            )
            resp.raise_for_status()
            data = resp.json()

        # Extract notebook output if present
        notebook_output = data.get("notebook_output", {})
        if notebook_output:
            result = notebook_output.get("result", "")
            if result:
                logs.append(f"[OUTPUT] {result}")
            if notebook_output.get("truncated"):
                logs.append("[WARN] Output was truncated")

        # Extract error info from run metadata
        metadata = data.get("metadata", {})
        state = metadata.get("state", {})
        run_name = metadata.get("run_name", external_run_id)
        life_cycle = state.get("life_cycle_state", "UNKNOWN")
        logs.insert(0, f"[INFO] Run: {run_name} (state: {life_cycle})")

        state_message = state.get("state_message", "")
        if state_message:
            logs.append(f"[ERROR] {state_message}")

        # Log output if present
        log_output = data.get("log", "")
        if log_output:
            for line in log_output.strip().splitlines():
                logs.append(f"[LOG] {line}")

        logger.info("databricks_logs_fetched", run_id=external_run_id, lines=len(logs))
        return logs

    async def check_source_availability(self, source_name: str) -> bool:
        """Check if the Databricks workspace is reachable.

        Uses GET /api/2.0/clusters/list as a lightweight health check.
        """
        try:
            async with self._client() as client:
                resp = await client.get(
                    f"{self._workspace_url}/api/2.0/clusters/list",
                    timeout=10.0,
                )
                available = resp.status_code == 200
        except (httpx.ConnectError, httpx.TimeoutException):
            available = False

        logger.info(
            "databricks_availability_check",
            source=source_name,
            available=available,
        )
        return available

    async def get_pipeline_metadata(self, pipeline_name: str) -> dict[str, object]:
        """Fetch job metadata from Databricks, filtered by name.

        Calls GET /api/2.1/jobs/list with a name filter and returns
        the first matching job's configuration.
        """
        async with self._client() as client:
            params: dict[str, str | int] = {"name": pipeline_name, "limit": 1}
            resp = await client.get(
                f"{self._workspace_url}/api/2.1/jobs/list",
                params=params,
            )
            resp.raise_for_status()
            data = resp.json()

        jobs = data.get("jobs", [])
        if not jobs:
            return {"pipeline_name": pipeline_name, "error": "not found"}

        job = jobs[0]
        job_settings = job.get("settings", {})

        return {
            "pipeline_name": job_settings.get("name", pipeline_name),
            "job_id": str(job.get("job_id", "")),
            "schedule": job_settings.get("schedule", {}).get("quartz_cron_expression", ""),
            "max_concurrent_runs": job_settings.get("max_concurrent_runs", 1),
            "timeout_seconds": job_settings.get("timeout_seconds", 0),
            "creator_user_name": job.get("creator_user_name", ""),
            "platform": "databricks",
        }
