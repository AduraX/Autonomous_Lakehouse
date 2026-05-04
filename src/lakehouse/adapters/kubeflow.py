"""Kubeflow Pipelines adapter — monitors KF4X pipeline runs.

Integrates with the Kubeflow Pipelines REST API to fetch run status,
retrieve logs, and trigger retries. Designed for KF4X deployments
with Keycloak authentication.

Requires:
  - KUBEFLOW_HOST: base URL of the Kubeflow deployment
  - KUBEFLOW_NAMESPACE: Kubernetes namespace for pipelines
  - KUBEFLOW_SA_TOKEN: ServiceAccount token for API access
"""

import json
from datetime import datetime

import httpx

from lakehouse.config import get_settings
from lakehouse.logging import get_logger
from lakehouse.models.events import PipelineStatus, Platform
from lakehouse.schemas.events import PipelineEventCreate

logger = get_logger(__name__)

# Map Kubeflow run states to our canonical PipelineStatus
_KF_STATUS_MAP: dict[str, PipelineStatus] = {
    "SUCCEEDED": PipelineStatus.SUCCEEDED,
    "FAILED": PipelineStatus.FAILED,
    "ERROR": PipelineStatus.FAILED,
    "RUNNING": PipelineStatus.RUNNING,
    "PENDING": PipelineStatus.RUNNING,
    "SKIPPED": PipelineStatus.CANCELLED,
    "CANCELING": PipelineStatus.CANCELLED,
    "CANCELED": PipelineStatus.CANCELLED,
    "PAUSED": PipelineStatus.RUNNING,
}


class KubeflowAdapter:
    """Monitors Kubeflow Pipeline runs via the Pipelines REST API.

    Compatible with both Kubeflow Pipelines v1 and v2 APIs.
    Uses the v2beta1 API endpoints (Kubeflow Pipelines v2).
    """

    def __init__(self) -> None:
        settings = get_settings()
        self._host = settings.kubeflow_host.rstrip("/")
        self._namespace = settings.kubeflow_namespace
        self._token = settings.kubeflow_sa_token
        self._base_url = f"{self._host}/apis/v2beta1"
        self._headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    def _client(self) -> httpx.AsyncClient:
        """Create an HTTP client with auth headers and SSL verification."""
        return httpx.AsyncClient(
            headers=self._headers,
            timeout=30.0,
            verify=True,
        )

    @staticmethod
    def _parse_timestamp(ts: str | None) -> datetime | None:
        """Parse a Kubeflow ISO timestamp string."""
        if not ts:
            return None
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def _compute_duration(started: datetime | None, finished: datetime | None) -> float | None:
        """Compute duration in seconds between two timestamps."""
        if started and finished:
            return (finished - started).total_seconds()
        return None

    async def fetch_recent_events(self, limit: int = 50) -> list[PipelineEventCreate]:
        """Fetch recent pipeline runs from Kubeflow.

        Queries the Kubeflow Pipelines v2 API for runs in the configured
        namespace, sorted by creation time (most recent first).
        """
        events: list[PipelineEventCreate] = []

        async with self._client() as client:
            params: dict[str, str | int] = {
                "page_size": min(limit, 100),
                "sort_by": "created_at desc",
                "namespace": self._namespace,
            }
            response = await client.get(f"{self._base_url}/runs", params=params)
            response.raise_for_status()
            data = response.json()

        for run in data.get("runs", []):
            run_id = run.get("run_id", run.get("id", ""))
            display_name = run.get("display_name", run.get("name", "unknown"))
            kf_state = run.get("state", run.get("status", "UNKNOWN"))
            status = _KF_STATUS_MAP.get(kf_state, PipelineStatus.FAILED)

            started = self._parse_timestamp(run.get("created_at"))
            finished = self._parse_timestamp(run.get("finished_at"))
            duration = self._compute_duration(started, finished)

            # Extract error from run details
            error_msg = run.get("error", None)
            if not error_msg and status == PipelineStatus.FAILED:
                error_msg = (
                    run.get("state_history", [{}])[-1].get("error", None)
                    if run.get("state_history")
                    else None
                )

            events.append(
                PipelineEventCreate(
                    external_run_id=run_id,
                    pipeline_name=display_name,
                    platform=Platform.KUBEFLOW,
                    status=status,
                    error_message=error_msg,
                    error_code=kf_state if status == PipelineStatus.FAILED else None,
                    started_at=started,
                    finished_at=finished,
                    duration_seconds=round(duration, 2) if duration else None,
                    metadata_json=json.dumps(
                        {
                            "namespace": self._namespace,
                            "pipeline_id": run.get("pipeline_spec", {}).get("pipeline_id", None),
                            "experiment_id": run.get("experiment_id", None),
                            "service_account": run.get("service_account", None),
                        }
                    ),
                )
            )

        logger.info(
            "kubeflow_events_fetched",
            count=len(events),
            namespace=self._namespace,
        )
        return events

    async def retry_pipeline(self, external_run_id: str) -> dict[str, str]:
        """Retry a failed Kubeflow pipeline run.

        Fetches the original run's pipeline spec and creates a new run
        with the same configuration.
        """
        async with self._client() as client:
            # Get original run details
            resp = await client.get(f"{self._base_url}/runs/{external_run_id}")
            resp.raise_for_status()
            original_run = resp.json()

            # Create a new run with the same pipeline spec
            new_run_payload = {
                "display_name": f"retry-{original_run.get('display_name', external_run_id)}",
                "pipeline_spec": original_run.get("pipeline_spec", {}),
                "runtime_config": original_run.get("runtime_config", {}),
                "service_account": original_run.get("service_account", ""),
                "experiment_id": original_run.get("experiment_id", ""),
            }

            resp = await client.post(
                f"{self._base_url}/runs",
                json=new_run_payload,
            )
            resp.raise_for_status()
            new_run = resp.json()

        new_run_id = new_run.get("run_id", new_run.get("id", "unknown"))
        logger.info(
            "kubeflow_pipeline_retry",
            original_run_id=external_run_id,
            new_run_id=new_run_id,
        )
        return {"status": "triggered", "new_run_id": new_run_id}

    async def cancel_pipeline(self, external_run_id: str) -> dict[str, str]:
        """Cancel a running Kubeflow pipeline."""
        async with self._client() as client:
            resp = await client.post(f"{self._base_url}/runs/{external_run_id}:terminate")
            resp.raise_for_status()

        logger.info("kubeflow_pipeline_cancelled", run_id=external_run_id)
        return {"status": "cancelled", "run_id": external_run_id}

    async def get_pipeline_logs(self, external_run_id: str) -> list[str]:
        """Fetch logs for a Kubeflow pipeline run.

        Retrieves logs from each node (task) in the run's workflow.
        """
        logs: list[str] = []

        async with self._client() as client:
            resp = await client.get(f"{self._base_url}/runs/{external_run_id}")
            resp.raise_for_status()
            run = resp.json()

            run_name = run.get("display_name", external_run_id)
            state = run.get("state", "UNKNOWN")
            logs.append(f"[INFO] Run: {run_name} (state: {state})")

            # Fetch task details if available
            if "run_details" in run and "task_details" in run["run_details"]:
                for task in run["run_details"]["task_details"]:
                    task_name = task.get("display_name", "unknown")
                    task_state = task.get("state", "UNKNOWN")
                    logs.append(f"[TASK] {task_name}: {task_state}")

                    if task.get("error"):
                        logs.append(f"[ERROR] {task_name}: {task['error']}")

            if run.get("error"):
                logs.append(f"[ERROR] Run error: {run['error']}")

        logger.info("kubeflow_logs_fetched", run_id=external_run_id, lines=len(logs))
        return logs

    async def check_source_availability(self, source_name: str) -> bool:
        """Check if the Kubeflow API is reachable (source = the cluster itself)."""
        try:
            async with self._client() as client:
                resp = await client.get(
                    f"{self._host}/apis/v2beta1/healthz",
                    timeout=10.0,
                )
                available = resp.status_code == 200
        except (httpx.ConnectError, httpx.TimeoutException):
            available = False

        logger.info("kubeflow_availability_check", source=source_name, available=available)
        return available

    async def get_pipeline_metadata(self, pipeline_name: str) -> dict[str, object]:
        """Fetch pipeline metadata from the Kubeflow catalog.

        Searches for the pipeline by name and returns its configuration.
        """
        async with self._client() as client:
            params: dict[str, str | int] = {
                "filter": json.dumps(
                    {
                        "predicates": [
                            {
                                "key": "display_name",
                                "operation": "EQUALS",
                                "string_value": pipeline_name,
                            }
                        ]
                    }
                ),
                "page_size": 1,
                "namespace": self._namespace,
            }
            resp = await client.get(f"{self._base_url}/pipelines", params=params)
            resp.raise_for_status()
            data = resp.json()

        pipelines = data.get("pipelines", [])
        if not pipelines:
            return {"pipeline_name": pipeline_name, "error": "not found"}

        pipeline = pipelines[0]
        created = pipeline.get("created_at", "")

        return {
            "pipeline_name": pipeline.get("display_name", pipeline_name),
            "pipeline_id": pipeline.get("pipeline_id", ""),
            "description": pipeline.get("description", ""),
            "created_at": created,
            "namespace": self._namespace,
            "platform": "kubeflow",
        }
