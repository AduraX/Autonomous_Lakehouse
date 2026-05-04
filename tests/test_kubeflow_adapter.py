"""Tests for the Kubeflow platform adapter.

Uses httpx mock transport to simulate Kubeflow API responses
without requiring a live cluster.
"""

import httpx
import pytest

from lakehouse.adapters.kubeflow import _KF_STATUS_MAP, KubeflowAdapter
from lakehouse.models.events import PipelineStatus


def _mock_runs_response() -> dict:
    """Generate a mock Kubeflow Pipelines v2 runs response."""
    return {
        "runs": [
            {
                "run_id": "run-001",
                "display_name": "train-model-v2",
                "state": "SUCCEEDED",
                "created_at": "2026-04-28T10:00:00Z",
                "finished_at": "2026-04-28T10:30:00Z",
                "pipeline_spec": {"pipeline_id": "pipe-abc"},
                "experiment_id": "exp-001",
            },
            {
                "run_id": "run-002",
                "display_name": "ingest-daily-data",
                "state": "FAILED",
                "created_at": "2026-04-28T09:00:00Z",
                "finished_at": "2026-04-28T09:15:00Z",
                "error": "OOMKilled: container exceeded memory limit",
                "pipeline_spec": {"pipeline_id": "pipe-def"},
                "experiment_id": "exp-002",
            },
            {
                "run_id": "run-003",
                "display_name": "feature-engineering",
                "state": "RUNNING",
                "created_at": "2026-04-28T11:00:00Z",
                "finished_at": None,
                "pipeline_spec": {},
            },
        ]
    }


class MockTransport(httpx.AsyncBaseTransport):
    """Mock HTTP transport that returns canned responses for Kubeflow API calls."""

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path

        if path.endswith("/runs") and request.method == "GET":
            return httpx.Response(200, json=_mock_runs_response())
        if "/runs/run-001" in path and request.method == "GET":
            return httpx.Response(200, json=_mock_runs_response()["runs"][0])
        if "/runs/run-002" in path and request.method == "GET":
            return httpx.Response(200, json=_mock_runs_response()["runs"][1])
        if path.endswith(":terminate"):
            return httpx.Response(200, json={})
        if path.endswith("/runs") and request.method == "POST":
            return httpx.Response(200, json={"run_id": "run-retry-001"})
        if path.endswith("/healthz"):
            return httpx.Response(200, json={"status": "ok"})
        if "/pipelines" in path and request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "pipelines": [
                        {
                            "pipeline_id": "pipe-abc",
                            "display_name": "train-model-v2",
                            "description": "Training pipeline",
                            "created_at": "2026-04-01T00:00:00Z",
                        }
                    ]
                },
            )

        return httpx.Response(404, json={"error": "not found"})


@pytest.fixture
def adapter(monkeypatch: pytest.MonkeyPatch) -> KubeflowAdapter:
    """Create a KubeflowAdapter with mock HTTP transport."""
    monkeypatch.setenv("KUBEFLOW_HOST", "https://kubeflow.test.com")
    monkeypatch.setenv("KUBEFLOW_NAMESPACE", "test-ns")
    monkeypatch.setenv("KUBEFLOW_SA_TOKEN", "fake-token")

    # Clear cached settings so new env vars take effect
    from lakehouse.config import get_settings

    get_settings.cache_clear()

    kf = KubeflowAdapter()

    # Patch _client to use mock transport

    def mock_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=MockTransport(),
            headers=kf._headers,
            base_url=kf._base_url,
        )

    kf._client = mock_client  # type: ignore[assignment]
    return kf


async def test_fetch_recent_events(adapter: KubeflowAdapter) -> None:
    """Should parse Kubeflow runs into PipelineEventCreate objects."""
    events = await adapter.fetch_recent_events(limit=10)

    assert len(events) == 3

    # First run: succeeded
    assert events[0].external_run_id == "run-001"
    assert events[0].pipeline_name == "train-model-v2"
    assert events[0].status == PipelineStatus.SUCCEEDED
    assert events[0].platform.value == "kubeflow"
    assert events[0].duration_seconds is not None
    assert events[0].duration_seconds == pytest.approx(1800.0, abs=1)

    # Second run: failed with error
    assert events[1].external_run_id == "run-002"
    assert events[1].status == PipelineStatus.FAILED
    assert "OOMKilled" in (events[1].error_message or "")

    # Third run: running (no finished_at)
    assert events[2].status == PipelineStatus.RUNNING
    assert events[2].duration_seconds is None


async def test_retry_pipeline(adapter: KubeflowAdapter) -> None:
    """Should trigger a retry and return the new run ID."""
    result = await adapter.retry_pipeline("run-002")
    assert result["status"] == "triggered"
    assert result["new_run_id"] == "run-retry-001"


async def test_cancel_pipeline(adapter: KubeflowAdapter) -> None:
    """Should cancel a running pipeline."""
    result = await adapter.cancel_pipeline("run-003")
    assert result["status"] == "cancelled"
    assert result["run_id"] == "run-003"


async def test_get_pipeline_logs(adapter: KubeflowAdapter) -> None:
    """Should return log lines for a run."""
    logs = await adapter.get_pipeline_logs("run-002")
    assert len(logs) >= 1
    assert any("ingest-daily-data" in line for line in logs)


async def test_check_source_availability(adapter: KubeflowAdapter) -> None:
    """Should return True when Kubeflow healthz responds 200."""
    # Patch _client for healthz endpoint (uses _host not _base_url)
    adapter._host = adapter._base_url  # type: ignore[assignment]
    available = await adapter.check_source_availability("kubeflow-cluster")
    assert available is True


async def test_get_pipeline_metadata(adapter: KubeflowAdapter) -> None:
    """Should return pipeline metadata."""
    meta = await adapter.get_pipeline_metadata("train-model-v2")
    assert meta["pipeline_name"] == "train-model-v2"
    assert meta["pipeline_id"] == "pipe-abc"


def test_status_mapping() -> None:
    """All Kubeflow states should map to a valid PipelineStatus."""
    for _kf_state, status in _KF_STATUS_MAP.items():
        assert isinstance(status, PipelineStatus)
