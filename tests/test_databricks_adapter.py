"""Tests for the Databricks adapter."""

import json

import httpx
import pytest

from lakehouse.adapters.databricks import DatabricksAdapter
from lakehouse.models.events import PipelineStatus, Platform


def _make_transport(handler):
    """Build an httpx.MockTransport from an async handler function."""
    return httpx.MockTransport(handler)


def _json_response(data: dict, status_code: int = 200) -> httpx.Response:
    return httpx.Response(status_code, json=data)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def adapter(monkeypatch: pytest.MonkeyPatch) -> DatabricksAdapter:
    monkeypatch.setenv("DATABRICKS_WORKSPACE_URL", "https://test.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test-token")
    from lakehouse.config import get_settings

    get_settings.cache_clear()
    adapter = DatabricksAdapter()
    yield adapter
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_fetch_recent_events(adapter: DatabricksAdapter) -> None:
    """fetch_recent_events maps Databricks runs to PipelineEventCreate."""

    async def handler(request: httpx.Request) -> httpx.Response:
        assert "/api/2.1/jobs/runs/list" in str(request.url)
        return _json_response(
            {
                "runs": [
                    {
                        "run_id": 101,
                        "run_name": "etl_daily",
                        "job_id": 10,
                        "state": {
                            "life_cycle_state": "TERMINATED",
                            "result_state": "SUCCESS",
                        },
                        "start_time": 1714300000000,
                        "end_time": 1714301800000,
                        "execution_duration": 1800000,
                        "cluster_instance": {"cluster_id": "abc-123"},
                        "run_page_url": "https://test.cloud.databricks.com/run/101",
                    },
                    {
                        "run_id": 102,
                        "run_name": "etl_hourly",
                        "job_id": 11,
                        "state": {
                            "life_cycle_state": "TERMINATED",
                            "result_state": "FAILED",
                            "state_message": "Task failed with exception",
                        },
                        "start_time": 1714300000000,
                        "end_time": 1714301000000,
                        "execution_duration": 1000000,
                        "cluster_instance": {"cluster_id": "abc-456"},
                    },
                    {
                        "run_id": 103,
                        "run_name": "streaming_job",
                        "job_id": 12,
                        "state": {
                            "life_cycle_state": "RUNNING",
                        },
                        "start_time": 1714300000000,
                        "cluster_instance": {},
                    },
                ],
            }
        )

    adapter._client = lambda: httpx.AsyncClient(transport=_make_transport(handler))

    events = await adapter.fetch_recent_events(limit=10)
    assert len(events) == 3

    # First run — succeeded
    assert events[0].platform == Platform.DATABRICKS
    assert events[0].external_run_id == "101"
    assert events[0].pipeline_name == "etl_daily"
    assert events[0].status == PipelineStatus.SUCCEEDED
    assert events[0].duration_seconds == 1800.0
    assert events[0].error_message is None

    # Second run — failed
    assert events[1].status == PipelineStatus.FAILED
    assert events[1].error_message == "Task failed with exception"
    assert events[1].error_code == "FAILED"

    # Third run — running
    assert events[2].status == PipelineStatus.RUNNING
    assert events[2].finished_at is None


async def test_retry_pipeline(adapter: DatabricksAdapter) -> None:
    """retry_pipeline fetches original run then triggers run-now."""
    call_log: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "/api/2.1/jobs/runs/get" in url and "output" not in url:
            call_log.append("get")
            return _json_response({"job_id": 42, "run_id": 101})
        if "/api/2.1/jobs/run-now" in url:
            call_log.append("run-now")
            body = json.loads(request.content)
            assert body["job_id"] == 42
            return _json_response({"run_id": 201})
        return _json_response({"error": "unexpected"}, 404)

    adapter._client = lambda: httpx.AsyncClient(transport=_make_transport(handler))

    result = await adapter.retry_pipeline("101")
    assert result["status"] == "triggered"
    assert result["new_run_id"] == "201"
    assert call_log == ["get", "run-now"]


async def test_cancel_pipeline(adapter: DatabricksAdapter) -> None:
    """cancel_pipeline sends a cancel request and returns status."""

    async def handler(request: httpx.Request) -> httpx.Response:
        assert "/api/2.1/jobs/runs/cancel" in str(request.url)
        body = json.loads(request.content)
        assert body["run_id"] == 101
        return _json_response({})

    adapter._client = lambda: httpx.AsyncClient(transport=_make_transport(handler))

    result = await adapter.cancel_pipeline("101")
    assert result["status"] == "cancelled"
    assert result["run_id"] == "101"


async def test_get_pipeline_logs(adapter: DatabricksAdapter) -> None:
    """get_pipeline_logs returns structured log lines from run output."""

    async def handler(request: httpx.Request) -> httpx.Response:
        assert "/api/2.1/jobs/runs/get-output" in str(request.url)
        return _json_response(
            {
                "notebook_output": {
                    "result": "Processed 1000 rows",
                    "truncated": False,
                },
                "metadata": {
                    "run_name": "etl_daily",
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "state_message": "",
                    },
                },
                "log": "Starting job...\nJob complete.",
            }
        )

    adapter._client = lambda: httpx.AsyncClient(transport=_make_transport(handler))

    logs = await adapter.get_pipeline_logs("101")
    assert len(logs) > 0
    assert any("etl_daily" in line for line in logs)
    assert any("Processed 1000 rows" in line for line in logs)
    assert any("Starting job" in line for line in logs)


async def test_check_source_availability(adapter: DatabricksAdapter) -> None:
    """check_source_availability returns True when workspace is reachable."""

    async def handler(request: httpx.Request) -> httpx.Response:
        assert "/api/2.0/clusters/list" in str(request.url)
        return _json_response({"clusters": []})

    adapter._client = lambda: httpx.AsyncClient(transport=_make_transport(handler))

    available = await adapter.check_source_availability("databricks-workspace")
    assert available is True


async def test_check_source_availability_unreachable(adapter: DatabricksAdapter) -> None:
    """check_source_availability returns False on connection error."""

    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Connection refused")

    adapter._client = lambda: httpx.AsyncClient(transport=_make_transport(handler))

    available = await adapter.check_source_availability("databricks-workspace")
    assert available is False


async def test_get_pipeline_metadata(adapter: DatabricksAdapter) -> None:
    """get_pipeline_metadata returns job config for a named pipeline."""

    async def handler(request: httpx.Request) -> httpx.Response:
        assert "/api/2.1/jobs/list" in str(request.url)
        return _json_response(
            {
                "jobs": [
                    {
                        "job_id": 42,
                        "creator_user_name": "admin@company.com",
                        "settings": {
                            "name": "etl_daily",
                            "schedule": {
                                "quartz_cron_expression": "0 0 8 * * ?",
                            },
                            "max_concurrent_runs": 1,
                            "timeout_seconds": 3600,
                        },
                    }
                ],
            }
        )

    adapter._client = lambda: httpx.AsyncClient(transport=_make_transport(handler))

    meta = await adapter.get_pipeline_metadata("etl_daily")
    assert meta["pipeline_name"] == "etl_daily"
    assert meta["job_id"] == "42"
    assert meta["platform"] == "databricks"
    assert meta["creator_user_name"] == "admin@company.com"
