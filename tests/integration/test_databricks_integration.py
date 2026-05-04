"""Integration tests for the Databricks adapter.

These tests call the real Databricks REST API and require valid credentials.
They are automatically skipped when the required environment variables are
not set, or when the DatabricksAdapter has not yet been implemented.

Run with:
    pytest tests/integration/test_databricks_integration.py -v
"""

import pytest

from lakehouse.schemas.events import PipelineEventCreate

from .conftest import requires_databricks

pytestmark = [pytest.mark.integration, requires_databricks]


def _import_adapter():
    """Import DatabricksAdapter or skip if not implemented."""
    try:
        from lakehouse.adapters.databricks import DatabricksAdapter

        return DatabricksAdapter
    except ImportError:
        pytest.skip("DatabricksAdapter not yet implemented")


@pytest.mark.timeout(60)
async def test_fetch_recent_events_live(databricks_adapter) -> None:
    """Fetch real pipeline run events from Databricks and verify their structure."""
    events = await databricks_adapter.fetch_recent_events(limit=10)

    assert isinstance(events, list), f"Expected list, got {type(events)}"
    for event in events:
        assert isinstance(event, PipelineEventCreate)
        assert isinstance(event.external_run_id, str)
        assert len(event.external_run_id) > 0
        assert isinstance(event.pipeline_name, str)
        assert len(event.pipeline_name) > 0
        assert event.status is not None
        if event.duration_seconds is not None:
            assert event.duration_seconds >= 0


@pytest.mark.timeout(30)
async def test_check_source_availability_live(databricks_adapter) -> None:
    """Verify the Databricks workspace is reachable."""
    result = await databricks_adapter.check_source_availability("databricks-workspace")

    assert isinstance(result, bool), f"Expected bool, got {type(result)}"
    assert result is True, (
        "Databricks source availability check returned False. "
        "Verify that credentials and workspace URL are correct."
    )


@pytest.mark.timeout(60)
async def test_get_pipeline_metadata_live(databricks_adapter) -> None:
    """Fetch metadata for a real Databricks pipeline/job."""
    events = await databricks_adapter.fetch_recent_events(limit=1)
    if not events:
        pytest.skip("No recent Databricks pipeline runs found to look up metadata for")

    pipeline_name = events[0].pipeline_name
    metadata = await databricks_adapter.get_pipeline_metadata(pipeline_name)

    assert isinstance(metadata, dict), f"Expected dict, got {type(metadata)}"
    assert "pipeline_name" in metadata


@pytest.mark.timeout(60)
async def test_get_pipeline_logs_live(databricks_adapter) -> None:
    """Fetch logs for a recent Databricks pipeline run."""
    events = await databricks_adapter.fetch_recent_events(limit=5)
    if not events:
        pytest.skip("No recent Databricks pipeline runs found to fetch logs for")

    run_id = events[0].external_run_id
    logs = await databricks_adapter.get_pipeline_logs(run_id)

    assert isinstance(logs, list), f"Expected list, got {type(logs)}"
    for entry in logs:
        assert isinstance(entry, str), f"Expected str log entry, got {type(entry)}"
