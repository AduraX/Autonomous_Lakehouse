"""Integration tests for the Azure Data Factory adapter.

These tests call the real ADF REST API and require valid Azure credentials.
They are automatically skipped when the required environment variables are
not set.

Run with:
    pytest tests/integration/test_adf_integration.py -v
"""

import pytest

from lakehouse.adapters.adf import AzureDataFactoryAdapter
from lakehouse.models.events import Platform
from lakehouse.schemas.events import PipelineEventCreate

from .conftest import requires_adf

pytestmark = [pytest.mark.integration, requires_adf]


@pytest.mark.timeout(60)
async def test_fetch_recent_events_live(adf_adapter: AzureDataFactoryAdapter) -> None:
    """Fetch real pipeline run events from ADF and verify their structure."""
    events = await adf_adapter.fetch_recent_events(limit=10)

    assert isinstance(events, list), f"Expected list, got {type(events)}"
    for event in events:
        assert isinstance(event, PipelineEventCreate)
        assert event.platform == Platform.ADF
        assert isinstance(event.external_run_id, str)
        assert len(event.external_run_id) > 0
        assert isinstance(event.pipeline_name, str)
        assert len(event.pipeline_name) > 0
        assert event.status is not None
        if event.duration_seconds is not None:
            assert event.duration_seconds >= 0


@pytest.mark.timeout(30)
async def test_check_source_availability_live(
    adf_adapter: AzureDataFactoryAdapter,
) -> None:
    """Verify the ADF service is reachable."""
    result = await adf_adapter.check_source_availability("adf-factory")

    assert isinstance(result, bool), f"Expected bool, got {type(result)}"
    # If credentials are valid, the factory should be reachable
    assert result is True, (
        "ADF source availability check returned False. "
        "Verify that credentials and factory name are correct."
    )


@pytest.mark.timeout(60)
async def test_get_pipeline_metadata_live(
    adf_adapter: AzureDataFactoryAdapter,
) -> None:
    """Fetch metadata for a real ADF pipeline."""
    # First get events to find a real pipeline name
    events = await adf_adapter.fetch_recent_events(limit=1)
    if not events:
        pytest.skip("No recent ADF pipeline runs found to look up metadata for")

    pipeline_name = events[0].pipeline_name
    metadata = await adf_adapter.get_pipeline_metadata(pipeline_name)

    assert isinstance(metadata, dict), f"Expected dict, got {type(metadata)}"
    assert "pipeline_name" in metadata
    assert metadata["pipeline_name"] == pipeline_name


@pytest.mark.timeout(60)
async def test_get_pipeline_logs_live(
    adf_adapter: AzureDataFactoryAdapter,
) -> None:
    """Fetch logs for a recent ADF pipeline run."""
    events = await adf_adapter.fetch_recent_events(limit=5)
    if not events:
        pytest.skip("No recent ADF pipeline runs found to fetch logs for")

    run_id = events[0].external_run_id
    logs = await adf_adapter.get_pipeline_logs(run_id)

    assert isinstance(logs, list), f"Expected list, got {type(logs)}"
    for entry in logs:
        assert isinstance(entry, str), f"Expected str log entry, got {type(entry)}"
