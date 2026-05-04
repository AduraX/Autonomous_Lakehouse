"""Integration tests for the Microsoft Fabric adapter.

These tests call the real Fabric REST API and require valid Azure credentials.
They are automatically skipped when the required environment variables are
not set.

Run with:
    pytest tests/integration/test_fabric_integration.py -v
"""

import pytest

from lakehouse.adapters.fabric import FabricAdapter
from lakehouse.models.events import Platform
from lakehouse.schemas.events import PipelineEventCreate

from .conftest import requires_fabric

pytestmark = [pytest.mark.integration, requires_fabric]


@pytest.mark.timeout(60)
async def test_fetch_recent_events_live(fabric_adapter: FabricAdapter) -> None:
    """Fetch real pipeline run events from Fabric and verify their structure."""
    events = await fabric_adapter.fetch_recent_events(limit=10)

    assert isinstance(events, list), f"Expected list, got {type(events)}"
    for event in events:
        assert isinstance(event, PipelineEventCreate)
        assert event.platform == Platform.FABRIC
        assert isinstance(event.external_run_id, str)
        assert len(event.external_run_id) > 0
        assert isinstance(event.pipeline_name, str)
        assert len(event.pipeline_name) > 0
        assert event.status is not None
        if event.duration_seconds is not None:
            assert event.duration_seconds >= 0


@pytest.mark.timeout(30)
async def test_check_source_availability_live(
    fabric_adapter: FabricAdapter,
) -> None:
    """Verify the Fabric workspace is reachable."""
    result = await fabric_adapter.check_source_availability("fabric-workspace")

    assert isinstance(result, bool), f"Expected bool, got {type(result)}"
    assert result is True, (
        "Fabric source availability check returned False. "
        "Verify that credentials and workspace ID are correct."
    )


@pytest.mark.timeout(60)
async def test_get_pipeline_metadata_live(
    fabric_adapter: FabricAdapter,
) -> None:
    """Fetch metadata for a real Fabric pipeline."""
    events = await fabric_adapter.fetch_recent_events(limit=1)
    if not events:
        pytest.skip("No recent Fabric pipeline runs found to look up metadata for")

    pipeline_name = events[0].pipeline_name
    metadata = await fabric_adapter.get_pipeline_metadata(pipeline_name)

    assert isinstance(metadata, dict), f"Expected dict, got {type(metadata)}"
    assert "pipeline_name" in metadata
    assert metadata["pipeline_name"] == pipeline_name


@pytest.mark.timeout(60)
async def test_get_pipeline_logs_live(
    fabric_adapter: FabricAdapter,
) -> None:
    """Fetch logs for a recent Fabric pipeline run."""
    events = await fabric_adapter.fetch_recent_events(limit=5)
    if not events:
        pytest.skip("No recent Fabric pipeline runs found to fetch logs for")

    run_id = events[0].external_run_id
    logs = await fabric_adapter.get_pipeline_logs(run_id)

    assert isinstance(logs, list), f"Expected list, got {type(logs)}"
    for entry in logs:
        assert isinstance(entry, str), f"Expected str log entry, got {type(entry)}"
