"""Integration tests for the Kubeflow Pipelines adapter.

These tests call the real Kubeflow Pipelines v2 REST API and require
valid credentials. They are automatically skipped when the required
environment variables are not set.

Run with:
    pytest tests/integration/test_kubeflow_integration.py -v
"""

import pytest

from lakehouse.adapters.kubeflow import KubeflowAdapter
from lakehouse.models.events import Platform
from lakehouse.schemas.events import PipelineEventCreate

from .conftest import requires_kubeflow

pytestmark = [pytest.mark.integration, requires_kubeflow]


@pytest.mark.timeout(60)
async def test_fetch_recent_events_live(kubeflow_adapter: KubeflowAdapter) -> None:
    """Fetch real pipeline run events from Kubeflow and verify their structure."""
    events = await kubeflow_adapter.fetch_recent_events(limit=10)

    assert isinstance(events, list), f"Expected list, got {type(events)}"
    for event in events:
        assert isinstance(event, PipelineEventCreate)
        assert event.platform == Platform.KUBEFLOW
        assert isinstance(event.external_run_id, str)
        assert len(event.external_run_id) > 0
        assert isinstance(event.pipeline_name, str)
        assert len(event.pipeline_name) > 0
        assert event.status is not None
        if event.duration_seconds is not None:
            assert event.duration_seconds >= 0
        # Kubeflow adapter always sets metadata_json
        if event.metadata_json is not None:
            assert isinstance(event.metadata_json, str)


@pytest.mark.timeout(30)
async def test_check_source_availability_live(
    kubeflow_adapter: KubeflowAdapter,
) -> None:
    """Verify the Kubeflow API healthz endpoint is reachable."""
    result = await kubeflow_adapter.check_source_availability("kubeflow-cluster")

    assert isinstance(result, bool), f"Expected bool, got {type(result)}"
    assert result is True, (
        "Kubeflow source availability check returned False. "
        "Verify that KUBEFLOW_HOST is reachable and KUBEFLOW_SA_TOKEN is valid."
    )


@pytest.mark.timeout(60)
async def test_get_pipeline_metadata_live(
    kubeflow_adapter: KubeflowAdapter,
) -> None:
    """Fetch metadata for a real Kubeflow pipeline."""
    events = await kubeflow_adapter.fetch_recent_events(limit=1)
    if not events:
        pytest.skip("No recent Kubeflow pipeline runs found to look up metadata for")

    pipeline_name = events[0].pipeline_name
    metadata = await kubeflow_adapter.get_pipeline_metadata(pipeline_name)

    assert isinstance(metadata, dict), f"Expected dict, got {type(metadata)}"
    assert "pipeline_name" in metadata
    # Kubeflow metadata should include namespace and platform fields
    if "error" not in metadata:
        assert "namespace" in metadata
        assert metadata["platform"] == "kubeflow"


@pytest.mark.timeout(60)
async def test_get_pipeline_logs_live(
    kubeflow_adapter: KubeflowAdapter,
) -> None:
    """Fetch logs for a recent Kubeflow pipeline run."""
    events = await kubeflow_adapter.fetch_recent_events(limit=5)
    if not events:
        pytest.skip("No recent Kubeflow pipeline runs found to fetch logs for")

    run_id = events[0].external_run_id
    logs = await kubeflow_adapter.get_pipeline_logs(run_id)

    assert isinstance(logs, list), f"Expected list, got {type(logs)}"
    assert len(logs) > 0, "Expected at least one log line for a real pipeline run"
    for entry in logs:
        assert isinstance(entry, str), f"Expected str log entry, got {type(entry)}"
