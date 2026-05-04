"""Tests for the Microsoft Fabric adapter."""

import pytest

from lakehouse.adapters.fabric import FabricAdapter
from lakehouse.models.events import Platform


@pytest.fixture
def adapter(monkeypatch: pytest.MonkeyPatch) -> FabricAdapter:
    monkeypatch.setenv("FABRIC_WORKSPACE_ID", "test-workspace-id")
    from lakehouse.config import get_settings

    get_settings.cache_clear()
    adapter = FabricAdapter()
    yield adapter
    get_settings.cache_clear()


async def test_fetch_recent_events(adapter: FabricAdapter) -> None:
    events = await adapter.fetch_recent_events(limit=5)
    assert len(events) == 5
    for event in events:
        assert event.platform == Platform.FABRIC
        assert event.external_run_id.startswith("fabric-run-")


async def test_retry_pipeline(adapter: FabricAdapter) -> None:
    result = await adapter.retry_pipeline("fabric-run-123")
    assert result["status"] == "triggered"
    assert "fabric-run-123" in result["new_run_id"]


async def test_cancel_pipeline(adapter: FabricAdapter) -> None:
    result = await adapter.cancel_pipeline("fabric-run-123")
    assert result["status"] == "cancelled"


async def test_get_pipeline_logs(adapter: FabricAdapter) -> None:
    logs = await adapter.get_pipeline_logs("fabric-run-123")
    assert len(logs) > 0
    assert any("fabric-run-123" in line for line in logs)


async def test_check_source_availability(adapter: FabricAdapter) -> None:
    available = await adapter.check_source_availability("my-lakehouse")
    assert available is True


async def test_get_pipeline_metadata(adapter: FabricAdapter) -> None:
    meta = await adapter.get_pipeline_metadata("lakehouse_refresh")
    assert meta["pipeline_name"] == "lakehouse_refresh"
    assert meta["workspace_id"] == "test-workspace-id"
