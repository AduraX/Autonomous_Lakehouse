"""Tests for the Azure Data Factory adapter."""

import pytest

from lakehouse.adapters.adf import AzureDataFactoryAdapter
from lakehouse.models.events import Platform


@pytest.fixture
def adapter(monkeypatch: pytest.MonkeyPatch) -> AzureDataFactoryAdapter:
    monkeypatch.setenv("ADF_SUBSCRIPTION_ID", "test-sub")
    monkeypatch.setenv("ADF_RESOURCE_GROUP", "test-rg")
    monkeypatch.setenv("ADF_FACTORY_NAME", "test-factory")
    # Clear cached settings so monkeypatched env is picked up
    from lakehouse.config import get_settings

    get_settings.cache_clear()
    adapter = AzureDataFactoryAdapter()
    yield adapter
    get_settings.cache_clear()


async def test_fetch_recent_events(adapter: AzureDataFactoryAdapter) -> None:
    events = await adapter.fetch_recent_events(limit=5)
    assert len(events) == 5
    for event in events:
        assert event.platform == Platform.ADF
        assert event.external_run_id.startswith("adf-run-")


async def test_retry_pipeline(adapter: AzureDataFactoryAdapter) -> None:
    result = await adapter.retry_pipeline("adf-run-123")
    assert result["status"] == "triggered"
    assert "adf-run-123" in result["new_run_id"]


async def test_cancel_pipeline(adapter: AzureDataFactoryAdapter) -> None:
    result = await adapter.cancel_pipeline("adf-run-123")
    assert result["status"] == "cancelled"


async def test_get_pipeline_logs(adapter: AzureDataFactoryAdapter) -> None:
    logs = await adapter.get_pipeline_logs("adf-run-123")
    assert len(logs) > 0
    assert any("adf-run-123" in line for line in logs)


async def test_check_source_availability(adapter: AzureDataFactoryAdapter) -> None:
    available = await adapter.check_source_availability("my-source")
    assert available is True


async def test_get_pipeline_metadata(adapter: AzureDataFactoryAdapter) -> None:
    meta = await adapter.get_pipeline_metadata("ingest_sales_data")
    assert meta["pipeline_name"] == "ingest_sales_data"
    assert meta["factory_name"] == "test-factory"
