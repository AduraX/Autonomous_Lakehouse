"""Tests for the Iceberg Data Quality Agent.

Uses httpx mock transport to simulate Iceberg REST Catalog responses.
"""

import httpx
import pytest

from lakehouse.agents.iceberg_quality import CheckStatus, IcebergQualityAgent


def _table_metadata(
    schemas: list[dict] | None = None,
    snapshots: list[dict] | None = None,
) -> dict:
    """Build a mock Iceberg table metadata response."""
    if schemas is None:
        schemas = [
            {
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "type": "long", "required": True},
                    {"id": 2, "name": "name", "type": "string", "required": False},
                    {"id": 3, "name": "amount", "type": "decimal(18,2)", "required": False},
                ],
            }
        ]
    if snapshots is None:
        import time

        now_ms = int(time.time() * 1000)
        snapshots = [
            {
                "snapshot-id": 1,
                "timestamp-ms": now_ms - 3600_000,  # 1 hour ago
                "summary": {"added-records": "1000", "total-records": "1000"},
            },
            {
                "snapshot-id": 2,
                "timestamp-ms": now_ms,  # now
                "summary": {"added-records": "1100", "total-records": "2100"},
            },
        ]
    return {
        "metadata": {
            "format-version": 2,
            "current-schema": schemas[-1] if schemas else {},
            "schemas": schemas,
            "snapshots": snapshots,
        }
    }


class MockCatalogTransport(httpx.AsyncBaseTransport):
    """Mock Iceberg REST Catalog transport."""

    def __init__(self, table_meta: dict | None = None) -> None:
        self._table_meta = table_meta or _table_metadata()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path

        if path == "/v1/namespaces":
            return httpx.Response(200, json={"namespaces": [["lakehouse"]]})
        if path == "/v1/namespaces/lakehouse/tables":
            return httpx.Response(
                200,
                json={
                    "identifiers": [
                        {"namespace": ["lakehouse"], "name": "sales"},
                        {"namespace": ["lakehouse"], "name": "customers"},
                    ]
                },
            )
        if "/tables/" in path and request.method == "GET":
            return httpx.Response(200, json=self._table_meta)

        return httpx.Response(404)


@pytest.fixture
def agent(monkeypatch: pytest.MonkeyPatch) -> IcebergQualityAgent:
    """Create an IcebergQualityAgent with mock transport."""
    monkeypatch.setenv("ICEBERG_REST_CATALOG_URL", "http://iceberg.test:8181")

    from lakehouse.config import get_settings

    get_settings.cache_clear()

    ag = IcebergQualityAgent()

    def mock_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=MockCatalogTransport(),
            headers=ag._headers,
            base_url=ag._catalog_url,
        )

    ag._client = mock_client  # type: ignore[assignment]
    return ag


def _agent_with_meta(monkeypatch: pytest.MonkeyPatch, meta: dict) -> IcebergQualityAgent:
    """Helper to create an agent with specific table metadata."""
    monkeypatch.setenv("ICEBERG_REST_CATALOG_URL", "http://iceberg.test:8181")

    from lakehouse.config import get_settings

    get_settings.cache_clear()

    ag = IcebergQualityAgent()

    def mock_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=MockCatalogTransport(table_meta=meta),
            headers=ag._headers,
            base_url=ag._catalog_url,
        )

    ag._client = mock_client  # type: ignore[assignment]
    return ag


async def test_list_namespaces(agent: IcebergQualityAgent) -> None:
    """Should list catalog namespaces."""
    ns = await agent.list_namespaces()
    assert "lakehouse" in ns


async def test_list_tables(agent: IcebergQualityAgent) -> None:
    """Should list tables in a namespace."""
    tables = await agent.list_tables("lakehouse")
    assert len(tables) == 2
    assert any("sales" in t for t in tables)


async def test_schema_drift_passes_no_drift(agent: IcebergQualityAgent) -> None:
    """Should pass when no schema drift is detected."""
    result = await agent.check_schema_drift("lakehouse", "sales")
    assert result.status == CheckStatus.PASSED


async def test_schema_drift_fails_with_expected_columns(agent: IcebergQualityAgent) -> None:
    """Should fail when current columns don't match expected."""
    result = await agent.check_schema_drift(
        "lakehouse",
        "sales",
        expected_columns=["id", "name", "amount", "category"],  # 'category' missing
    )
    assert result.status == CheckStatus.FAILED
    assert "removed" in result.message


async def test_schema_drift_warns_on_evolution(monkeypatch: pytest.MonkeyPatch) -> None:
    """Should warn when schema has evolved between versions."""
    meta = _table_metadata(
        schemas=[
            {
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "type": "long", "required": True},
                    {"id": 2, "name": "name", "type": "string", "required": False},
                ],
            },
            {
                "schema-id": 1,
                "fields": [
                    {"id": 1, "name": "id", "type": "long", "required": True},
                    {"id": 2, "name": "name", "type": "string", "required": False},
                    {"id": 3, "name": "email", "type": "string", "required": False},
                ],
            },
        ]
    )
    ag = _agent_with_meta(monkeypatch, meta)
    result = await ag.check_schema_drift("lakehouse", "sales")
    assert result.status == CheckStatus.WARNING
    assert "email" in str(result.details.get("added_columns"))


async def test_freshness_passes(agent: IcebergQualityAgent) -> None:
    """Should pass when table was updated recently."""
    result = await agent.check_freshness("lakehouse", "sales", max_age_hours=24.0)
    assert result.status == CheckStatus.PASSED


async def test_freshness_fails_stale(monkeypatch: pytest.MonkeyPatch) -> None:
    """Should fail when table snapshot is older than threshold."""
    import time

    old_ts = int(time.time() * 1000) - (48 * 3600 * 1000)  # 48 hours ago
    meta = _table_metadata(
        snapshots=[
            {"snapshot-id": 1, "timestamp-ms": old_ts, "summary": {}},
        ]
    )
    ag = _agent_with_meta(monkeypatch, meta)
    result = await ag.check_freshness("lakehouse", "sales", max_age_hours=24.0)
    assert result.status == CheckStatus.FAILED
    assert "stale" in result.message.lower()


async def test_volume_anomaly_passes(agent: IcebergQualityAgent) -> None:
    """Should pass when volume change is within thresholds."""
    result = await agent.check_snapshot_volume("lakehouse", "sales")
    assert result.status == CheckStatus.PASSED


async def test_volume_anomaly_fails_spike(monkeypatch: pytest.MonkeyPatch) -> None:
    """Should fail when volume change exceeds threshold."""
    import time

    now_ms = int(time.time() * 1000)
    meta = _table_metadata(
        snapshots=[
            {
                "snapshot-id": 1,
                "timestamp-ms": now_ms - 3600_000,
                "summary": {"added-records": "100", "total-records": "100"},
            },
            {
                "snapshot-id": 2,
                "timestamp-ms": now_ms,
                "summary": {"added-records": "10000", "total-records": "10100"},
            },
        ]
    )
    ag = _agent_with_meta(monkeypatch, meta)
    result = await ag.check_snapshot_volume("lakehouse", "sales")
    assert result.status == CheckStatus.FAILED
    assert "anomaly" in result.message.lower()


async def test_run_all_checks(agent: IcebergQualityAgent) -> None:
    """Should return results for all three checks."""
    results = await agent.run_all_checks("lakehouse", "sales")
    assert len(results) == 3
    check_names = {r.check_name for r in results}
    assert check_names == {"schema_drift", "freshness", "volume_anomaly"}
