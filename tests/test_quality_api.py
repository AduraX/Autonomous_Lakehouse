"""Tests for the Data Quality API endpoints.

Uses monkeypatched IcebergQualityAgent with mock transport.
"""

from collections.abc import AsyncGenerator

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from lakehouse.api.app import create_app
from lakehouse.api.dependencies import get_db
from lakehouse.models.base import Base


def _table_metadata_fresh() -> dict:
    """Mock table metadata — fresh, healthy table."""
    import time

    now_ms = int(time.time() * 1000)
    return {
        "metadata": {
            "format-version": 2,
            "current-schema": {
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "type": "long", "required": True},
                    {"id": 2, "name": "name", "type": "string", "required": False},
                ],
            },
            "schemas": [
                {
                    "schema-id": 0,
                    "fields": [
                        {"id": 1, "name": "id", "type": "long", "required": True},
                        {"id": 2, "name": "name", "type": "string", "required": False},
                    ],
                }
            ],
            "snapshots": [
                {
                    "snapshot-id": 1,
                    "timestamp-ms": now_ms - 3600_000,
                    "summary": {"added-records": "1000", "total-records": "1000"},
                },
                {
                    "snapshot-id": 2,
                    "timestamp-ms": now_ms,
                    "summary": {"added-records": "1100", "total-records": "2100"},
                },
            ],
        }
    }


class MockCatalogTransport(httpx.AsyncBaseTransport):
    """Mock Iceberg REST Catalog for API tests."""

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
                    ]
                },
            )
        if "/tables/" in path:
            return httpx.Response(200, json=_table_metadata_fresh())
        return httpx.Response(404)


@pytest.fixture
async def quality_client(monkeypatch: pytest.MonkeyPatch) -> AsyncGenerator[AsyncClient, None]:
    """HTTP client with Iceberg configured and mocked."""
    monkeypatch.setenv("ICEBERG_REST_CATALOG_URL", "http://iceberg.test:8181")

    from lakehouse.config import get_settings

    get_settings.cache_clear()

    # Patch the IcebergQualityAgent to use mock transport
    import lakehouse.agents.iceberg_quality as iq_mod

    original_init = iq_mod.IcebergQualityAgent.__init__

    def patched_init(self):
        original_init(self)

    def patched_client(self):
        return httpx.AsyncClient(
            transport=MockCatalogTransport(),
            headers=self._headers,
            base_url=self._catalog_url,
        )

    monkeypatch.setattr(iq_mod.IcebergQualityAgent, "_client", patched_client)

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    app = create_app()

    async def override_db():
        async with factory() as s:
            try:
                yield s
                await s.commit()
            except Exception:
                await s.rollback()
                raise

    app.dependency_overrides[get_db] = override_db

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c

    await engine.dispose()


async def test_list_namespaces(quality_client: AsyncClient) -> None:
    """Should return namespaces from Iceberg catalog."""
    resp = await quality_client.get("/api/v1/quality/namespaces")
    assert resp.status_code == 200
    assert "lakehouse" in resp.json()["namespaces"]


async def test_list_tables(quality_client: AsyncClient) -> None:
    """Should return tables in a namespace."""
    resp = await quality_client.get("/api/v1/quality/namespaces/lakehouse/tables")
    assert resp.status_code == 200
    assert any("sales" in t for t in resp.json()["tables"])


async def test_check_table_all_pass(quality_client: AsyncClient) -> None:
    """Should return all checks passing for a healthy table."""
    resp = await quality_client.post(
        "/api/v1/quality/check",
        json={
            "namespace": "lakehouse",
            "table": "sales",
            "max_age_hours": 24.0,
        },
    )
    assert resp.status_code == 200

    data = resp.json()
    assert data["table"] == "lakehouse.sales"
    assert data["passed"] >= 1
    assert data["failed"] == 0
    assert data["actions_proposed"] == 0
    assert len(data["checks"]) == 3


async def test_check_table_schema_drift_fails(quality_client: AsyncClient) -> None:
    """Should detect schema drift when expected columns don't match."""
    resp = await quality_client.post(
        "/api/v1/quality/check",
        json={
            "namespace": "lakehouse",
            "table": "sales",
            "expected_columns": ["id", "name", "amount", "category"],
        },
    )
    assert resp.status_code == 200

    data = resp.json()
    assert data["failed"] >= 1
    assert data["actions_proposed"] >= 1  # Should propose block + notify

    # Check that a schema_drift check failed
    schema_checks = [c for c in data["checks"] if c["check_name"] == "schema_drift"]
    assert schema_checks[0]["status"] == "failed"


async def test_scan_namespace(quality_client: AsyncClient) -> None:
    """Should scan all tables in a namespace."""
    resp = await quality_client.post(
        "/api/v1/quality/scan",
        json={
            "namespace": "lakehouse",
            "max_age_hours": 24.0,
        },
    )
    assert resp.status_code == 200

    data = resp.json()
    assert data["namespace"] == "lakehouse"
    assert data["tables_scanned"] >= 1
    assert data["total_checks"] >= 3
    assert "tables" in data
    assert "scanned_at" in data


async def test_quality_not_configured() -> None:
    """Should return 503 when Iceberg is not configured."""
    from lakehouse.config import get_settings

    get_settings.cache_clear()

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    app = create_app()

    async def override_db():
        async with factory() as s:
            yield s

    app.dependency_overrides[get_db] = override_db

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/v1/quality/check",
            json={
                "namespace": "lakehouse",
                "table": "sales",
            },
        )
        assert resp.status_code == 503

    await engine.dispose()
