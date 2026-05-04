"""Tests for the coordination API endpoint (POST /api/v1/coordinate)."""

from collections.abc import AsyncGenerator

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
from lakehouse.models.events import PipelineEvent, PipelineStatus, Platform


@pytest.fixture
async def db_and_client() -> AsyncGenerator[tuple[AsyncSession, AsyncClient], None]:
    """Set up a test database and HTTP client with mock adapter."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    app = create_app()

    async def override_db() -> AsyncGenerator[AsyncSession, None]:
        async with factory() as s:
            try:
                yield s
                await s.commit()
            except Exception:
                await s.rollback()
                raise

    app.dependency_overrides[get_db] = override_db

    # Seed a failed event
    async with factory() as seed_session:
        event = PipelineEvent(
            external_run_id="coord-test-001",
            pipeline_name="ingest_orders",
            platform=Platform.MOCK,
            status=PipelineStatus.FAILED,
            error_message="Connection timeout to source database after 300s",
            error_code="TIMEOUT_5001",
        )
        seed_session.add(event)
        await seed_session.commit()

    transport = ASGITransport(app=app)
    async with (
        AsyncClient(transport=transport, base_url="http://test") as client,
        factory() as session,
    ):
        yield session, client

    await engine.dispose()


async def test_coordinate_endpoint(db_and_client: tuple[AsyncSession, AsyncClient]) -> None:
    """POST /api/v1/coordinate should return findings and proposed actions."""
    _, client = db_and_client

    resp = await client.post("/api/v1/coordinate", json={"event_id": 1})
    assert resp.status_code == 200

    data = resp.json()
    assert data["event_id"] == 1
    assert len(data["findings"]) == 1
    assert data["findings"][0]["category"] == "timeout"
    assert data["findings"][0]["confidence"] > 0.8
    assert len(data["proposed_actions"]) >= 1
    assert "coordinated_at" in data


async def test_coordinate_not_found(db_and_client: tuple[AsyncSession, AsyncClient]) -> None:
    """Should return 404 for non-existent event."""
    _, client = db_and_client

    resp = await client.post("/api/v1/coordinate", json={"event_id": 99999})
    assert resp.status_code == 404


async def test_coordinate_response_includes_policy(
    db_and_client: tuple[AsyncSession, AsyncClient],
) -> None:
    """Proposed actions should include policy classification."""
    _, client = db_and_client

    resp = await client.post("/api/v1/coordinate", json={"event_id": 1})
    data = resp.json()

    for action in data["proposed_actions"]:
        assert "policy" in action
        assert action["policy"] in ("auto_approved", "requires_approval", "forbidden")
