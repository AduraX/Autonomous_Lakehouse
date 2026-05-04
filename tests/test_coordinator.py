"""Tests for the Coordinator Agent.

Two test layers:
  1. Unit tests — mock adapter + in-memory DB, test diagnosis logic and resilience
  2. API integration tests — HTTP client through the full FastAPI stack
"""

from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from lakehouse.adapters.base import PlatformAdapter
from lakehouse.agents.coordinator import CoordinatorAgent
from lakehouse.models.actions import Action, ActionStatus, ActionType
from lakehouse.models.base import Base
from lakehouse.models.diagnoses import Diagnosis, FailureCategory
from lakehouse.models.events import PipelineEvent, PipelineStatus, Platform

# ============================================================
# Unit test fixtures and helpers
# ============================================================


@pytest.fixture
async def unit_session() -> AsyncGenerator[AsyncSession, None]:
    """Fresh in-memory database for unit tests."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        yield s
    await engine.dispose()


def _mock_adapter(log_lines: list[str] | None = None) -> AsyncMock:
    """Create a mock platform adapter."""
    adapter = AsyncMock(spec=PlatformAdapter)
    adapter.get_pipeline_logs = AsyncMock(return_value=log_lines or [])
    adapter.get_pipeline_metadata = AsyncMock(
        return_value={
            "pipeline_name": "test-pipeline",
            "owner": "eng@company.com",
        }
    )
    return adapter


async def _insert_event(
    session: AsyncSession,
    status: PipelineStatus = PipelineStatus.FAILED,
    error_message: str = "Schema mismatch on column 'amount': expected DECIMAL, got STRING",
) -> PipelineEvent:
    """Insert a test pipeline event."""
    event = PipelineEvent(
        external_run_id="test-run-001",
        pipeline_name="ingest_sales",
        platform=Platform.MOCK,
        status=status,
        error_message=error_message,
        started_at=datetime(2026, 4, 28, 10, 0, tzinfo=UTC),
        finished_at=datetime(2026, 4, 28, 10, 15, tzinfo=UTC),
        duration_seconds=900.0,
    )
    session.add(event)
    await session.flush()
    await session.refresh(event)
    return event


# ============================================================
# Unit tests — Coordinator logic + error resilience
# ============================================================


async def test_unit_coordinate_failed_event(unit_session: AsyncSession) -> None:
    """Full unit workflow: failed event -> diagnosis -> actions persisted."""
    event = await _insert_event(unit_session)
    coordinator = CoordinatorAgent(adapter=_mock_adapter(), session=unit_session)

    result = await coordinator.coordinate(event.id)

    assert result.event_id == event.id
    assert len(result.findings) == 1
    assert result.findings[0].category == FailureCategory.SCHEMA_MISMATCH
    assert result.findings[0].confidence > 0.8
    assert len(result.proposed_actions) >= 1
    assert "schema_mismatch" in result.summary


async def test_unit_coordinate_skips_succeeded(unit_session: AsyncSession) -> None:
    """Should skip coordination for successful events."""
    event = await _insert_event(unit_session, status=PipelineStatus.SUCCEEDED)
    adapter = _mock_adapter()
    coordinator = CoordinatorAgent(adapter=adapter, session=unit_session)

    result = await coordinator.coordinate(event.id)

    assert len(result.findings) == 0
    assert "No coordination needed" in result.summary
    adapter.get_pipeline_logs.assert_not_called()


async def test_unit_coordinate_missing_event(unit_session: AsyncSession) -> None:
    """Should raise ValueError for non-existent event."""
    coordinator = CoordinatorAgent(adapter=_mock_adapter(), session=unit_session)
    with pytest.raises(ValueError, match="not found"):
        await coordinator.coordinate(99999)


async def test_unit_persists_diagnosis(unit_session: AsyncSession) -> None:
    """Diagnosis should be saved to the database."""
    event = await _insert_event(unit_session)
    coordinator = CoordinatorAgent(adapter=_mock_adapter(), session=unit_session)

    await coordinator.coordinate(event.id)
    await unit_session.commit()

    result = await unit_session.execute(select(Diagnosis).where(Diagnosis.event_id == event.id))
    diagnoses = list(result.scalars().all())
    assert len(diagnoses) == 1
    assert diagnoses[0].category == FailureCategory.SCHEMA_MISMATCH


async def test_unit_persists_actions_with_policy(unit_session: AsyncSession) -> None:
    """Actions should be persisted with correct policy-driven status."""
    event = await _insert_event(unit_session)
    coordinator = CoordinatorAgent(adapter=_mock_adapter(), session=unit_session)

    await coordinator.coordinate(event.id)
    await unit_session.commit()

    result = await unit_session.execute(select(Action))
    actions = list(result.scalars().all())
    assert len(actions) >= 1

    notify = [a for a in actions if a.action_type == ActionType.NOTIFY_OWNER]
    if notify:
        assert notify[0].status == ActionStatus.APPROVED  # auto-approved by policy

    block = [a for a in actions if a.action_type == ActionType.BLOCK_DOWNSTREAM]
    if block:
        assert block[0].status == ActionStatus.PROPOSED  # requires approval


# --- Error resilience ---


async def test_unit_survives_logs_failure(unit_session: AsyncSession) -> None:
    """Should still diagnose when get_pipeline_logs() fails."""
    event = await _insert_event(unit_session)
    adapter = _mock_adapter()
    adapter.get_pipeline_logs = AsyncMock(side_effect=ConnectionError("network timeout"))

    coordinator = CoordinatorAgent(adapter=adapter, session=unit_session)
    result = await coordinator.coordinate(event.id)

    assert len(result.findings) == 1
    assert result.findings[0].category == FailureCategory.SCHEMA_MISMATCH


async def test_unit_survives_metadata_failure(unit_session: AsyncSession) -> None:
    """Should still diagnose when get_pipeline_metadata() fails."""
    event = await _insert_event(unit_session)
    adapter = _mock_adapter()
    adapter.get_pipeline_metadata = AsyncMock(side_effect=TimeoutError("API timeout"))

    coordinator = CoordinatorAgent(adapter=adapter, session=unit_session)
    result = await coordinator.coordinate(event.id)

    assert len(result.findings) == 1
    assert len(result.proposed_actions) >= 1


async def test_unit_survives_both_failures(unit_session: AsyncSession) -> None:
    """Should produce a diagnosis even when both adapter calls fail."""
    event = await _insert_event(unit_session)
    adapter = _mock_adapter()
    adapter.get_pipeline_logs = AsyncMock(side_effect=Exception("broken"))
    adapter.get_pipeline_metadata = AsyncMock(side_effect=Exception("broken"))

    coordinator = CoordinatorAgent(adapter=adapter, session=unit_session)
    result = await coordinator.coordinate(event.id)

    assert len(result.findings) == 1
    assert result.findings[0].confidence > 0


# --- Diagnosis pattern coverage ---


async def test_unit_diagnose_timeout(unit_session: AsyncSession) -> None:
    """Should classify timeout errors."""
    event = await _insert_event(unit_session, error_message="Connection timeout after 300s")
    coordinator = CoordinatorAgent(adapter=_mock_adapter(), session=unit_session)
    result = await coordinator.coordinate(event.id)
    assert result.findings[0].category == FailureCategory.TIMEOUT


async def test_unit_diagnose_permission(unit_session: AsyncSession) -> None:
    """Should classify permission errors."""
    event = await _insert_event(unit_session, error_message="Permission denied: lacks Reader role")
    coordinator = CoordinatorAgent(adapter=_mock_adapter(), session=unit_session)
    result = await coordinator.coordinate(event.id)
    assert result.findings[0].category == FailureCategory.PERMISSION_DENIED


async def test_unit_diagnose_oom(unit_session: AsyncSession) -> None:
    """Should classify OOM as resource exhaustion."""
    event = await _insert_event(unit_session, error_message="Out of memory: executor exceeded 8GB")
    coordinator = CoordinatorAgent(adapter=_mock_adapter(), session=unit_session)
    result = await coordinator.coordinate(event.id)
    assert result.findings[0].category == FailureCategory.RESOURCE_EXHAUSTION


async def test_unit_diagnose_unknown(unit_session: AsyncSession) -> None:
    """Unrecognized errors should fall back to UNKNOWN with low confidence."""
    event = await _insert_event(unit_session, error_message="Something completely unexpected")
    coordinator = CoordinatorAgent(adapter=_mock_adapter(), session=unit_session)
    result = await coordinator.coordinate(event.id)
    assert result.findings[0].category == FailureCategory.UNKNOWN
    assert result.findings[0].confidence < 0.5


async def test_unit_diagnose_from_logs_fallback(unit_session: AsyncSession) -> None:
    """Should fall back to log analysis when error_message has no match."""
    event = await _insert_event(unit_session, error_message="generic error")
    adapter = _mock_adapter(log_lines=["[ERROR] out of memory on executor node 3"])
    coordinator = CoordinatorAgent(adapter=adapter, session=unit_session)
    result = await coordinator.coordinate(event.id)
    assert result.findings[0].category == FailureCategory.RESOURCE_EXHAUSTION


async def test_unit_diagnose_duplicate_key(unit_session: AsyncSession) -> None:
    """Should classify duplicate key errors as data quality."""
    event = await _insert_event(unit_session, error_message="Duplicate key violation on 'order_id'")
    coordinator = CoordinatorAgent(adapter=_mock_adapter(), session=unit_session)
    result = await coordinator.coordinate(event.id)
    assert result.findings[0].category == FailureCategory.DATA_QUALITY


async def test_unit_diagnose_upstream_dependency(unit_session: AsyncSession) -> None:
    """Should classify upstream dependency errors."""
    event = await _insert_event(
        unit_session, error_message="Upstream pipeline 'load_raw' not completed"
    )
    coordinator = CoordinatorAgent(adapter=_mock_adapter(), session=unit_session)
    result = await coordinator.coordinate(event.id)
    assert result.findings[0].category == FailureCategory.DEPENDENCY_FAILURE


# ============================================================
# API integration tests (via HTTP client)
# ============================================================


async def _create_failed_event(
    client: AsyncClient, error_message: str, error_code: str | None = None
) -> int:
    """Helper: create a failed pipeline event and return its ID."""
    resp = await client.post(
        "/api/v1/events",
        json={
            "external_run_id": f"coord-test-{error_message[:10]}",
            "pipeline_name": "ingest_sales_data",
            "platform": "mock",
            "status": "failed",
            "error_message": error_message,
            "error_code": error_code,
        },
    )
    assert resp.status_code == 201
    return resp.json()["id"]


async def test_coordinate_schema_mismatch(client: AsyncClient) -> None:
    """Coordinator should diagnose schema_mismatch and propose block + notify."""
    event_id = await _create_failed_event(client, "Column 'customer_id' not found in source")

    resp = await client.post("/api/v1/coordinate", json={"event_id": event_id})
    assert resp.status_code == 200

    result = resp.json()
    assert result["event_id"] == event_id
    assert len(result["findings"]) == 1
    assert result["findings"][0]["category"] == "schema_mismatch"
    assert result["findings"][0]["confidence"] >= 0.8

    action_types = [a["action_type"] for a in result["proposed_actions"]]
    assert "block_downstream" in action_types
    assert "notify_owner" in action_types


async def test_coordinate_timeout(client: AsyncClient) -> None:
    """Coordinator should diagnose timeout and propose retry + scale."""
    event_id = await _create_failed_event(
        client, "Connection timeout to source database after 300s"
    )

    resp = await client.post("/api/v1/coordinate", json={"event_id": event_id})
    assert resp.status_code == 200

    result = resp.json()
    assert result["findings"][0]["category"] == "timeout"

    action_types = [a["action_type"] for a in result["proposed_actions"]]
    assert "retry_pipeline" in action_types


async def test_coordinate_unknown_error(client: AsyncClient) -> None:
    """Coordinator should still produce findings even for unrecognized errors.

    Note: With MockAdapter, logs always contain schema-related keywords,
    so the second-pass log matching may classify even unknown errors.
    This test verifies the coordination flow completes and produces actions.
    """
    event_id = await _create_failed_event(client, "Some completely unexpected error occurred")

    resp = await client.post("/api/v1/coordinate", json={"event_id": event_id})
    assert resp.status_code == 200

    result = resp.json()
    assert len(result["findings"]) == 1
    assert len(result["proposed_actions"]) > 0
    # Always proposes at least a notify_owner action
    action_types = [a["action_type"] for a in result["proposed_actions"]]
    assert "notify_owner" in action_types


async def test_coordinate_succeeded_event_skips(client: AsyncClient) -> None:
    """Coordinator should skip coordination for non-failed events."""
    resp = await client.post(
        "/api/v1/events",
        json={
            "external_run_id": "coord-test-success",
            "pipeline_name": "ingest_sales_data",
            "platform": "mock",
            "status": "succeeded",
        },
    )
    event_id = resp.json()["id"]

    resp = await client.post("/api/v1/coordinate", json={"event_id": event_id})
    assert resp.status_code == 200

    result = resp.json()
    assert result["findings"] == []
    assert result["proposed_actions"] == []


async def test_coordinate_nonexistent_event(client: AsyncClient) -> None:
    """Coordinator should return 404 for missing events."""
    resp = await client.post("/api/v1/coordinate", json={"event_id": 99999})
    assert resp.status_code == 404


async def test_coordinate_policy_applied(client: AsyncClient) -> None:
    """Proposed actions should have correct policy classifications."""
    event_id = await _create_failed_event(client, "Column 'amount' not found")

    resp = await client.post("/api/v1/coordinate", json={"event_id": event_id})
    result = resp.json()

    for action in result["proposed_actions"]:
        if action["action_type"] == "notify_owner":
            assert action["policy"] == "auto_approved"
        elif action["action_type"] == "block_downstream":
            assert action["policy"] == "requires_approval"
