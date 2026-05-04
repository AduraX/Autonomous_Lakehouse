"""Tests for the Action Executor — retry logic, backoff, and audit logging."""

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from lakehouse.adapters.base import PlatformAdapter
from lakehouse.agents.action_executor import ActionExecutor
from lakehouse.models.actions import Action, ActionStatus, ActionType
from lakehouse.models.audit import AuditLog
from lakehouse.models.base import Base
from lakehouse.models.diagnoses import Diagnosis, FailureCategory
from lakehouse.models.events import PipelineEvent, PipelineStatus, Platform


@pytest.fixture
async def session() -> AsyncGenerator[AsyncSession, None]:
    """Fresh in-memory database."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        yield s
    await engine.dispose()


async def _seed_action(
    session: AsyncSession,
    action_type: ActionType = ActionType.RETRY_PIPELINE,
    status: ActionStatus = ActionStatus.APPROVED,
) -> tuple[PipelineEvent, Diagnosis, Action]:
    """Create event → diagnosis → action chain for testing."""
    event = PipelineEvent(
        external_run_id="exec-test-001",
        pipeline_name="ingest_sales",
        platform=Platform.MOCK,
        status=PipelineStatus.FAILED,
        error_message="Schema mismatch",
    )
    session.add(event)
    await session.flush()

    diagnosis = Diagnosis(
        event_id=event.id,
        category=FailureCategory.SCHEMA_MISMATCH,
        confidence=0.9,
        summary="Schema mismatch detected",
        agent_name="coordinator",
    )
    session.add(diagnosis)
    await session.flush()

    action = Action(
        diagnosis_id=diagnosis.id,
        action_type=action_type,
        description="Retry the pipeline",
        status=status,
        executed_by="coordinator_agent",
    )
    session.add(action)
    await session.flush()
    await session.refresh(action)

    return event, diagnosis, action


def _mock_adapter(succeed: bool = True, fail_count: int = 0) -> AsyncMock:
    """Create a mock adapter that optionally fails N times before succeeding."""
    adapter = AsyncMock(spec=PlatformAdapter)
    call_count = 0

    async def retry_side_effect(run_id: str) -> dict[str, str]:
        nonlocal call_count
        call_count += 1
        if call_count <= fail_count:
            raise ConnectionError(f"Network error (attempt {call_count})")
        if not succeed:
            raise ConnectionError("Permanent network failure")
        return {"status": "triggered", "new_run_id": f"retry-{run_id}"}

    adapter.retry_pipeline = AsyncMock(side_effect=retry_side_effect)
    adapter.get_pipeline_metadata = AsyncMock(
        return_value={"pipeline_name": "test", "owner": "eng@co.com"}
    )
    return adapter


# --- Execution Tests ---


async def test_execute_succeeds(session: AsyncSession) -> None:
    """Should execute an approved action and mark it SUCCEEDED."""
    _, _, action = await _seed_action(session)
    adapter = _mock_adapter(succeed=True)
    executor = ActionExecutor(adapter=adapter, session=session, backoff_base=0.01)

    result = await executor.execute(action.id)

    assert result.status == ActionStatus.SUCCEEDED
    assert result.attempts == 1
    assert result.error is None

    await session.refresh(action)
    assert action.status == ActionStatus.SUCCEEDED
    assert action.result_json is not None


async def test_execute_retries_on_transient_failure(session: AsyncSession) -> None:
    """Should retry on transient errors and succeed after failures."""
    _, _, action = await _seed_action(session)
    adapter = _mock_adapter(succeed=True, fail_count=2)  # fail twice, then succeed
    executor = ActionExecutor(
        adapter=adapter,
        session=session,
        max_retries=3,
        backoff_base=0.01,
    )

    result = await executor.execute(action.id)

    assert result.status == ActionStatus.SUCCEEDED
    assert result.attempts == 3  # 2 failures + 1 success


async def test_execute_fails_after_max_retries(session: AsyncSession) -> None:
    """Should fail after exhausting all retry attempts."""
    _, _, action = await _seed_action(session)
    adapter = _mock_adapter(succeed=False)
    executor = ActionExecutor(
        adapter=adapter,
        session=session,
        max_retries=3,
        backoff_base=0.01,
    )

    result = await executor.execute(action.id)

    assert result.status == ActionStatus.FAILED
    assert result.attempts == 3
    assert "Failed after 3 attempts" in (result.error or "")

    await session.refresh(action)
    assert action.status == ActionStatus.FAILED
    assert action.error_message is not None


async def test_execute_fails_immediately_on_permanent_error(session: AsyncSession) -> None:
    """Should not retry permanent errors (ValueError, PermissionError)."""
    _, _, action = await _seed_action(session)
    adapter = AsyncMock(spec=PlatformAdapter)
    adapter.retry_pipeline = AsyncMock(side_effect=PermissionError("Access denied"))
    executor = ActionExecutor(adapter=adapter, session=session, backoff_base=0.01)

    result = await executor.execute(action.id)

    assert result.status == ActionStatus.FAILED
    assert result.attempts == 1  # no retries
    assert "Permanent error" in (result.error or "")


async def test_execute_rejects_non_approved(session: AsyncSession) -> None:
    """Should reject actions not in APPROVED status."""
    _, _, action = await _seed_action(session, status=ActionStatus.PROPOSED)
    adapter = _mock_adapter()
    executor = ActionExecutor(adapter=adapter, session=session)

    with pytest.raises(ValueError, match="must be 'approved'"):
        await executor.execute(action.id)


async def test_execute_rejects_missing_action(session: AsyncSession) -> None:
    """Should raise for non-existent action ID."""
    adapter = _mock_adapter()
    executor = ActionExecutor(adapter=adapter, session=session)

    with pytest.raises(ValueError, match="not found"):
        await executor.execute(99999)


async def test_execute_creates_audit_log(session: AsyncSession) -> None:
    """Should write an audit log entry after execution."""
    _, _, action = await _seed_action(session)
    adapter = _mock_adapter(succeed=True)
    executor = ActionExecutor(adapter=adapter, session=session, backoff_base=0.01)

    await executor.execute(action.id)
    await session.commit()

    result = await session.execute(select(AuditLog).where(AuditLog.resource_id == action.id))
    audits = list(result.scalars().all())
    assert len(audits) == 1
    assert audits[0].event_type == "action_executed"
    assert "succeeded" in audits[0].summary.lower()


async def test_execute_notify_owner(session: AsyncSession) -> None:
    """Should handle NOTIFY_OWNER action type."""
    _, _, action = await _seed_action(session, action_type=ActionType.NOTIFY_OWNER)
    adapter = _mock_adapter()
    executor = ActionExecutor(adapter=adapter, session=session, backoff_base=0.01)

    result = await executor.execute(action.id)
    assert result.status == ActionStatus.SUCCEEDED
