"""Tests for the Cost Optimization Agent — baselines, anomaly detection, recommendations."""

from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from lakehouse.agents.cost_optimizer import AnomalyType, CostOptimizer
from lakehouse.models.actions import Action, ActionType
from lakehouse.models.base import Base
from lakehouse.models.cost import CostBaseline
from lakehouse.models.events import PipelineEvent, PipelineStatus, Platform


@pytest.fixture
async def session() -> AsyncGenerator[AsyncSession, None]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        yield s
    await engine.dispose()


def _make_event(
    pipeline: str = "ingest_sales",
    duration: float = 120.0,
    status: PipelineStatus = PipelineStatus.SUCCEEDED,
) -> PipelineEvent:
    now = datetime.now(UTC)
    return PipelineEvent(
        external_run_id=f"cost-test-{duration}-{now.timestamp()}",
        pipeline_name=pipeline,
        platform=Platform.MOCK,
        status=status,
        duration_seconds=duration,
        started_at=now - timedelta(seconds=duration),
        finished_at=now,
    )


async def _seed_baseline(
    session: AsyncSession,
    optimizer: CostOptimizer,
    pipeline: str = "ingest_sales",
    durations: list[float] | None = None,
) -> CostBaseline:
    """Insert events to build a baseline."""
    if durations is None:
        durations = [100, 110, 105, 95, 108, 102, 97, 103]
    for d in durations:
        event = _make_event(pipeline=pipeline, duration=d)
        session.add(event)
        await session.flush()
        await optimizer.update_baseline(event)
    await session.flush()
    baseline = await optimizer.get_baseline(pipeline)
    assert baseline is not None
    return baseline


# --- Baseline Tests ---


async def test_baseline_created_on_first_event(session: AsyncSession) -> None:
    """First event should create a new baseline."""
    optimizer = CostOptimizer(session=session)
    event = _make_event(duration=120.0)
    session.add(event)
    await session.flush()

    baseline = await optimizer.update_baseline(event)

    assert baseline.pipeline_name == "ingest_sales"
    assert baseline.total_runs == 1
    assert baseline.avg_duration == 120.0
    assert baseline.min_duration == 120.0
    assert baseline.max_duration == 120.0


async def test_baseline_updates_on_subsequent_events(session: AsyncSession) -> None:
    """Subsequent events should update running statistics."""
    optimizer = CostOptimizer(session=session)

    for d in [100, 200, 150]:
        event = _make_event(duration=d)
        session.add(event)
        await session.flush()
        await optimizer.update_baseline(event)

    baseline = await optimizer.get_baseline("ingest_sales")
    assert baseline is not None
    assert baseline.total_runs == 3
    assert baseline.avg_duration == pytest.approx(150.0, abs=1)
    assert baseline.min_duration == 100.0
    assert baseline.max_duration == 200.0
    assert baseline.stddev_duration > 0


async def test_baseline_tracks_failures(session: AsyncSession) -> None:
    """Failures should be counted in the baseline."""
    optimizer = CostOptimizer(session=session)

    for status in [PipelineStatus.SUCCEEDED, PipelineStatus.FAILED, PipelineStatus.SUCCEEDED]:
        event = _make_event(status=status)
        session.add(event)
        await session.flush()
        await optimizer.update_baseline(event)

    baseline = await optimizer.get_baseline("ingest_sales")
    assert baseline is not None
    assert baseline.total_successes == 2
    assert baseline.total_failures == 1
    assert baseline.failure_rate == pytest.approx(1 / 3, abs=0.01)


# --- Anomaly Detection Tests ---


async def test_no_anomaly_within_threshold(session: AsyncSession) -> None:
    """Normal duration should not trigger an anomaly."""
    optimizer = CostOptimizer(session=session)
    baseline = await _seed_baseline(session, optimizer)

    # A run within 2 stddev should be fine
    normal_duration = baseline.avg_duration + baseline.stddev_duration * 1.5
    finding = optimizer.detect_duration_anomaly(baseline, normal_duration)
    assert finding is None


async def test_anomaly_detected_above_threshold(session: AsyncSession) -> None:
    """Duration way above baseline should trigger an anomaly."""
    optimizer = CostOptimizer(session=session)
    baseline = await _seed_baseline(session, optimizer)

    # A run at 5x stddev above mean
    spike_duration = baseline.avg_duration + baseline.stddev_duration * 5
    finding = optimizer.detect_duration_anomaly(baseline, spike_duration)

    assert finding is not None
    assert finding.anomaly_type == AnomalyType.DURATION_SPIKE
    assert finding.severity in ("medium", "high")
    assert "spike" in finding.message.lower()
    assert len(finding.recommendations) > 0


async def test_no_anomaly_with_few_runs(session: AsyncSession) -> None:
    """Should not flag anomalies with fewer than MIN_RUNS_FOR_ANOMALY runs."""
    optimizer = CostOptimizer(session=session)
    # Only 3 runs — below threshold
    baseline = await _seed_baseline(session, optimizer, durations=[100, 110, 105])

    finding = optimizer.detect_duration_anomaly(baseline, 500.0)
    assert finding is None  # Not enough data to judge


async def test_high_failure_rate_detected(session: AsyncSession) -> None:
    """High failure rate should be flagged."""
    optimizer = CostOptimizer(session=session)
    # 4 failures out of 6 runs = 67%
    durations_statuses = [
        (100, PipelineStatus.FAILED),
        (110, PipelineStatus.FAILED),
        (105, PipelineStatus.SUCCEEDED),
        (95, PipelineStatus.FAILED),
        (108, PipelineStatus.FAILED),
        (102, PipelineStatus.SUCCEEDED),
    ]
    for d, s in durations_statuses:
        event = _make_event(duration=d, status=s)
        session.add(event)
        await session.flush()
        await optimizer.update_baseline(event)

    baseline = await optimizer.get_baseline("ingest_sales")
    finding = optimizer.detect_failure_rate_anomaly(baseline)

    assert finding is not None
    assert finding.anomaly_type == AnomalyType.HIGH_FAILURE_RATE
    assert "failure rate" in finding.message.lower()


async def test_normal_failure_rate_passes(session: AsyncSession) -> None:
    """Low failure rate should not be flagged."""
    optimizer = CostOptimizer(session=session)
    # 1 failure out of 6 runs = 17%
    for i in range(6):
        status = PipelineStatus.FAILED if i == 0 else PipelineStatus.SUCCEEDED
        event = _make_event(duration=100.0 + i, status=status)
        session.add(event)
        await session.flush()
        await optimizer.update_baseline(event)

    baseline = await optimizer.get_baseline("ingest_sales")
    finding = optimizer.detect_failure_rate_anomaly(baseline)
    assert finding is None


# --- Coordination Tests ---


async def test_analyze_and_coordinate_creates_actions(session: AsyncSession) -> None:
    """Anomalies should create diagnoses and proposed actions."""
    optimizer = CostOptimizer(session=session)
    await _seed_baseline(session, optimizer)

    baseline = await optimizer.get_baseline("ingest_sales")
    spike = baseline.avg_duration + baseline.stddev_duration * 6

    event = _make_event(duration=spike)
    session.add(event)
    await session.flush()
    await session.refresh(event)

    findings, actions_count = await optimizer.analyze_and_coordinate(event)
    await session.commit()

    assert len(findings) >= 1
    assert actions_count >= 1

    # Check actions were persisted
    result = await session.execute(select(Action))
    actions = list(result.scalars().all())
    assert len(actions) >= 1
    action_types = {a.action_type for a in actions}
    assert ActionType.NOTIFY_OWNER in action_types


async def test_analyze_normal_event_no_actions(session: AsyncSession) -> None:
    """Normal events should not produce anomaly actions."""
    optimizer = CostOptimizer(session=session)
    await _seed_baseline(session, optimizer)

    event = _make_event(duration=105.0)  # Within normal range
    session.add(event)
    await session.flush()
    await session.refresh(event)

    findings, actions_count = await optimizer.analyze_and_coordinate(event)
    assert len(findings) == 0
    assert actions_count == 0


async def test_consecutive_anomalies_tracked(session: AsyncSession) -> None:
    """Consecutive anomalies should increment the counter."""
    optimizer = CostOptimizer(session=session)
    # Use a larger baseline for more stable stddev
    await _seed_baseline(
        session,
        optimizer,
        durations=[100, 100, 100, 100, 100, 100, 100, 100, 100, 100],
    )

    baseline = await optimizer.get_baseline("ingest_sales")
    # Use a massive spike that remains anomalous even after baseline shift
    spike = 10000.0

    # Two consecutive spikes
    for _ in range(2):
        event = _make_event(duration=spike)
        session.add(event)
        await session.flush()
        await optimizer.analyze_event(event)

    baseline = await optimizer.get_baseline("ingest_sales")
    assert baseline.consecutive_anomalies >= 2


# --- API Helper Tests ---


async def test_get_top_cost_pipelines(session: AsyncSession) -> None:
    """Should return pipelines ordered by total cost."""
    optimizer = CostOptimizer(session=session)

    # Create baselines for two pipelines
    await _seed_baseline(session, optimizer, pipeline="cheap_pipeline", durations=[10, 12, 11])
    await _seed_baseline(
        session, optimizer, pipeline="expensive_pipeline", durations=[500, 600, 550]
    )

    top = await optimizer.get_top_cost_pipelines(limit=2)
    assert len(top) == 2
    assert top[0].pipeline_name == "expensive_pipeline"
