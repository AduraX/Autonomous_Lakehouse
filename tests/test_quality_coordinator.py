"""Tests for the Quality Coordinator — DQ failures creating remediation actions."""

import time
from collections.abc import AsyncGenerator

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from lakehouse.agents.iceberg_quality import CheckStatus, IcebergQualityAgent, QualityCheckResult
from lakehouse.agents.quality_coordinator import (
    _determine_actions,
    run_quality_checks_and_coordinate,
)
from lakehouse.models.actions import Action, ActionStatus, ActionType
from lakehouse.models.base import Base
from lakehouse.models.diagnoses import Diagnosis, FailureCategory
from lakehouse.models.events import PipelineEvent, PipelineStatus


@pytest.fixture
async def session() -> AsyncGenerator[AsyncSession, None]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        yield s
    await engine.dispose()


def _stale_table_metadata() -> dict:
    """Table metadata with a 48-hour-old snapshot (freshness fail)."""
    old_ts = int(time.time() * 1000) - (48 * 3600 * 1000)
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
                    "timestamp-ms": old_ts,
                    "summary": {"added-records": "1000", "total-records": "1000"},
                },
            ],
        }
    }


class StaleMockTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if "/tables/" in path:
            return httpx.Response(200, json=_stale_table_metadata())
        if path.endswith("/namespaces"):
            return httpx.Response(200, json={"namespaces": [["lakehouse"]]})
        if path.endswith("/tables"):
            return httpx.Response(
                200, json={"identifiers": [{"namespace": ["lakehouse"], "name": "orders"}]}
            )
        return httpx.Response(404)


def _stale_agent(monkeypatch: pytest.MonkeyPatch) -> IcebergQualityAgent:
    monkeypatch.setenv("ICEBERG_REST_CATALOG_URL", "http://iceberg.test:8181")
    from lakehouse.config import get_settings

    get_settings.cache_clear()

    ag = IcebergQualityAgent()

    def mock_client():
        return httpx.AsyncClient(
            transport=StaleMockTransport(),
            headers=ag._headers,
            base_url=ag._catalog_url,
        )

    ag._client = mock_client  # type: ignore[assignment]
    return ag


async def test_dq_failure_creates_event_and_diagnosis(
    session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Failed DQ checks should create a PipelineEvent and Diagnosis."""
    agent = _stale_agent(monkeypatch)

    results, _actions_count = await run_quality_checks_and_coordinate(
        agent,
        session,
        "lakehouse",
        "orders",
        max_age_hours=24.0,
    )
    await session.commit()

    # Should have at least one failed check (freshness)
    failed = [r for r in results if r.status == CheckStatus.FAILED]
    assert len(failed) >= 1

    # Should have created a pipeline event
    events_result = await session.execute(select(PipelineEvent))
    events = list(events_result.scalars().all())
    assert len(events) == 1
    assert events[0].pipeline_name == "dq_check:lakehouse.orders"
    assert events[0].status == PipelineStatus.FAILED

    # Should have created a diagnosis
    diag_result = await session.execute(select(Diagnosis))
    diagnoses = list(diag_result.scalars().all())
    assert len(diagnoses) == 1
    assert diagnoses[0].category == FailureCategory.DATA_QUALITY


async def test_dq_failure_proposes_actions(
    session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Failed DQ checks should propose remediation actions."""
    agent = _stale_agent(monkeypatch)

    _, actions_count = await run_quality_checks_and_coordinate(
        agent,
        session,
        "lakehouse",
        "orders",
        max_age_hours=24.0,
    )
    await session.commit()

    assert actions_count >= 1

    actions_result = await session.execute(select(Action))
    actions = list(actions_result.scalars().all())
    assert len(actions) >= 1

    action_types = {a.action_type for a in actions}
    assert ActionType.NOTIFY_OWNER in action_types

    # NOTIFY_OWNER should be auto-approved by policy
    notify = [a for a in actions if a.action_type == ActionType.NOTIFY_OWNER]
    assert notify[0].status == ActionStatus.APPROVED


async def test_passing_checks_create_no_actions(
    session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """All-passing checks should not create events or actions."""

    # Use fresh table metadata
    class FreshTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            now_ms = int(time.time() * 1000)
            return httpx.Response(
                200,
                json={
                    "metadata": {
                        "format-version": 2,
                        "current-schema": {
                            "schema-id": 0,
                            "fields": [{"id": 1, "name": "id", "type": "long", "required": True}],
                        },
                        "schemas": [
                            {
                                "schema-id": 0,
                                "fields": [
                                    {"id": 1, "name": "id", "type": "long", "required": True}
                                ],
                            }
                        ],
                        "snapshots": [
                            {
                                "snapshot-id": 1,
                                "timestamp-ms": now_ms - 1000,
                                "summary": {"added-records": "100", "total-records": "100"},
                            },
                            {
                                "snapshot-id": 2,
                                "timestamp-ms": now_ms,
                                "summary": {"added-records": "110", "total-records": "210"},
                            },
                        ],
                    }
                },
            )

    monkeypatch.setenv("ICEBERG_REST_CATALOG_URL", "http://iceberg.test:8181")
    from lakehouse.config import get_settings

    get_settings.cache_clear()

    ag = IcebergQualityAgent()
    ag._client = lambda: httpx.AsyncClient(  # type: ignore[assignment]
        transport=FreshTransport(),
        headers=ag._headers,
        base_url=ag._catalog_url,
    )

    _, actions_count = await run_quality_checks_and_coordinate(
        ag,
        session,
        "lakehouse",
        "clean_table",
        max_age_hours=24.0,
    )
    await session.commit()

    assert actions_count == 0

    events_result = await session.execute(select(PipelineEvent))
    assert len(list(events_result.scalars().all())) == 0


def test_determine_actions_schema_drift() -> None:
    """Schema drift should propose block_downstream + notify_owner."""
    failed = [
        QualityCheckResult(
            check_name="schema_drift",
            status=CheckStatus.FAILED,
            table="ns.t",
            message="drift detected",
        )
    ]
    actions = _determine_actions(failed, "ns.t")
    types = {a[0] for a in actions}
    assert ActionType.BLOCK_DOWNSTREAM in types
    assert ActionType.NOTIFY_OWNER in types


def test_determine_actions_freshness() -> None:
    """Freshness failure should propose notify_owner + retry_pipeline."""
    failed = [
        QualityCheckResult(
            check_name="freshness",
            status=CheckStatus.FAILED,
            table="ns.t",
            message="stale",
        )
    ]
    actions = _determine_actions(failed, "ns.t")
    types = {a[0] for a in actions}
    assert ActionType.NOTIFY_OWNER in types
    assert ActionType.RETRY_PIPELINE in types


def test_determine_actions_volume() -> None:
    """Volume anomaly should propose block_downstream + notify_owner."""
    failed = [
        QualityCheckResult(
            check_name="volume_anomaly",
            status=CheckStatus.FAILED,
            table="ns.t",
            message="anomaly",
        )
    ]
    actions = _determine_actions(failed, "ns.t")
    types = {a[0] for a in actions}
    assert ActionType.BLOCK_DOWNSTREAM in types
    assert ActionType.NOTIFY_OWNER in types


def test_determine_actions_deduplicates() -> None:
    """Multiple failures should not produce duplicate action types."""
    failed = [
        QualityCheckResult(
            check_name="schema_drift", status=CheckStatus.FAILED, table="t", message="a"
        ),
        QualityCheckResult(
            check_name="volume_anomaly", status=CheckStatus.FAILED, table="t", message="b"
        ),
    ]
    actions = _determine_actions(failed, "t")
    action_types = [a[0] for a in actions]
    # Should deduplicate BLOCK_DOWNSTREAM and NOTIFY_OWNER
    assert len(action_types) == len(set(action_types))
