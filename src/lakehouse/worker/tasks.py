"""Background worker tasks via arq (Redis-backed).

Three task types:
  1. coordinate_event — run the coordinator on a failed event
  2. execute_action — execute an approved action with retry
  3. poll_events — poll the platform adapter for new events and coordinate failures

Usage:
  # Start the worker
  arq lakehouse.worker.tasks.WorkerSettings

  # Enqueue from API
  from arq import ArqRedis
  await redis.enqueue_job("coordinate_event", event_id=42)
"""

from datetime import UTC, datetime
from typing import Any

from arq import ArqRedis
from arq.connections import RedisSettings
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from lakehouse.adapters import get_adapter
from lakehouse.agents.action_executor import ActionExecutor
from lakehouse.agents.coordinator import CoordinatorAgent
from lakehouse.config import get_settings
from lakehouse.logging import get_logger, setup_logging
from lakehouse.models.events import PipelineEvent, PipelineStatus

logger = get_logger(__name__)

# Module-level session factory (initialized in startup)
_session_factory: async_sessionmaker[AsyncSession] | None = None


async def startup(ctx: dict[str, Any]) -> None:
    """Worker startup — initialize database connection and logging."""
    global _session_factory
    settings = get_settings()
    setup_logging(settings.app_log_level)

    engine = create_async_engine(
        settings.database_url,
        pool_size=10,
        max_overflow=5,
        pool_pre_ping=True,
    )
    _session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    ctx["adapter"] = get_adapter()
    logger.info("worker_started", platform=settings.platform_adapter.value)


async def shutdown(ctx: dict[str, Any]) -> None:
    """Worker shutdown — clean up resources."""
    logger.info("worker_shutting_down")


def _get_session() -> AsyncSession:
    """Get a database session from the module-level factory."""
    if _session_factory is None:
        raise RuntimeError("Worker not initialized — call startup() first")
    return _session_factory()


# ============================================================
# Task: coordinate_event
# ============================================================


async def coordinate_event(ctx: dict[str, Any], event_id: int) -> dict[str, Any]:
    """Run the coordinator agent on a pipeline event.

    Called when a new failed event is ingested, or manually triggered.
    """
    adapter = ctx["adapter"]

    async with _get_session() as session:
        coordinator = CoordinatorAgent(adapter=adapter, session=session)

        try:
            result = await coordinator.coordinate(event_id)
            await session.commit()

            logger.info(
                "task_coordinate_completed",
                event_id=event_id,
                findings=len(result.findings),
                actions=len(result.proposed_actions),
            )
            return {
                "event_id": event_id,
                "findings": len(result.findings),
                "proposed_actions": len(result.proposed_actions),
                "summary": result.summary,
            }

        except Exception as e:
            await session.rollback()
            logger.error("task_coordinate_failed", event_id=event_id, error=str(e))
            raise


# ============================================================
# Task: execute_action
# ============================================================


async def execute_action(ctx: dict[str, Any], action_id: int) -> dict[str, Any]:
    """Execute an approved remediation action.

    Called when an action is approved (manually or auto-approved by policy).
    """
    adapter = ctx["adapter"]

    async with _get_session() as session:
        executor = ActionExecutor(adapter=adapter, session=session)

        try:
            result = await executor.execute(action_id)
            await session.commit()

            logger.info(
                "task_execute_completed",
                action_id=action_id,
                status=result.status.value,
                attempts=result.attempts,
            )
            return {
                "action_id": action_id,
                "status": result.status.value,
                "attempts": result.attempts,
                "duration_seconds": round(result.duration_seconds, 2),
                "error": result.error,
            }

        except Exception as e:
            await session.rollback()
            logger.error("task_execute_failed", action_id=action_id, error=str(e))
            raise


# ============================================================
# Task: poll_events
# ============================================================


async def poll_events(ctx: dict[str, Any], limit: int = 50) -> dict[str, Any]:
    """Poll the platform adapter for new events, ingest them, and coordinate failures.

    This is the main event loop — called on a schedule or manually.
    """
    adapter = ctx["adapter"]
    redis: ArqRedis = ctx["redis"]

    async with _get_session() as session:
        try:
            events = await adapter.fetch_recent_events(limit=limit)
            ingested = 0
            coordinated = 0

            for event_data in events:
                # Insert event
                event = PipelineEvent(**event_data.model_dump())
                session.add(event)
                await session.flush()
                await session.refresh(event)
                ingested += 1

                # Enqueue coordination for failures
                status_val = (
                    event.status.value if isinstance(event.status, PipelineStatus) else event.status
                )
                if status_val in (
                    PipelineStatus.FAILED.value,
                    PipelineStatus.TIMED_OUT.value,
                ):
                    await redis.enqueue_job("coordinate_event", event_id=event.id)
                    coordinated += 1

            await session.commit()

            logger.info(
                "task_poll_completed",
                ingested=ingested,
                coordinated=coordinated,
            )
            return {
                "ingested": ingested,
                "coordinated": coordinated,
                "polled_at": datetime.now(UTC).isoformat(),
            }

        except Exception as e:
            await session.rollback()
            logger.error("task_poll_failed", error=str(e))
            raise


# ============================================================
# Worker settings
# ============================================================


def _redis_settings() -> RedisSettings:
    """Parse Redis URL into arq RedisSettings."""
    settings = get_settings()
    url = settings.redis_url
    # arq expects host/port, not a URL
    # redis://localhost:6379/0 → host=localhost, port=6379, database=0
    if url.startswith("redis://"):
        url = url[len("redis://") :]
    parts = url.split("/")
    host_port = parts[0]
    database = int(parts[1]) if len(parts) > 1 else 0
    host, _, port_str = host_port.partition(":")
    port = int(port_str) if port_str else 6379

    return RedisSettings(host=host or "localhost", port=port, database=database)


class WorkerSettings:
    """arq worker configuration.

    Start with: arq lakehouse.worker.tasks.WorkerSettings
    """

    functions = [coordinate_event, execute_action, poll_events]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = _redis_settings()
    max_jobs = 10
    job_timeout = 300  # 5 minutes
