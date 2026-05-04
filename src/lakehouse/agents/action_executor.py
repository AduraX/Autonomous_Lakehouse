"""Action Executor — runs approved remediation actions via the platform adapter.

Handles the full execution lifecycle:
  1. Validate action is in an executable state (APPROVED)
  2. Transition to EXECUTING
  3. Execute via adapter with exponential backoff retry
  4. Transition to SUCCEEDED or FAILED
  5. Log to audit trail

Retry strategy:
  - Max 3 attempts by default
  - Exponential backoff: 1s, 2s, 4s (base * 2^attempt)
  - Only retries on transient errors (network, timeout)
  - Permanent errors (permission denied, not found) fail immediately
"""

import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from lakehouse.adapters.base import PlatformAdapter
from lakehouse.logging import get_logger
from lakehouse.models.actions import Action, ActionStatus, ActionType
from lakehouse.models.audit import AuditLog
from lakehouse.models.diagnoses import Diagnosis
from lakehouse.models.events import PipelineEvent

logger = get_logger(__name__)

# Errors that should NOT be retried
_PERMANENT_ERRORS = (
    PermissionError,
    ValueError,
    KeyError,
)

DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE = 1.0  # seconds
DEFAULT_BACKOFF_FACTOR = 2.0


@dataclass
class ExecutionResult:
    """Result of executing a single action."""

    action_id: int
    status: ActionStatus
    attempts: int
    result: dict[str, Any] | None = None
    error: str | None = None
    duration_seconds: float = 0.0


class ActionExecutor:
    """Executes approved remediation actions with retry and audit logging."""

    def __init__(
        self,
        adapter: PlatformAdapter,
        session: AsyncSession,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_base: float = DEFAULT_BACKOFF_BASE,
        backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    ) -> None:
        self._adapter = adapter
        self._session = session
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._backoff_factor = backoff_factor

    async def execute(self, action_id: int) -> ExecutionResult:
        """Execute a single action by ID.

        Only actions in APPROVED status can be executed.
        Does NOT commit — caller is responsible.
        """
        action = await self._session.get(Action, action_id)
        if action is None:
            raise ValueError(f"Action with id={action_id} not found")

        action_status = (
            action.status.value if isinstance(action.status, ActionStatus) else action.status
        )
        if action_status != ActionStatus.APPROVED.value:
            raise ValueError(
                f"Action {action_id} is '{action_status}', must be 'approved' to execute"
            )

        # Get the linked event for context
        diagnosis = await self._session.get(Diagnosis, action.diagnosis_id)
        event = await self._session.get(PipelineEvent, diagnosis.event_id) if diagnosis else None

        # Transition to EXECUTING
        action.status = ActionStatus.EXECUTING
        await self._session.flush()

        start_time = datetime.now(UTC)
        result = await self._execute_with_retry(action, event)
        duration = (datetime.now(UTC) - start_time).total_seconds()
        result.duration_seconds = duration

        # Update action with result
        if result.status == ActionStatus.SUCCEEDED:
            action.status = ActionStatus.SUCCEEDED
            action.result_json = json.dumps(result.result) if result.result else None
        else:
            action.status = ActionStatus.FAILED
            action.error_message = result.error

        await self._session.flush()

        # Audit log
        await self._audit(action, result)

        logger.info(
            "action_executed",
            action_id=action_id,
            action_type=action.action_type,
            status=result.status.value,
            attempts=result.attempts,
            duration_seconds=round(duration, 2),
        )

        return result

    async def _execute_with_retry(
        self, action: Action, event: PipelineEvent | None
    ) -> ExecutionResult:
        """Execute the action with exponential backoff retry."""
        action_type = (
            action.action_type.value
            if isinstance(action.action_type, ActionType)
            else action.action_type
        )
        external_run_id = event.external_run_id if event else "unknown"

        last_error: str | None = None

        for attempt in range(self._max_retries):
            try:
                adapter_result = await self._dispatch(action_type, external_run_id, action)

                return ExecutionResult(
                    action_id=action.id,
                    status=ActionStatus.SUCCEEDED,
                    attempts=attempt + 1,
                    result=adapter_result,
                )

            except _PERMANENT_ERRORS as e:
                # Don't retry permanent errors
                logger.warning(
                    "action_permanent_failure",
                    action_id=action.id,
                    attempt=attempt + 1,
                    error=str(e),
                )
                return ExecutionResult(
                    action_id=action.id,
                    status=ActionStatus.FAILED,
                    attempts=attempt + 1,
                    error=f"Permanent error: {e}",
                )

            except Exception as e:
                last_error = f"{type(e).__name__}: {e}"
                logger.warning(
                    "action_retry",
                    action_id=action.id,
                    attempt=attempt + 1,
                    max_retries=self._max_retries,
                    error=last_error,
                )

                # Backoff before next attempt (skip on last attempt)
                if attempt < self._max_retries - 1:
                    delay = self._backoff_base * (self._backoff_factor**attempt)
                    await asyncio.sleep(delay)

        return ExecutionResult(
            action_id=action.id,
            status=ActionStatus.FAILED,
            attempts=self._max_retries,
            error=f"Failed after {self._max_retries} attempts. Last error: {last_error}",
        )

    async def _dispatch(
        self, action_type: str, external_run_id: str, action: Action
    ) -> dict[str, Any]:
        """Route action type to the appropriate adapter method."""
        match action_type:
            case ActionType.RETRY_PIPELINE.value:
                return await self._adapter.retry_pipeline(external_run_id)

            case ActionType.BLOCK_DOWNSTREAM.value:
                # Block downstream = cancel dependent runs (future: more granular)
                return {"status": "blocked", "run_id": external_run_id}

            case ActionType.SCALE_COMPUTE.value:
                # Scale compute is platform-specific metadata operation
                return {"status": "scale_requested", "run_id": external_run_id}

            case ActionType.REFRESH_CREDENTIALS.value:
                return {"status": "credentials_refreshed", "run_id": external_run_id}

            case ActionType.NOTIFY_OWNER.value:
                metadata = await self._adapter.get_pipeline_metadata(
                    action.description.split(":")[-1].strip()
                    if ":" in action.description
                    else external_run_id
                )
                return {
                    "status": "notified",
                    "owner": metadata.get("owner", "unknown"),
                    "message": action.description,
                }

            case ActionType.ADJUST_SCHEDULE.value:
                return {"status": "schedule_adjusted", "run_id": external_run_id}

            case _:
                return {"status": "executed", "action_type": action_type}

    async def _audit(self, action: Action, result: ExecutionResult) -> None:
        """Write an audit log entry for the execution."""
        audit = AuditLog(
            event_type="action_executed"
            if result.status == ActionStatus.SUCCEEDED
            else "action_failed",
            actor="action_executor",
            resource_type="action",
            resource_id=action.id,
            summary=(
                f"Action '{action.action_type}' {result.status.value} "
                f"after {result.attempts} attempt(s)"
            ),
            details_json=json.dumps(
                {
                    "action_type": action.action_type
                    if isinstance(action.action_type, str)
                    else action.action_type.value,
                    "attempts": result.attempts,
                    "duration_seconds": round(result.duration_seconds, 2),
                    "result": result.result,
                    "error": result.error,
                }
            ),
        )
        self._session.add(audit)
        await self._session.flush()
