"""Quality Coordinator — bridges DQ check failures into the remediation pipeline.

When Iceberg quality checks fail, this module:
  1. Creates a PipelineEvent of type "data_quality_check"
  2. Creates a Diagnosis with category DATA_QUALITY
  3. Proposes remediation actions (block downstream, notify owner)
  4. Applies policy classification to each action

This ensures DQ failures flow through the same audit trail,
approval gates, and execution pipeline as pipeline failures.
"""

import json
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from lakehouse.agents.iceberg_quality import CheckStatus, IcebergQualityAgent, QualityCheckResult
from lakehouse.logging import get_logger
from lakehouse.models.actions import Action, ActionType
from lakehouse.models.diagnoses import Diagnosis, FailureCategory
from lakehouse.models.events import PipelineEvent, PipelineStatus, Platform
from lakehouse.policies import apply_policy

logger = get_logger(__name__)


async def run_quality_checks_and_coordinate(
    agent: IcebergQualityAgent,
    session: AsyncSession,
    namespace: str,
    table: str,
    expected_columns: list[str] | None = None,
    max_age_hours: float = 24.0,
) -> tuple[list[QualityCheckResult], int]:
    """Run quality checks on a table and create remediation actions for failures.

    Returns:
        Tuple of (check_results, actions_proposed_count)
    """
    results = await agent.run_all_checks(
        namespace,
        table,
        expected_columns=expected_columns,
        max_age_hours=max_age_hours,
    )

    failed_checks = [r for r in results if r.status == CheckStatus.FAILED]
    if not failed_checks:
        return results, 0

    # Create a pipeline event to represent this DQ failure
    full_table = f"{namespace}.{table}"
    failure_summary = "; ".join(r.message for r in failed_checks)

    event = PipelineEvent(
        external_run_id=f"dq-check-{full_table}-{int(datetime.now(UTC).timestamp())}",
        pipeline_name=f"dq_check:{full_table}",
        platform=Platform.MOCK,  # DQ checks are platform-agnostic
        status=PipelineStatus.FAILED,
        error_message=failure_summary,
        error_code="DQ_CHECK_FAILED",
        metadata_json=json.dumps(
            {
                "check_type": "iceberg_quality",
                "namespace": namespace,
                "table": table,
                "failed_checks": [r.check_name for r in failed_checks],
            }
        ),
    )
    session.add(event)
    await session.flush()
    await session.refresh(event)

    # Create diagnosis
    diagnosis = Diagnosis(
        event_id=event.id,
        category=FailureCategory.DATA_QUALITY,
        confidence=0.95,  # DQ checks are deterministic
        summary=f"Data quality check failed on {full_table}: {failure_summary}",
        agent_name="iceberg_quality_agent",
        details_json=json.dumps([r.to_dict() for r in failed_checks]),
    )
    session.add(diagnosis)
    await session.flush()
    await session.refresh(diagnosis)

    # Propose actions based on which checks failed
    actions_proposed = 0
    action_specs = _determine_actions(failed_checks, full_table)

    for action_type, description in action_specs:
        initial_status = apply_policy(action_type)
        action = Action(
            diagnosis_id=diagnosis.id,
            action_type=action_type,
            description=description,
            status=initial_status,
            executed_by="quality_coordinator",
        )
        session.add(action)
        actions_proposed += 1

    await session.flush()

    logger.info(
        "quality_coordination_completed",
        table=full_table,
        failed_checks=len(failed_checks),
        actions_proposed=actions_proposed,
        event_id=event.id,
    )

    return results, actions_proposed


def _determine_actions(
    failed_checks: list[QualityCheckResult], table: str
) -> list[tuple[ActionType, str]]:
    """Map failed quality checks to specific remediation actions."""
    actions: list[tuple[ActionType, str]] = []
    check_names = {r.check_name for r in failed_checks}

    if "schema_drift" in check_names:
        actions.append(
            (
                ActionType.BLOCK_DOWNSTREAM,
                f"Block downstream consumers of {table} — schema drift detected",
            )
        )
        actions.append(
            (
                ActionType.NOTIFY_OWNER,
                f"Notify owner: schema drift detected on {table}",
            )
        )

    if "freshness" in check_names:
        actions.append(
            (
                ActionType.NOTIFY_OWNER,
                f"Notify owner: {table} is stale — freshness check failed",
            )
        )
        actions.append(
            (
                ActionType.RETRY_PIPELINE,
                f"Retry upstream pipeline to refresh {table}",
            )
        )

    if "volume_anomaly" in check_names:
        actions.append(
            (
                ActionType.BLOCK_DOWNSTREAM,
                f"Block downstream consumers of {table} — volume anomaly detected",
            )
        )
        actions.append(
            (
                ActionType.NOTIFY_OWNER,
                f"Notify owner: volume anomaly on {table}",
            )
        )

    # Always notify if no specific actions matched
    if not actions:
        actions.append(
            (
                ActionType.NOTIFY_OWNER,
                f"Notify owner: quality check failed on {table}",
            )
        )

    # Deduplicate by action type (keep first description)
    seen: set[ActionType] = set()
    unique: list[tuple[ActionType, str]] = []
    for at, desc in actions:
        if at not in seen:
            seen.add(at)
            unique.append((at, desc))

    return unique


async def scan_namespace_and_coordinate(
    agent: IcebergQualityAgent,
    session: AsyncSession,
    namespace: str,
    max_age_hours: float = 24.0,
) -> tuple[dict[str, list[QualityCheckResult]], int]:
    """Scan all tables in a namespace and coordinate failures.

    Returns:
        Tuple of (table_results_dict, total_actions_proposed)
    """
    tables = await agent.list_tables(namespace)
    all_results: dict[str, list[QualityCheckResult]] = {}
    total_actions = 0

    for full_name in tables:
        table_name = full_name.split(".")[-1]
        results, actions = await run_quality_checks_and_coordinate(
            agent,
            session,
            namespace,
            table_name,
            max_age_hours=max_age_hours,
        )
        all_results[full_name] = results
        total_actions += actions

    logger.info(
        "namespace_scan_coordination_completed",
        namespace=namespace,
        tables=len(tables),
        total_actions=total_actions,
    )

    return all_results, total_actions
