"""Coordinator Agent — orchestrates specialist agents for pipeline failure remediation.

Ported from agentic-orch's coordinator + failure diagnosis pattern.
Loads a failed pipeline event, diagnoses the root cause, proposes
remediation actions, and persists results to the database.
"""

from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from lakehouse.adapters.base import PlatformAdapter
from lakehouse.agents.iceberg_quality import IcebergQualityAgent
from lakehouse.config import get_settings
from lakehouse.llm.base import LLMProvider
from lakehouse.logging import get_logger
from lakehouse.models.actions import Action, ActionType
from lakehouse.models.diagnoses import Diagnosis, FailureCategory
from lakehouse.models.events import PipelineEvent, PipelineStatus
from lakehouse.policies import apply_policy, classify_action
from lakehouse.schemas.coordination import (
    AgentFinding,
    CoordinationResult,
    ProposedAction,
)

logger = get_logger(__name__)

# Maps failure categories to recommended action types.
_CATEGORY_TO_ACTIONS: dict[FailureCategory, list[ActionType]] = {
    FailureCategory.SCHEMA_MISMATCH: [ActionType.BLOCK_DOWNSTREAM, ActionType.NOTIFY_OWNER],
    FailureCategory.SOURCE_UNAVAILABLE: [ActionType.RETRY_PIPELINE, ActionType.NOTIFY_OWNER],
    FailureCategory.SINK_UNAVAILABLE: [ActionType.RETRY_PIPELINE, ActionType.NOTIFY_OWNER],
    FailureCategory.TIMEOUT: [ActionType.RETRY_PIPELINE, ActionType.SCALE_COMPUTE],
    FailureCategory.PERMISSION_DENIED: [ActionType.REFRESH_CREDENTIALS, ActionType.NOTIFY_OWNER],
    FailureCategory.DATA_QUALITY: [ActionType.BLOCK_DOWNSTREAM, ActionType.NOTIFY_OWNER],
    FailureCategory.RESOURCE_EXHAUSTION: [ActionType.SCALE_COMPUTE, ActionType.RETRY_PIPELINE],
    FailureCategory.CONFIGURATION_ERROR: [ActionType.NOTIFY_OWNER, ActionType.REQUEST_APPROVAL],
    FailureCategory.DEPENDENCY_FAILURE: [ActionType.ADJUST_SCHEDULE, ActionType.NOTIFY_OWNER],
    FailureCategory.UNKNOWN: [ActionType.NOTIFY_OWNER, ActionType.REQUEST_APPROVAL],
}

# Pattern-matching rules for failure diagnosis (ported from agentic-orch).
_DIAGNOSIS_PATTERNS: list[tuple[list[str], FailureCategory, float, str]] = [
    (
        ["column", "not found"],
        FailureCategory.SCHEMA_MISMATCH,
        0.91,
        "Schema mismatch detected: missing column in source",
    ),
    (
        ["schema", "mismatch"],
        FailureCategory.SCHEMA_MISMATCH,
        0.89,
        "Schema mismatch between source and target",
    ),
    (["type mismatch"], FailureCategory.SCHEMA_MISMATCH, 0.87, "Column type mismatch detected"),
    (["timeout"], FailureCategory.TIMEOUT, 0.85, "Pipeline execution timed out"),
    (["connection timeout"], FailureCategory.TIMEOUT, 0.88, "Connection timeout to data source"),
    (
        ["not found", "source"],
        FailureCategory.SOURCE_UNAVAILABLE,
        0.90,
        "Source data store not found",
    ),
    (["not found", "sink"], FailureCategory.SINK_UNAVAILABLE, 0.90, "Sink data store not found"),
    (
        ["permission denied"],
        FailureCategory.PERMISSION_DENIED,
        0.92,
        "Insufficient permissions for operation",
    ),
    (
        ["lacks", "role"],
        FailureCategory.PERMISSION_DENIED,
        0.88,
        "Service principal missing required role",
    ),
    (["out of memory"], FailureCategory.RESOURCE_EXHAUSTION, 0.90, "Executor ran out of memory"),
    (
        ["duplicate", "key"],
        FailureCategory.DATA_QUALITY,
        0.86,
        "Duplicate key constraint violation",
    ),
    (
        ["null values"],
        FailureCategory.DATA_QUALITY,
        0.84,
        "Unexpected null values in required column",
    ),
    (
        ["upstream", "not completed"],
        FailureCategory.DEPENDENCY_FAILURE,
        0.87,
        "Upstream pipeline dependency not met",
    ),
]


class CoordinatorAgent:
    """Orchestrates specialist agents to diagnose and remediate pipeline failures."""

    def __init__(
        self,
        adapter: PlatformAdapter,
        session: AsyncSession,
        quality_agent: "IcebergQualityAgent | None" = None,
        llm_provider: LLMProvider | None = None,
    ) -> None:
        self._adapter = adapter
        self._session = session
        self._quality_agent = quality_agent
        self._llm_provider = llm_provider

    async def coordinate(self, event_id: int) -> CoordinationResult:
        """Run the full coordination workflow for a pipeline event.

        For failed events: diagnose and propose actions.
        For succeeded events: optionally run DQ checks (if quality_agent is set
        and the pipeline name contains an Iceberg table reference).

        Does NOT commit the session — the caller is responsible for committing.
        """
        event = await self._session.get(PipelineEvent, event_id)
        if event is None:
            raise ValueError(f"PipelineEvent with id={event_id} not found")

        status_val = (
            event.status.value if isinstance(event.status, PipelineStatus) else event.status
        )

        # For succeeded events, run post-success DQ checks if configured
        if status_val == PipelineStatus.SUCCEEDED.value:
            return await self._handle_success(event_id, event)

        # Skip running events
        if status_val == PipelineStatus.RUNNING.value:
            logger.info("coordination_skipped", event_id=event_id, status=status_val)
            return CoordinationResult(
                event_id=event_id,
                findings=[],
                proposed_actions=[],
                summary=f"No coordination needed — event status is {status_val}",
                coordinated_at=datetime.now(UTC),
            )

        # Gather context from the platform adapter (resilient — degrades gracefully)
        logs, metadata = await self._fetch_adapter_context(event)

        # Diagnose the failure (hybrid: rules first, LLM escalation if needed)
        finding = await self._hybrid_diagnose(event, logs, metadata)

        # Build proposed actions from the finding
        proposed_actions = self._build_actions(finding)

        # Persist to database
        await self._persist_diagnosis(event_id, finding)
        await self._persist_actions(proposed_actions)

        logger.info(
            "coordination_completed",
            event_id=event_id,
            category=finding.category.value,
            confidence=finding.confidence,
            actions_proposed=len(proposed_actions),
        )

        return CoordinationResult(
            event_id=event_id,
            findings=[finding],
            proposed_actions=proposed_actions,
            summary=(
                f"Diagnosed '{finding.category.value}' with "
                f"{finding.confidence:.0%} confidence. "
                f"Proposed {len(proposed_actions)} remediation action(s)."
            ),
            coordinated_at=datetime.now(UTC),
        )

    async def _fetch_adapter_context(
        self, event: PipelineEvent
    ) -> tuple[list[str], dict[str, object]]:
        """Fetch logs and metadata from the platform adapter.

        If either call fails (network timeout, API error, etc.), the coordinator
        continues with degraded context rather than failing the entire workflow.
        """
        logs: list[str] = []
        metadata: dict[str, object] = {}

        try:
            logs = await self._adapter.get_pipeline_logs(event.external_run_id)
        except Exception as e:
            logger.warning(
                "adapter_logs_failed",
                run_id=event.external_run_id,
                error=str(e),
            )

        try:
            metadata = await self._adapter.get_pipeline_metadata(event.pipeline_name)
        except Exception as e:
            logger.warning(
                "adapter_metadata_failed",
                pipeline=event.pipeline_name,
                error=str(e),
            )

        return logs, metadata

    async def _persist_diagnosis(self, event_id: int, finding: AgentFinding) -> Diagnosis:
        """Create and persist a Diagnosis record from an agent finding."""
        diagnosis = Diagnosis(
            event_id=event_id,
            category=finding.category,
            confidence=finding.confidence,
            summary=finding.summary,
            agent_name=finding.agent_name,
        )
        self._session.add(diagnosis)
        await self._session.flush()
        await self._session.refresh(diagnosis)
        self._last_diagnosis = diagnosis
        return diagnosis

    async def _persist_actions(self, proposed_actions: list[ProposedAction]) -> list[Action]:
        """Create and persist Action records from proposed actions."""
        diagnosis = self._last_diagnosis
        actions: list[Action] = []
        for pa in proposed_actions:
            initial_status = apply_policy(pa.action_type)
            action = Action(
                diagnosis_id=diagnosis.id,
                action_type=pa.action_type,
                description=pa.description,
                status=initial_status,
                executed_by="coordinator_agent",
            )
            self._session.add(action)
            actions.append(action)
        await self._session.flush()
        return actions

    async def _hybrid_diagnose(
        self,
        event: PipelineEvent,
        logs: list[str],
        metadata: dict[str, object],
    ) -> AgentFinding:
        """Hybrid diagnosis: rules first, LLM escalation for low confidence.

        1. Run rule-based pattern matching (fast, < 10ms)
        2. If confidence >= threshold AND category != UNKNOWN → use rule result
        3. Otherwise → escalate to LLM for deeper analysis
        4. If LLM fails → fall back to rule result
        """
        # Step 1: Rule-based diagnosis (always runs)
        rule_finding = self._diagnose_failure(event, logs)

        # Step 2: Check if LLM escalation is needed
        settings = get_settings()
        threshold = settings.llm_confidence_threshold

        needs_llm = self._llm_provider is not None and (
            rule_finding.category == FailureCategory.UNKNOWN or rule_finding.confidence < threshold
        )

        if not needs_llm:
            return rule_finding

        # Step 3: LLM escalation
        logger.info(
            "llm_escalation",
            event_id=event.id,
            rule_category=rule_finding.category.value,
            rule_confidence=rule_finding.confidence,
            threshold=threshold,
        )

        try:
            from lakehouse.agents.llm_diagnosis import LLMDiagnosisAgent

            llm_agent = LLMDiagnosisAgent(self._llm_provider)  # type: ignore[arg-type]
            platform_val = (
                event.platform.value if hasattr(event.platform, "value") else str(event.platform)
            )

            llm_finding = await llm_agent.diagnose(
                pipeline_name=event.pipeline_name,
                platform=platform_val,
                error_message=event.error_message or "",
                error_code=event.error_code or "",
                logs=logs,
                metadata=metadata,
            )

            # Use LLM result if it's more confident than rules
            if llm_finding.confidence > rule_finding.confidence:
                logger.info(
                    "llm_diagnosis_used",
                    event_id=event.id,
                    llm_category=llm_finding.category.value,
                    llm_confidence=llm_finding.confidence,
                    rule_category=rule_finding.category.value,
                    rule_confidence=rule_finding.confidence,
                )
                return llm_finding

            # Otherwise keep the rule result
            logger.info(
                "rule_diagnosis_preferred",
                event_id=event.id,
                reason="LLM confidence not higher than rules",
            )
            return rule_finding

        except Exception as e:
            # Step 4: LLM failed — fall back to rule result
            logger.warning(
                "llm_escalation_failed",
                event_id=event.id,
                error=str(e),
            )
            return rule_finding

    async def _handle_success(self, event_id: int, event: PipelineEvent) -> CoordinationResult:
        """Handle a successful pipeline event.

        If a quality_agent is configured, runs DQ checks on the table
        referenced in the pipeline name. DQ failures create findings
        and proposed actions.
        """
        if not self._quality_agent:
            logger.info("coordination_skipped", event_id=event_id, status="succeeded")
            return CoordinationResult(
                event_id=event_id,
                findings=[],
                proposed_actions=[],
                summary="No coordination needed — event status is succeeded",
                coordinated_at=datetime.now(UTC),
            )

        # Try to extract namespace.table from pipeline name or metadata
        # Convention: pipeline names like "ingest_<table>" or metadata with table info
        table_ref = self._extract_table_ref(event)
        if not table_ref:
            logger.info(
                "dq_skipped_no_table_ref",
                event_id=event_id,
                pipeline=event.pipeline_name,
            )
            return CoordinationResult(
                event_id=event_id,
                findings=[],
                proposed_actions=[],
                summary="Pipeline succeeded — no table reference found for DQ checks",
                coordinated_at=datetime.now(UTC),
            )

        namespace, table = table_ref

        try:
            from lakehouse.agents.quality_coordinator import run_quality_checks_and_coordinate

            results, actions_proposed = await run_quality_checks_and_coordinate(
                self._quality_agent,
                self._session,
                namespace=namespace,
                table=table,
            )

            # Convert DQ results to findings
            from lakehouse.agents.iceberg_quality import CheckStatus

            findings: list[AgentFinding] = []
            for r in results:
                if r.status in (CheckStatus.FAILED, CheckStatus.WARNING):
                    findings.append(
                        AgentFinding(
                            agent_name="iceberg_quality_agent",
                            category=FailureCategory.DATA_QUALITY,
                            confidence=0.95,
                            summary=r.message,
                            details=r.details,
                        )
                    )

            summary = (
                f"Pipeline succeeded. DQ checks on {namespace}.{table}: "
                f"{sum(1 for r in results if r.status == CheckStatus.PASSED)} passed, "
                f"{sum(1 for r in results if r.status == CheckStatus.FAILED)} failed. "
                f"{actions_proposed} action(s) proposed."
            )

            logger.info(
                "post_success_dq_completed",
                event_id=event_id,
                table=f"{namespace}.{table}",
                findings=len(findings),
                actions=actions_proposed,
            )

            return CoordinationResult(
                event_id=event_id,
                findings=findings,
                proposed_actions=[],  # Actions are created by quality_coordinator directly
                summary=summary,
                coordinated_at=datetime.now(UTC),
            )

        except Exception as e:
            logger.warning(
                "post_success_dq_failed",
                event_id=event_id,
                error=str(e),
            )
            return CoordinationResult(
                event_id=event_id,
                findings=[],
                proposed_actions=[],
                summary=f"Pipeline succeeded. DQ checks failed: {e}",
                coordinated_at=datetime.now(UTC),
            )

    @staticmethod
    def _extract_table_ref(event: PipelineEvent) -> tuple[str, str] | None:
        """Extract (namespace, table) from an event's metadata or pipeline name.

        Looks for:
          1. metadata_json with "iceberg_namespace" and "iceberg_table" keys
          2. Pipeline name in format "dq_check:namespace.table"
          3. Pipeline name in format "ingest_<table>" (assumes "lakehouse" namespace)
        """
        import json as _json

        # Check metadata
        if event.metadata_json:
            try:
                meta = _json.loads(event.metadata_json)
                ns = meta.get("iceberg_namespace")
                tbl = meta.get("iceberg_table")
                if ns and tbl:
                    return (str(ns), str(tbl))
            except (ValueError, TypeError):
                pass

        # Check pipeline name format "dq_check:namespace.table"
        name = event.pipeline_name or ""
        if ":" in name:
            ref = name.split(":", 1)[1]
            if "." in ref:
                parts = ref.split(".", 1)
                return (parts[0], parts[1])

        return None

    def _diagnose_failure(self, event: PipelineEvent, logs: list[str]) -> AgentFinding:
        """Pattern-match on error message and logs to classify the failure.

        Checks the event error_message first for a direct match, then
        falls back to combining error_message + logs.
        """
        error_text = (event.error_message or "").lower()

        # First pass: match on error_message alone (higher signal)
        for keywords, category, confidence, summary in _DIAGNOSIS_PATTERNS:
            if all(kw in error_text for kw in keywords):
                return AgentFinding(
                    agent_name="failure_diagnosis",
                    category=category,
                    confidence=confidence,
                    summary=summary,
                    details={
                        "matched_keywords": keywords,
                        "error_message": event.error_message or "",
                        "error_code": event.error_code or "",
                    },
                )

        # Second pass: include logs for broader context
        log_text = " ".join(logs).lower()
        combined = f"{error_text} {log_text}"

        for keywords, category, confidence, summary in _DIAGNOSIS_PATTERNS:
            if all(kw in combined for kw in keywords):
                return AgentFinding(
                    agent_name="failure_diagnosis",
                    category=category,
                    confidence=confidence,
                    summary=summary,
                    details={
                        "matched_keywords": keywords,
                        "error_message": event.error_message or "",
                        "error_code": event.error_code or "",
                    },
                )

        return AgentFinding(
            agent_name="failure_diagnosis",
            category=FailureCategory.UNKNOWN,
            confidence=0.40,
            summary="Unable to classify failure — escalating for human review",
            details={
                "error_message": event.error_message or "",
                "error_code": event.error_code or "",
            },
        )

    def _build_actions(self, finding: AgentFinding) -> list[ProposedAction]:
        """Map a diagnosis finding to proposed remediation actions."""
        action_types = _CATEGORY_TO_ACTIONS.get(
            finding.category,
            [ActionType.NOTIFY_OWNER, ActionType.REQUEST_APPROVAL],
        )

        descriptions: dict[ActionType, str] = {
            ActionType.RETRY_PIPELINE: (
                f"Retry failed pipeline — diagnosed as {finding.category.value}"
            ),
            ActionType.BLOCK_DOWNSTREAM: (
                f"Block downstream pipelines — {finding.category.value} detected"
            ),
            ActionType.SCALE_COMPUTE: "Scale compute resources to prevent resource exhaustion",
            ActionType.REFRESH_CREDENTIALS: "Refresh service principal credentials",
            ActionType.NOTIFY_OWNER: f"Notify pipeline owner: {finding.summary}",
            ActionType.REQUEST_APPROVAL: "Request human approval for next steps",
            ActionType.ADJUST_SCHEDULE: "Adjust pipeline schedule to account for dependency timing",
            ActionType.CUSTOM: f"Custom remediation for {finding.category.value}",
        }

        return [
            ProposedAction(
                action_type=at,
                description=descriptions.get(at, f"Execute {at.value}"),
                policy=classify_action(at),
            )
            for at in action_types
        ]
