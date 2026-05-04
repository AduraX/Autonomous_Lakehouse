"""Cost Optimization Agent — baseline tracking, anomaly detection, recommendations.

Maintains rolling duration baselines per pipeline and flags anomalies using
z-score detection. When a run's duration exceeds the baseline by a configurable
threshold, the agent produces findings and optimization recommendations.

Anomaly detection:
  z-score = (duration - avg) / stddev
  If z-score > threshold (default 2.0), the run is anomalous.

Baseline update uses Welford's online algorithm for running mean + variance,
so we never need to scan the full event history.
"""

import json
import math
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from lakehouse.logging import get_logger
from lakehouse.models.actions import Action, ActionType
from lakehouse.models.cost import CostBaseline
from lakehouse.models.diagnoses import Diagnosis, FailureCategory
from lakehouse.models.events import PipelineEvent, PipelineStatus
from lakehouse.policies import apply_policy

logger = get_logger(__name__)

DEFAULT_Z_THRESHOLD = 2.0
MIN_RUNS_FOR_ANOMALY = 5  # Need at least N runs before flagging anomalies


class AnomalyType(StrEnum):
    DURATION_SPIKE = "duration_spike"
    HIGH_FAILURE_RATE = "high_failure_rate"
    REPEATED_RETRIES = "repeated_retries"


@dataclass
class CostFinding:
    """A cost anomaly or optimization opportunity."""

    anomaly_type: AnomalyType
    pipeline_name: str
    severity: str  # "low", "medium", "high"
    message: str
    details: dict[str, object] = field(default_factory=dict)
    recommendations: list[str] = field(default_factory=list)


@dataclass
class CostReport:
    """Cost analysis report for one or more pipelines."""

    pipelines_analyzed: int
    anomalies_found: int
    findings: list[CostFinding]
    total_actions_proposed: int = 0
    analyzed_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class CostOptimizer:
    """Analyzes pipeline cost/duration and detects anomalies."""

    def __init__(
        self,
        session: AsyncSession,
        z_threshold: float = DEFAULT_Z_THRESHOLD,
    ) -> None:
        self._session = session
        self._z_threshold = z_threshold

    async def update_baseline(self, event: PipelineEvent) -> CostBaseline:
        """Update the rolling baseline for a pipeline after a run completes.

        Uses Welford's online algorithm to maintain running mean and variance
        without needing to re-scan history.
        """
        result = await self._session.execute(
            select(CostBaseline).where(CostBaseline.pipeline_name == event.pipeline_name)
        )
        baseline = result.scalar_one_or_none()

        if baseline is None:
            baseline = CostBaseline(
                pipeline_name=event.pipeline_name,
                avg_duration=0.0,
                min_duration=0.0,
                max_duration=0.0,
                stddev_duration=0.0,
                total_runs=0,
                total_failures=0,
                total_successes=0,
                total_duration_seconds=0.0,
                consecutive_anomalies=0,
            )
            self._session.add(baseline)

        duration = event.duration_seconds or 0.0
        status_val = (
            event.status.value if isinstance(event.status, PipelineStatus) else event.status
        )

        # Update counters
        baseline.total_runs += 1
        baseline.total_duration_seconds += duration
        baseline.last_duration = duration
        baseline.last_run_at = event.finished_at or datetime.now(UTC)

        if status_val == PipelineStatus.SUCCEEDED.value:
            baseline.total_successes += 1
        elif status_val in (PipelineStatus.FAILED.value, PipelineStatus.TIMED_OUT.value):
            baseline.total_failures += 1

        # Welford's online algorithm for running mean + variance
        n = baseline.total_runs
        old_mean = baseline.avg_duration
        new_mean = old_mean + (duration - old_mean) / n
        baseline.avg_duration = new_mean

        if n >= 2:
            # Update variance using Welford's method
            # We track stddev, so we need to work with variance
            old_var = baseline.stddev_duration**2
            new_var = old_var + ((duration - old_mean) * (duration - new_mean) - old_var) / n
            baseline.stddev_duration = math.sqrt(max(new_var, 0.0))

        # Update min/max
        if n == 1:
            baseline.min_duration = duration
            baseline.max_duration = duration
        else:
            baseline.min_duration = min(baseline.min_duration, duration)
            baseline.max_duration = max(baseline.max_duration, duration)

        await self._session.flush()
        return baseline

    def detect_duration_anomaly(
        self, baseline: CostBaseline, duration: float
    ) -> CostFinding | None:
        """Check if a run duration is anomalous using z-score detection."""
        if baseline.total_runs < MIN_RUNS_FOR_ANOMALY:
            return None

        if baseline.stddev_duration == 0:
            return None

        z_score = (duration - baseline.avg_duration) / baseline.stddev_duration

        if z_score <= self._z_threshold:
            # Normal run — reset consecutive anomalies
            baseline.consecutive_anomalies = 0
            return None

        baseline.consecutive_anomalies += 1
        pct_over = ((duration - baseline.avg_duration) / baseline.avg_duration) * 100

        severity = "low" if z_score < 3.0 else ("medium" if z_score < 4.0 else "high")

        recommendations = []
        if pct_over > 100:
            recommendations.append("Review query execution plan for regressions")
            recommendations.append("Check for data volume spikes in source tables")
        if baseline.consecutive_anomalies >= 3:
            recommendations.append(
                "Pattern detected: 3+ consecutive slow runs — investigate infrastructure"
            )
        recommendations.append("Consider scaling compute resources")
        recommendations.append("Review partitioning strategy for large tables")

        return CostFinding(
            anomaly_type=AnomalyType.DURATION_SPIKE,
            pipeline_name=baseline.pipeline_name,
            severity=severity,
            message=(
                f"Duration spike: {duration:.0f}s is {pct_over:+.0f}% over baseline "
                f"(avg {baseline.avg_duration:.0f}s, z-score {z_score:.1f})"
            ),
            details={
                "duration_seconds": round(duration, 2),
                "avg_duration": round(baseline.avg_duration, 2),
                "stddev_duration": round(baseline.stddev_duration, 2),
                "z_score": round(z_score, 2),
                "pct_over_baseline": round(pct_over, 2),
                "consecutive_anomalies": baseline.consecutive_anomalies,
            },
            recommendations=recommendations,
        )

    def detect_failure_rate_anomaly(self, baseline: CostBaseline) -> CostFinding | None:
        """Check if a pipeline has an unusually high failure rate."""
        if baseline.total_runs < MIN_RUNS_FOR_ANOMALY:
            return None

        rate = baseline.failure_rate
        if rate < 0.3:  # Less than 30% failure is acceptable
            return None

        severity = "low" if rate < 0.5 else ("medium" if rate < 0.7 else "high")

        return CostFinding(
            anomaly_type=AnomalyType.HIGH_FAILURE_RATE,
            pipeline_name=baseline.pipeline_name,
            severity=severity,
            message=(
                f"High failure rate: {rate:.0%} "
                f"({baseline.total_failures}/{baseline.total_runs} runs failed)"
            ),
            details={
                "failure_rate": round(rate, 4),
                "total_runs": baseline.total_runs,
                "total_failures": baseline.total_failures,
                "total_successes": baseline.total_successes,
            },
            recommendations=[
                "Investigate recurring failure patterns in recent diagnoses",
                "Consider adding input validation before pipeline runs",
                "Review error handling and retry configuration",
            ],
        )

    async def analyze_event(self, event: PipelineEvent) -> list[CostFinding]:
        """Analyze a single pipeline event for cost anomalies.

        Updates the baseline and checks for anomalies.
        """
        duration = event.duration_seconds or 0.0
        baseline = await self.update_baseline(event)

        findings: list[CostFinding] = []

        # Duration anomaly
        duration_finding = self.detect_duration_anomaly(baseline, duration)
        if duration_finding:
            findings.append(duration_finding)

        # Failure rate anomaly
        failure_finding = self.detect_failure_rate_anomaly(baseline)
        if failure_finding:
            findings.append(failure_finding)

        if findings:
            logger.info(
                "cost_anomalies_detected",
                pipeline=event.pipeline_name,
                anomalies=len(findings),
                types=[f.anomaly_type.value for f in findings],
            )

        return findings

    async def analyze_and_coordinate(self, event: PipelineEvent) -> tuple[list[CostFinding], int]:
        """Analyze an event and create remediation actions for anomalies.

        Returns (findings, actions_proposed_count).
        """
        findings = await self.analyze_event(event)
        if not findings:
            return findings, 0

        actions_proposed = 0

        for finding in findings:
            # Create a diagnosis for the anomaly
            diagnosis = Diagnosis(
                event_id=event.id,
                category=FailureCategory.RESOURCE_EXHAUSTION
                if finding.anomaly_type == AnomalyType.DURATION_SPIKE
                else FailureCategory.DATA_QUALITY,
                confidence=0.80,
                summary=finding.message,
                agent_name="cost_optimizer",
                details_json=json.dumps(finding.details),
            )
            self._session.add(diagnosis)
            await self._session.flush()
            await self._session.refresh(diagnosis)

            # Propose actions based on anomaly type
            action_specs = self._determine_actions(finding)
            for action_type, description in action_specs:
                initial_status = apply_policy(action_type)
                action = Action(
                    diagnosis_id=diagnosis.id,
                    action_type=action_type,
                    description=description,
                    status=initial_status,
                    executed_by="cost_optimizer",
                )
                self._session.add(action)
                actions_proposed += 1

            await self._session.flush()

        return findings, actions_proposed

    @staticmethod
    def _determine_actions(finding: CostFinding) -> list[tuple[ActionType, str]]:
        """Map cost findings to remediation actions."""
        match finding.anomaly_type:
            case AnomalyType.DURATION_SPIKE:
                return [
                    (
                        ActionType.SCALE_COMPUTE,
                        f"Scale compute for {finding.pipeline_name}: {finding.message}",
                    ),
                    (
                        ActionType.NOTIFY_OWNER,
                        f"Notify owner: cost anomaly on {finding.pipeline_name}",
                    ),
                ]
            case AnomalyType.HIGH_FAILURE_RATE:
                return [
                    (
                        ActionType.NOTIFY_OWNER,
                        f"Notify owner: high failure rate on {finding.pipeline_name}",
                    ),
                    (
                        ActionType.REQUEST_APPROVAL,
                        (
                            f"Review pipeline {finding.pipeline_name}: "
                            f"{finding.failure_rate:.0%} failure rate"
                        )
                        if hasattr(finding, "failure_rate")
                        else f"Review pipeline {finding.pipeline_name}",
                    ),
                ]
            case _:
                return [
                    (
                        ActionType.NOTIFY_OWNER,
                        f"Cost anomaly on {finding.pipeline_name}: {finding.message}",
                    ),
                ]

    async def get_baseline(self, pipeline_name: str) -> CostBaseline | None:
        """Retrieve the baseline for a specific pipeline."""
        result = await self._session.execute(
            select(CostBaseline).where(CostBaseline.pipeline_name == pipeline_name)
        )
        return result.scalar_one_or_none()

    async def get_all_baselines(self) -> list[CostBaseline]:
        """Retrieve all pipeline baselines, ordered by total cost descending."""
        result = await self._session.execute(
            select(CostBaseline).order_by(CostBaseline.total_duration_seconds.desc())
        )
        return list(result.scalars().all())

    async def get_top_cost_pipelines(self, limit: int = 10) -> list[CostBaseline]:
        """Get the most expensive pipelines by total duration."""
        result = await self._session.execute(
            select(CostBaseline).order_by(CostBaseline.total_duration_seconds.desc()).limit(limit)
        )
        return list(result.scalars().all())
