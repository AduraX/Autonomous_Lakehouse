"""Incident Summarizer — LLM-powered natural language summaries.

Generates human-readable incident reports from the chain of
events, diagnoses, and actions. Used for:
  - Audit trail entries that operators actually read
  - Slack/email notifications with context
  - Post-mortem analysis

Falls back to template-based summaries when LLM is unavailable.
"""

from datetime import UTC, datetime

from lakehouse.llm.base import LLMProvider
from lakehouse.logging import get_logger

logger = get_logger(__name__)

_SYSTEM_PROMPT = (
    "You are an incident report writer for a data "
    "engineering platform.\n"
    "Write concise, professional incident summaries "
    "for pipeline operators.\n"
    "Use plain language. Be specific about what happened, "
    "what was diagnosed, and what actions were taken.\n"
    "Keep summaries under 200 words. No markdown formatting."
)

_INCIDENT_TEMPLATE = """Summarize this pipeline incident:

**Pipeline:** {pipeline_name}
**Platform:** {platform}
**Status:** {status}
**Error:** {error_message}
**Time:** {timestamp}

**Diagnosis:**
- Category: {diagnosis_category}
- Confidence: {diagnosis_confidence}
- Summary: {diagnosis_summary}

**Actions Proposed ({action_count} total):**
{actions_text}

Write a concise incident summary for the operations team."""

_COST_TEMPLATE = """Summarize this cost anomaly:

**Pipeline:** {pipeline_name}
**Anomaly Type:** {anomaly_type}
**Severity:** {severity}
**Details:** {message}

**Baseline Stats:**
- Average duration: {avg_duration}s
- This run duration: {current_duration}s
- Change: {pct_change}%

**Recommendations:**
{recommendations}

Write a concise cost alert summary for the operations team."""


class IncidentSummarizer:
    """Generates natural language summaries for incidents and alerts."""

    def __init__(self, provider: LLMProvider | None = None) -> None:
        self._provider = provider

    async def summarize_incident(
        self,
        pipeline_name: str,
        platform: str,
        status: str,
        error_message: str,
        diagnosis_category: str,
        diagnosis_confidence: float,
        diagnosis_summary: str,
        actions: list[dict[str, str]],
    ) -> str:
        """Generate a natural language summary for a pipeline incident."""
        actions_text = (
            "\n".join(
                f"- [{a.get('action_type', 'unknown')}] {a.get('description', '')}" for a in actions
            )
            or "(none)"
        )

        prompt = _INCIDENT_TEMPLATE.format(
            pipeline_name=pipeline_name,
            platform=platform,
            status=status,
            error_message=error_message or "(no error)",
            timestamp=datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
            diagnosis_category=diagnosis_category,
            diagnosis_confidence=f"{diagnosis_confidence:.0%}",
            diagnosis_summary=diagnosis_summary,
            action_count=len(actions),
            actions_text=actions_text,
        )

        if self._provider:
            try:
                response = await self._provider.complete(
                    prompt=prompt,
                    system=_SYSTEM_PROMPT,
                    max_tokens=300,
                    temperature=0.3,
                )
                logger.info(
                    "incident_summary_generated",
                    provider=self._provider.provider_name,
                    tokens=response.usage.get("output_tokens", 0),
                )
                return response.content.strip()
            except Exception as e:
                logger.warning("incident_summary_llm_failed", error=str(e))

        # Template fallback
        return self._template_incident_summary(
            pipeline_name,
            platform,
            status,
            error_message,
            diagnosis_category,
            diagnosis_confidence,
            diagnosis_summary,
            actions,
        )

    async def summarize_cost_anomaly(
        self,
        pipeline_name: str,
        anomaly_type: str,
        severity: str,
        message: str,
        avg_duration: float,
        current_duration: float,
        recommendations: list[str],
    ) -> str:
        """Generate a natural language summary for a cost anomaly."""
        pct_change = (
            ((current_duration - avg_duration) / avg_duration * 100) if avg_duration > 0 else 0
        )

        prompt = _COST_TEMPLATE.format(
            pipeline_name=pipeline_name,
            anomaly_type=anomaly_type,
            severity=severity,
            message=message,
            avg_duration=f"{avg_duration:.0f}",
            current_duration=f"{current_duration:.0f}",
            pct_change=f"{pct_change:+.0f}",
            recommendations="\n".join(f"- {r}" for r in recommendations) or "(none)",
        )

        if self._provider:
            try:
                response = await self._provider.complete(
                    prompt=prompt,
                    system=_SYSTEM_PROMPT,
                    max_tokens=200,
                    temperature=0.3,
                )
                return response.content.strip()
            except Exception as e:
                logger.warning("cost_summary_llm_failed", error=str(e))

        # Template fallback
        recs = "; ".join(recommendations[:3])
        return (
            f"Cost alert ({severity}): "
            f"{pipeline_name} — {message}. "
            f"Duration {current_duration:.0f}s vs baseline "
            f"{avg_duration:.0f}s ({pct_change:+.0f}%). "
            f"Recommendations: {recs}."
        )

    @staticmethod
    def _template_incident_summary(
        pipeline_name: str,
        platform: str,
        status: str,
        error_message: str,
        diagnosis_category: str,
        diagnosis_confidence: float,
        diagnosis_summary: str,
        actions: list[dict[str, str]],
    ) -> str:
        """Template-based fallback when LLM is unavailable."""
        action_types = [a.get("action_type", "unknown") for a in actions]
        return (
            f"Pipeline '{pipeline_name}' ({platform}) {status}. "
            f"Diagnosed as {diagnosis_category} ({diagnosis_confidence:.0%} confidence): "
            f"{diagnosis_summary}. "
            f"{len(actions)} action(s) proposed: {', '.join(action_types)}."
        )
