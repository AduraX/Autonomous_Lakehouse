"""Conversational Operations Interface — natural language queries about the lakehouse.

Parses user questions, queries the relevant database tables, and optionally
uses an LLM to produce a natural language answer. Falls back to structured
data-only responses when no LLM is available.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from lakehouse.llm.base import LLMProvider
from lakehouse.logging import get_logger
from lakehouse.models.actions import Action
from lakehouse.models.cost import CostBaseline
from lakehouse.models.diagnoses import Diagnosis
from lakehouse.models.events import PipelineEvent, PipelineStatus

logger = get_logger(__name__)

_MAX_RESULTS = 20

_SYSTEM_PROMPT = """You are an operations assistant for an autonomous lakehouse platform.
Answer the user's question based on the data provided. Be concise and specific.
If the data is empty, say so clearly. Use plain language suitable for a data engineer.
Do not invent data that is not in the context. Keep answers under 300 words."""

_USER_PROMPT_TEMPLATE = """User question: {question}

Detected intent: {intent}

Database query results:
{context}

Answer the user's question using only the data above."""


@dataclass
class ChatResponse:
    """Result from the ChatOps agent."""

    answer: str
    intent: str
    data: dict[str, Any] = field(default_factory=dict)
    llm_used: bool = False


class ChatOpsAgent:
    """Handles natural language queries about the lakehouse by querying the DB
    and optionally using an LLM for a conversational answer."""

    def __init__(self, llm_provider: LLMProvider | None = None) -> None:
        self._provider = llm_provider

    async def query(self, question: str, session: AsyncSession) -> ChatResponse:
        """Answer a natural language question by querying the database and using LLM."""
        intent = self._detect_intent(question)
        data = await self._fetch_data(intent, question, session)
        context = self._build_context(intent, data)

        if self._provider:
            try:
                answer = await self._llm_answer(question, intent, context)
                return ChatResponse(
                    answer=answer,
                    intent=intent,
                    data=data,
                    llm_used=True,
                )
            except Exception as e:
                logger.warning("chatops_llm_failed", error=str(e))

        # Fallback: structured data-only response
        answer = self._template_answer(intent, data)
        return ChatResponse(answer=answer, intent=intent, data=data, llm_used=False)

    # ------------------------------------------------------------------
    # Intent detection (keyword-based)
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_intent(question: str) -> str:
        """Classify the user's question into an intent category."""
        q = question.lower()

        if any(kw in q for kw in ("fail", "error", "broke")):
            return "failure_analysis"
        if any(kw in q for kw in ("cost", "expensive", "slow", "duration")):
            return "cost_inquiry"
        if any(kw in q for kw in ("action", "approve", "reject", "execute")):
            return "action_status"
        if "pipeline" in q and _extract_pipeline_name(q):
            return "pipeline_specific"
        return "general"

    # ------------------------------------------------------------------
    # Database queries
    # ------------------------------------------------------------------

    async def _fetch_data(
        self, intent: str, question: str, session: AsyncSession
    ) -> dict[str, Any]:
        match intent:
            case "failure_analysis":
                return await self._fetch_failures(session)
            case "cost_inquiry":
                return await self._fetch_costs(session)
            case "action_status":
                return await self._fetch_actions(session)
            case "pipeline_specific":
                name = _extract_pipeline_name(question.lower()) or ""
                return await self._fetch_pipeline(name, session)
            case _:
                return await self._fetch_general(session)

    async def _fetch_failures(self, session: AsyncSession) -> dict[str, Any]:
        # Recent failed events
        stmt = (
            select(PipelineEvent)
            .where(PipelineEvent.status == PipelineStatus.FAILED)
            .order_by(PipelineEvent.created_at.desc())
            .limit(_MAX_RESULTS)
        )
        result = await session.execute(stmt)
        events = list(result.scalars().all())

        # Diagnoses for those events
        event_ids = [e.id for e in events]
        diagnoses: list[Any] = []
        if event_ids:
            d_stmt = (
                select(Diagnosis)
                .where(Diagnosis.event_id.in_(event_ids))
                .order_by(Diagnosis.created_at.desc())
                .limit(_MAX_RESULTS)
            )
            d_result = await session.execute(d_stmt)
            diagnoses = list(d_result.scalars().all())

        return {
            "failed_events": [_event_to_dict(e) for e in events],
            "diagnoses": [_diagnosis_to_dict(d) for d in diagnoses],
        }

    async def _fetch_costs(self, session: AsyncSession) -> dict[str, Any]:
        stmt = (
            select(CostBaseline)
            .order_by(CostBaseline.total_duration_seconds.desc())
            .limit(_MAX_RESULTS)
        )
        result = await session.execute(stmt)
        baselines = list(result.scalars().all())
        return {
            "cost_baselines": [_baseline_to_dict(b) for b in baselines],
        }

    async def _fetch_actions(self, session: AsyncSession) -> dict[str, Any]:
        stmt = select(Action).order_by(Action.created_at.desc()).limit(_MAX_RESULTS)
        result = await session.execute(stmt)
        actions = list(result.scalars().all())
        return {
            "actions": [_action_to_dict(a) for a in actions],
        }

    async def _fetch_pipeline(self, pipeline_name: str, session: AsyncSession) -> dict[str, Any]:
        stmt = (
            select(PipelineEvent)
            .where(PipelineEvent.pipeline_name.ilike(f"%{pipeline_name}%"))
            .order_by(PipelineEvent.created_at.desc())
            .limit(_MAX_RESULTS)
        )
        result = await session.execute(stmt)
        events = list(result.scalars().all())
        return {
            "pipeline_events": [_event_to_dict(e) for e in events],
        }

    async def _fetch_general(self, session: AsyncSession) -> dict[str, Any]:
        # Recent events
        stmt = select(PipelineEvent).order_by(PipelineEvent.created_at.desc()).limit(10)
        result = await session.execute(stmt)
        events = list(result.scalars().all())

        # Summary stats
        total_q = select(func.count(PipelineEvent.id))
        total = (await session.execute(total_q)).scalar_one()

        failed_q = select(func.count(PipelineEvent.id)).where(
            PipelineEvent.status == PipelineStatus.FAILED
        )
        failed = (await session.execute(failed_q)).scalar_one()

        return {
            "recent_events": [_event_to_dict(e) for e in events],
            "total_events": total,
            "total_failures": failed,
        }

    # ------------------------------------------------------------------
    # Context building
    # ------------------------------------------------------------------

    @staticmethod
    def _build_context(intent: str, data: dict[str, Any]) -> str:
        """Serialize query results into a text block for the LLM."""
        parts: list[str] = []
        for key, value in data.items():
            if isinstance(value, list):
                parts.append(f"{key} ({len(value)} items):")
                for item in value[:_MAX_RESULTS]:
                    parts.append(f"  {json.dumps(item, default=str)}")
                if not value:
                    parts.append("  (none)")
            else:
                parts.append(f"{key}: {value}")
        return "\n".join(parts) if parts else "(no data found)"

    # ------------------------------------------------------------------
    # LLM answer
    # ------------------------------------------------------------------

    async def _llm_answer(self, question: str, intent: str, context: str) -> str:
        assert self._provider is not None
        prompt = _USER_PROMPT_TEMPLATE.format(
            question=question, intent=intent, context=context[:4000]
        )
        response = await self._provider.complete(
            prompt=prompt,
            system=_SYSTEM_PROMPT,
            max_tokens=512,
            temperature=0.2,
        )
        logger.info(
            "chatops_llm_response",
            provider=self._provider.provider_name,
            tokens=response.usage.get("output_tokens", 0),
        )
        return response.content.strip()

    # ------------------------------------------------------------------
    # Template fallback
    # ------------------------------------------------------------------

    @staticmethod
    def _template_answer(intent: str, data: dict[str, Any]) -> str:
        """Generate a structured text answer without an LLM."""
        match intent:
            case "failure_analysis":
                events = data.get("failed_events", [])
                diagnoses = data.get("diagnoses", [])
                if not events:
                    return "No recent failures found."
                lines = [f"Found {len(events)} recent failure(s)."]
                for e in events[:5]:
                    err_msg = e.get("error_message", "no error message")
                    lines.append(f"- {e['pipeline_name']} ({e['platform']}): {err_msg}")
                if diagnoses:
                    lines.append(f"\n{len(diagnoses)} diagnosis(es) available:")
                    for d in diagnoses[:5]:
                        lines.append(
                            f"- [{d['category']}] {d['summary']} (confidence: {d['confidence']})"
                        )
                return "\n".join(lines)

            case "cost_inquiry":
                baselines = data.get("cost_baselines", [])
                if not baselines:
                    return "No cost baselines found."
                lines = [f"Found {len(baselines)} pipeline cost baseline(s)."]
                for b in baselines[:5]:
                    lines.append(
                        f"- {b['pipeline_name']}: avg {b['avg_duration']:.1f}s, "
                        f"{b['total_runs']} runs, {b['total_failures']} failures"
                    )
                return "\n".join(lines)

            case "action_status":
                actions = data.get("actions", [])
                if not actions:
                    return "No actions found."
                lines = [f"Found {len(actions)} action(s)."]
                for a in actions[:5]:
                    lines.append(f"- [{a['status']}] {a['action_type']}: {a['description']}")
                return "\n".join(lines)

            case "pipeline_specific":
                events = data.get("pipeline_events", [])
                if not events:
                    return "No events found for that pipeline."
                lines = [f"Found {len(events)} event(s) for the pipeline."]
                for e in events[:5]:
                    lines.append(f"- {e['pipeline_name']} [{e['status']}] at {e['created_at']}")
                return "\n".join(lines)

            case _:
                total = data.get("total_events", 0)
                failed = data.get("total_failures", 0)
                recent = data.get("recent_events", [])
                lines = [f"System overview: {total} total events, {failed} failures."]
                if recent:
                    lines.append("Recent events:")
                    for e in recent[:5]:
                        lines.append(f"- {e['pipeline_name']} [{e['status']}] ({e['platform']})")
                return "\n".join(lines)


# ------------------------------------------------------------------
# Helper serializers
# ------------------------------------------------------------------


def _extract_pipeline_name(question: str) -> str | None:
    """Try to extract a pipeline name from the question text."""
    # Match quoted names first
    match = re.search(r'["\']([^"\']+)["\']', question)
    if match:
        return match.group(1)
    # Match word after "pipeline"
    match = re.search(r"pipeline\s+(\S+)", question)
    if match:
        name = match.group(1).strip("?.,!")
        # Avoid matching generic words
        if name and name not in ("is", "was", "has", "the", "a", "an", "did", "do", "run"):
            return name
    return None


def _event_to_dict(event: PipelineEvent) -> dict[str, Any]:
    return {
        "id": event.id,
        "pipeline_name": event.pipeline_name,
        "platform": event.platform.value
        if hasattr(event.platform, "value")
        else str(event.platform),
        "status": event.status.value if hasattr(event.status, "value") else str(event.status),
        "error_message": event.error_message,
        "error_code": event.error_code,
        "duration_seconds": event.duration_seconds,
        "created_at": str(event.created_at),
    }


def _diagnosis_to_dict(diag: Diagnosis) -> dict[str, Any]:
    return {
        "id": diag.id,
        "event_id": diag.event_id,
        "category": diag.category.value if hasattr(diag.category, "value") else str(diag.category),
        "confidence": diag.confidence,
        "summary": diag.summary,
        "agent_name": diag.agent_name,
    }


def _action_to_dict(action: Action) -> dict[str, Any]:
    return {
        "id": action.id,
        "action_type": action.action_type.value
        if hasattr(action.action_type, "value")
        else str(action.action_type),
        "status": action.status.value if hasattr(action.status, "value") else str(action.status),
        "description": action.description,
        "approved_by": action.approved_by,
        "executed_by": action.executed_by,
    }


def _baseline_to_dict(baseline: CostBaseline) -> dict[str, Any]:
    return {
        "pipeline_name": baseline.pipeline_name,
        "avg_duration": baseline.avg_duration,
        "min_duration": baseline.min_duration,
        "max_duration": baseline.max_duration,
        "total_runs": baseline.total_runs,
        "total_failures": baseline.total_failures,
        "total_duration_seconds": baseline.total_duration_seconds,
        "failure_rate": baseline.failure_rate,
    }
