"""Adaptive Policy Learner — analyzes action approval history to suggest policy changes.

Learns from operator approval/rejection patterns to recommend promoting
actions to auto-approve, flagging actions as forbidden, or keeping them
in the requires_approval tier. Optionally uses an LLM for richer reasoning.
"""

import json
from dataclasses import dataclass

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from lakehouse.llm.base import LLMProvider
from lakehouse.logging import get_logger
from lakehouse.models.actions import Action, ActionStatus, ActionType
from lakehouse.models.diagnoses import Diagnosis
from lakehouse.policies import PolicyClassification, classify_action

logger = get_logger(__name__)


@dataclass
class ActionStats:
    """Approval/rejection statistics for an (action_type, failure_category) pair."""

    action_type: str
    failure_category: str
    total: int
    approved: int
    rejected: int
    succeeded: int
    failed: int
    approval_rate: float


@dataclass
class PolicySuggestion:
    """A suggested policy change based on historical action outcomes."""

    action_type: str
    failure_category: str
    current_policy: str  # "auto_approved", "requires_approval", "forbidden"
    suggested_policy: str
    confidence: float
    reason: str
    stats: ActionStats


_SYSTEM_PROMPT = """You are a policy advisor for an autonomous lakehouse operations platform.

You analyze action approval/rejection statistics and suggest policy tier changes.
Policy tiers are:
- auto_approved: action executes immediately without human review
- requires_approval: action needs human approval before execution
- forbidden: action is never allowed

Given the statistics for each (action_type, failure_category) pair, provide reasoning
for whether the policy should change.

You MUST respond with a valid JSON array of objects matching this schema:
[
  {
    "action_type": "<action type>",
    "failure_category": "<failure category>",
    "suggested_policy": "<auto_approved|requires_approval|forbidden>",
    "confidence": <float 0.0-1.0>,
    "reason": "<1-2 sentence explanation>"
  }
]

Rules:
- Only suggest changes where the data strongly supports it.
- High approval rate (>90%) with sufficient volume (>=10) suggests auto_approved.
- High rejection rate (>80%) with sufficient volume (>=5) suggests forbidden.
- If outcomes are mixed, keep requires_approval.
- Consider the risk: some action types are inherently riskier than others.
- Respond with ONLY the JSON array. No markdown, no explanation outside the JSON."""

_USER_PROMPT_TEMPLATE = (
    "Analyze the following action approval statistics "
    "and suggest policy changes:\n\n"
    "{stats_json}\n\n"
    "For each entry, consider the approval rate, total "
    "volume, and outcome success/failure\n"
    "rates to recommend whether the policy tier "
    "should change."
)


class PolicyLearner:
    """Analyzes action approval history to suggest policy changes."""

    def __init__(self, llm_provider: LLMProvider | None = None) -> None:
        self._llm = llm_provider

    async def get_action_stats(self, session: AsyncSession) -> list[ActionStats]:
        """Get approval/rejection stats per action_type + failure_category."""
        stmt = (
            select(
                Action.action_type,
                Diagnosis.category,
                func.count().label("total"),
                func.sum(
                    case(
                        (
                            Action.status.in_(
                                [
                                    ActionStatus.APPROVED,
                                    ActionStatus.EXECUTING,
                                    ActionStatus.SUCCEEDED,
                                ]
                            ),
                            1,
                        ),
                        else_=0,
                    )
                ).label("approved"),
                func.sum(
                    case(
                        (Action.status == ActionStatus.REJECTED, 1),
                        else_=0,
                    )
                ).label("rejected"),
                func.sum(
                    case(
                        (Action.status == ActionStatus.SUCCEEDED, 1),
                        else_=0,
                    )
                ).label("succeeded"),
                func.sum(
                    case(
                        (Action.status == ActionStatus.FAILED, 1),
                        else_=0,
                    )
                ).label("failed"),
            )
            .join(Diagnosis, Action.diagnosis_id == Diagnosis.id)
            .group_by(Action.action_type, Diagnosis.category)
        )

        result = await session.execute(stmt)
        rows = result.all()

        stats = []
        for row in rows:
            total = int(row.total)
            approved = int(row.approved)
            approval_rate = approved / total if total > 0 else 0.0
            stats.append(
                ActionStats(
                    action_type=row.action_type
                    if isinstance(row.action_type, str)
                    else row.action_type.value,
                    failure_category=row.category
                    if isinstance(row.category, str)
                    else row.category.value,
                    total=total,
                    approved=approved,
                    rejected=int(row.rejected),
                    succeeded=int(row.succeeded),
                    failed=int(row.failed),
                    approval_rate=round(approval_rate, 4),
                )
            )

        logger.info("action_stats_computed", group_count=len(stats))
        return stats

    async def analyze_approval_history(self, session: AsyncSession) -> list[PolicySuggestion]:
        """Analyze approval/rejection patterns and suggest policy changes."""
        stats = await self.get_action_stats(session)

        if not stats:
            logger.info("no_action_history", message="No actions to analyze")
            return []

        # Rule-based suggestions
        suggestions = self._rule_based_suggestions(stats)

        # Enhance with LLM reasoning if available
        if self._llm is not None:
            suggestions = await self._llm_enhanced_suggestions(stats, suggestions)

        logger.info(
            "policy_analysis_complete",
            suggestion_count=len(suggestions),
            llm_used=self._llm is not None,
        )
        return suggestions

    def _rule_based_suggestions(self, stats: list[ActionStats]) -> list[PolicySuggestion]:
        """Generate suggestions based on threshold rules."""
        suggestions: list[PolicySuggestion] = []

        for s in stats:
            try:
                action_type_enum = ActionType(s.action_type)
                current = classify_action(action_type_enum).value
            except ValueError:
                current = PolicyClassification.REQUIRES_APPROVAL.value

            rejection_rate = s.rejected / s.total if s.total > 0 else 0.0

            if s.approval_rate > 0.9 and s.total >= 10:
                suggested = PolicyClassification.AUTO_APPROVED.value
                confidence = min(s.approval_rate, 0.95)
                reason = (
                    f"Approval rate of {s.approval_rate:.0%} across {s.total} actions "
                    f"suggests this can be safely auto-approved."
                )
            elif rejection_rate > 0.8 and s.total >= 5:
                suggested = PolicyClassification.FORBIDDEN.value
                confidence = min(rejection_rate, 0.95)
                reason = (
                    f"Rejection rate of {rejection_rate:.0%} across {s.total} actions "
                    f"suggests this action should be forbidden."
                )
            else:
                suggested = PolicyClassification.REQUIRES_APPROVAL.value
                confidence = 0.5
                reason = (
                    f"Mixed approval rate of {s.approval_rate:.0%} across {s.total} actions "
                    f"— human review should continue."
                )

            if suggested != current or s.total < 5:
                suggestions.append(
                    PolicySuggestion(
                        action_type=s.action_type,
                        failure_category=s.failure_category,
                        current_policy=current,
                        suggested_policy=suggested,
                        confidence=round(confidence, 4),
                        reason=reason,
                        stats=s,
                    )
                )

        return suggestions

    async def _llm_enhanced_suggestions(
        self,
        stats: list[ActionStats],
        rule_suggestions: list[PolicySuggestion],
    ) -> list[PolicySuggestion]:
        """Use LLM to provide richer reasoning for policy suggestions."""
        assert self._llm is not None

        stats_dicts = [
            {
                "action_type": s.action_type,
                "failure_category": s.failure_category,
                "total": s.total,
                "approved": s.approved,
                "rejected": s.rejected,
                "succeeded": s.succeeded,
                "failed": s.failed,
                "approval_rate": s.approval_rate,
            }
            for s in stats
        ]

        prompt = _USER_PROMPT_TEMPLATE.format(stats_json=json.dumps(stats_dicts, indent=2))

        try:
            response = await self._llm.complete(
                prompt=prompt,
                system=_SYSTEM_PROMPT,
                max_tokens=1024,
                temperature=0.0,
            )

            llm_suggestions = self._parse_llm_response(response.content, stats)

            logger.info(
                "llm_policy_analysis_complete",
                provider=self._llm.provider_name,
                suggestion_count=len(llm_suggestions),
            )

            if llm_suggestions:
                return llm_suggestions

        except Exception as e:
            logger.warning("llm_policy_analysis_failed", error=str(e))

        # Fall back to rule-based suggestions
        return rule_suggestions

    def _parse_llm_response(self, content: str, stats: list[ActionStats]) -> list[PolicySuggestion]:
        """Parse LLM JSON response into PolicySuggestion objects."""
        content = content.strip()
        if content.startswith("```"):
            lines = content.split("\n")
            content = "\n".join(line for line in lines if not line.strip().startswith("```"))

        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            logger.warning("llm_policy_parse_error", content=content[:200])
            return []

        if not isinstance(data, list):
            return []

        # Build a lookup for stats
        stats_map = {(s.action_type, s.failure_category): s for s in stats}

        valid_policies = {p.value for p in PolicyClassification}
        suggestions: list[PolicySuggestion] = []

        for item in data:
            at = str(item.get("action_type", ""))
            fc = str(item.get("failure_category", ""))
            suggested = str(item.get("suggested_policy", ""))
            confidence = float(item.get("confidence", 0.5))
            reason = str(item.get("reason", "LLM suggestion"))

            if suggested not in valid_policies:
                continue

            matched_stats = stats_map.get((at, fc))
            if not matched_stats:
                continue

            try:
                action_type_enum = ActionType(at)
                current = classify_action(action_type_enum).value
            except ValueError:
                current = PolicyClassification.REQUIRES_APPROVAL.value

            suggestions.append(
                PolicySuggestion(
                    action_type=at,
                    failure_category=fc,
                    current_policy=current,
                    suggested_policy=suggested,
                    confidence=max(0.0, min(1.0, confidence)),
                    reason=reason,
                    stats=matched_stats,
                )
            )

        return suggestions
