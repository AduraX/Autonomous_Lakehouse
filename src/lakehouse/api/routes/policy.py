"""Adaptive Policy Learning API routes — stats, suggestions, and configuration."""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from lakehouse.agents.policy_learner import PolicyLearner
from lakehouse.api.dependencies import get_db
from lakehouse.logging import get_logger
from lakehouse.models.actions import ActionType
from lakehouse.policies import classify_action
from lakehouse.schemas.policy import (
    ActionStatsResponse,
    PolicyAnalysisResponse,
    PolicyConfigEntry,
    PolicyConfigResponse,
    PolicySuggestionResponse,
)

router = APIRouter()
logger = get_logger(__name__)


@router.get("/policy/stats", response_model=list[ActionStatsResponse])
async def get_policy_stats(
    session: AsyncSession = Depends(get_db),
) -> list[ActionStatsResponse]:
    """Get action approval/rejection statistics grouped by action_type and failure_category."""
    learner = PolicyLearner()
    stats = await learner.get_action_stats(session)
    return [
        ActionStatsResponse(
            action_type=s.action_type,
            failure_category=s.failure_category,
            total=s.total,
            approved=s.approved,
            rejected=s.rejected,
            succeeded=s.succeeded,
            failed=s.failed,
            approval_rate=s.approval_rate,
        )
        for s in stats
    ]


@router.get("/policy/suggestions", response_model=PolicyAnalysisResponse)
async def get_policy_suggestions(
    session: AsyncSession = Depends(get_db),
) -> PolicyAnalysisResponse:
    """Analyze action history and suggest policy changes."""
    learner = PolicyLearner()
    suggestions = await learner.analyze_approval_history(session)
    stats = await learner.get_action_stats(session)

    suggestion_responses = [
        PolicySuggestionResponse(
            action_type=s.action_type,
            failure_category=s.failure_category,
            current_policy=s.current_policy,
            suggested_policy=s.suggested_policy,
            confidence=s.confidence,
            reason=s.reason,
            stats=ActionStatsResponse(
                action_type=s.stats.action_type,
                failure_category=s.stats.failure_category,
                total=s.stats.total,
                approved=s.stats.approved,
                rejected=s.stats.rejected,
                succeeded=s.stats.succeeded,
                failed=s.stats.failed,
                approval_rate=s.stats.approval_rate,
            ),
        )
        for s in suggestions
    ]

    auto_count = sum(1 for s in suggestions if s.suggested_policy == "auto_approved")
    forbid_count = sum(1 for s in suggestions if s.suggested_policy == "forbidden")
    keep_count = sum(1 for s in suggestions if s.suggested_policy == "requires_approval")

    summary_parts = []
    if auto_count:
        summary_parts.append(f"{auto_count} action(s) suggested for auto-approval")
    if forbid_count:
        summary_parts.append(f"{forbid_count} action(s) suggested to forbid")
    if keep_count:
        summary_parts.append(f"{keep_count} action(s) should keep requiring approval")
    summary = "; ".join(summary_parts) if summary_parts else "No policy changes suggested."

    return PolicyAnalysisResponse(
        suggestions=suggestion_responses,
        total_groups_analyzed=len(stats),
        suggestions_count=len(suggestions),
        summary=summary,
    )


@router.get("/policy/current", response_model=PolicyConfigResponse)
async def get_current_policy() -> PolicyConfigResponse:
    """Get the current static policy configuration for all action types."""
    policies = []
    auto_approved = []
    requires_approval = []
    forbidden = []

    for action_type in ActionType:
        classification = classify_action(action_type)
        policies.append(
            PolicyConfigEntry(
                action_type=action_type.value,
                policy=classification.value,
            )
        )
        if classification.value == "auto_approved":
            auto_approved.append(action_type.value)
        elif classification.value == "requires_approval":
            requires_approval.append(action_type.value)
        elif classification.value == "forbidden":
            forbidden.append(action_type.value)

    return PolicyConfigResponse(
        policies=policies,
        auto_approved=auto_approved,
        requires_approval=requires_approval,
        forbidden=forbidden,
    )
