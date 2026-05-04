"""Tests for Adaptive Policy Learning — PolicyLearner agent and API endpoints."""

from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from lakehouse.agents.policy_learner import PolicyLearner
from lakehouse.llm.base import LLMResponse
from lakehouse.models.actions import Action, ActionStatus, ActionType
from lakehouse.models.diagnoses import Diagnosis, FailureCategory
from lakehouse.models.events import PipelineEvent, PipelineStatus, Platform

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _seed_actions(
    session: AsyncSession,
    action_type: ActionType,
    category: FailureCategory,
    statuses: list[ActionStatus],
) -> None:
    """Create a PipelineEvent, Diagnosis, and Actions with given statuses."""
    event = PipelineEvent(
        external_run_id=f"run-{action_type.value}-{category.value}",
        pipeline_name=f"pipeline-{action_type.value}",
        platform=Platform.MOCK,
        status=PipelineStatus.FAILED,
        error_message="test error",
    )
    session.add(event)
    await session.flush()

    diag = Diagnosis(
        event_id=event.id,
        category=category,
        confidence=0.9,
        summary="Test diagnosis",
        agent_name="test",
    )
    session.add(diag)
    await session.flush()

    for status in statuses:
        action = Action(
            diagnosis_id=diag.id,
            action_type=action_type,
            description=f"Test action {status.value}",
            status=status,
            executed_by="test",
        )
        session.add(action)

    await session.flush()


def _make_mock_llm(response_content: str) -> AsyncMock:
    """Create a mock LLM provider returning the given content."""
    mock = AsyncMock()
    mock.provider_name = "mock"
    mock.model_name = "mock-model"
    mock.complete = AsyncMock(
        return_value=LLMResponse(
            content=response_content,
            model="mock-model",
            provider="mock",
            usage={"input_tokens": 100, "output_tokens": 50},
        )
    )
    return mock


# ---------------------------------------------------------------------------
# Stats calculation tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_action_stats_empty(db_session: AsyncSession) -> None:
    """No actions should return an empty stats list."""
    learner = PolicyLearner()
    stats = await learner.get_action_stats(db_session)
    assert stats == []


@pytest.mark.asyncio
async def test_get_action_stats_counts(db_session: AsyncSession) -> None:
    """Stats should correctly count approved, rejected, succeeded, failed."""
    await _seed_actions(
        db_session,
        ActionType.RETRY_PIPELINE,
        FailureCategory.TIMEOUT,
        [
            ActionStatus.APPROVED,
            ActionStatus.APPROVED,
            ActionStatus.SUCCEEDED,
            ActionStatus.REJECTED,
            ActionStatus.FAILED,
        ],
    )
    await db_session.commit()

    learner = PolicyLearner()
    stats = await learner.get_action_stats(db_session)

    assert len(stats) == 1
    s = stats[0]
    assert s.action_type == "retry_pipeline"
    assert s.failure_category == "timeout"
    assert s.total == 5
    assert s.approved == 3  # APPROVED(2) + SUCCEEDED(1) counted as approved
    assert s.rejected == 1
    assert s.succeeded == 1
    assert s.failed == 1
    assert 0.59 < s.approval_rate < 0.61  # 3/5 = 0.6


# ---------------------------------------------------------------------------
# Suggestion threshold tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_suggest_auto_approve_high_approval_rate(db_session: AsyncSession) -> None:
    """Approval rate >90% with >=10 actions should suggest auto-approve."""
    statuses = [ActionStatus.APPROVED] * 10 + [ActionStatus.REJECTED]
    await _seed_actions(db_session, ActionType.RETRY_PIPELINE, FailureCategory.TIMEOUT, statuses)
    await db_session.commit()

    learner = PolicyLearner()
    suggestions = await learner.analyze_approval_history(db_session)

    auto_suggestions = [s for s in suggestions if s.suggested_policy == "auto_approved"]
    assert len(auto_suggestions) >= 1
    assert auto_suggestions[0].action_type == "retry_pipeline"
    assert auto_suggestions[0].confidence > 0.8


@pytest.mark.asyncio
async def test_suggest_forbid_high_rejection_rate(db_session: AsyncSession) -> None:
    """Rejection rate >80% with >=5 actions should suggest forbidden."""
    statuses = [ActionStatus.REJECTED] * 9 + [ActionStatus.APPROVED]
    await _seed_actions(
        db_session, ActionType.SCALE_COMPUTE, FailureCategory.RESOURCE_EXHAUSTION, statuses
    )
    await db_session.commit()

    learner = PolicyLearner()
    suggestions = await learner.analyze_approval_history(db_session)

    forbid_suggestions = [s for s in suggestions if s.suggested_policy == "forbidden"]
    assert len(forbid_suggestions) >= 1
    assert forbid_suggestions[0].action_type == "scale_compute"
    assert forbid_suggestions[0].confidence > 0.8


@pytest.mark.asyncio
async def test_suggest_keep_requires_approval_mixed(db_session: AsyncSession) -> None:
    """Mixed approval rates should not suggest a change (already requires_approval)."""
    statuses = [ActionStatus.APPROVED] * 4 + [ActionStatus.REJECTED] * 3
    await _seed_actions(
        db_session, ActionType.BLOCK_DOWNSTREAM, FailureCategory.DEPENDENCY_FAILURE, statuses
    )
    await db_session.commit()

    learner = PolicyLearner()
    suggestions = await learner.analyze_approval_history(db_session)

    # Mixed approval rate with current policy already requires_approval
    # means no change is suggested — this is correct behavior.
    block_suggestions = [s for s in suggestions if s.action_type == "block_downstream"]
    assert len(block_suggestions) == 0, (
        "No policy change should be suggested when mixed rate matches current tier"
    )


# ---------------------------------------------------------------------------
# LLM integration tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_with_llm_provider(db_session: AsyncSession) -> None:
    """When LLM is available, its suggestions should be used."""
    statuses = [ActionStatus.APPROVED] * 12
    await _seed_actions(
        db_session, ActionType.NOTIFY_OWNER, FailureCategory.SCHEMA_MISMATCH, statuses
    )
    await db_session.commit()

    llm_response = (
        '[{"action_type": "notify_owner", '
        '"failure_category": "schema_mismatch", '
        '"suggested_policy": "auto_approved", '
        '"confidence": 0.95, '
        '"reason": "Notifications are safe to auto-approve."}]'
    )
    mock_llm = _make_mock_llm(llm_response)

    learner = PolicyLearner(llm_provider=mock_llm)
    suggestions = await learner.analyze_approval_history(db_session)

    mock_llm.complete.assert_awaited_once()
    assert len(suggestions) >= 1
    assert suggestions[0].reason == "Notifications are safe to auto-approve."


@pytest.mark.asyncio
async def test_without_llm_rule_based_only(db_session: AsyncSession) -> None:
    """Without LLM, should return rule-based suggestions only."""
    statuses = [ActionStatus.APPROVED] * 15
    await _seed_actions(db_session, ActionType.RETRY_PIPELINE, FailureCategory.TIMEOUT, statuses)
    await db_session.commit()

    learner = PolicyLearner(llm_provider=None)
    suggestions = await learner.analyze_approval_history(db_session)

    assert len(suggestions) >= 1
    assert (
        "approval rate" in suggestions[0].reason.lower() or "Approval rate" in suggestions[0].reason
    )


@pytest.mark.asyncio
async def test_llm_fallback_on_error(db_session: AsyncSession) -> None:
    """When LLM fails, should fall back to rule-based suggestions."""
    statuses = [ActionStatus.APPROVED] * 12
    await _seed_actions(db_session, ActionType.RETRY_PIPELINE, FailureCategory.TIMEOUT, statuses)
    await db_session.commit()

    mock_llm = _make_mock_llm("")
    mock_llm.complete = AsyncMock(side_effect=ConnectionError("LLM unavailable"))

    learner = PolicyLearner(llm_provider=mock_llm)
    suggestions = await learner.analyze_approval_history(db_session)

    # Should still get rule-based suggestions
    assert len(suggestions) >= 1


@pytest.mark.asyncio
async def test_empty_history_no_suggestions(db_session: AsyncSession) -> None:
    """Empty action history should produce no suggestions."""
    learner = PolicyLearner()
    suggestions = await learner.analyze_approval_history(db_session)
    assert suggestions == []


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_api_policy_stats(client, db_session: AsyncSession) -> None:
    """GET /api/v1/policy/stats should return action statistics."""
    await _seed_actions(
        db_session,
        ActionType.RETRY_PIPELINE,
        FailureCategory.TIMEOUT,
        [ActionStatus.APPROVED, ActionStatus.REJECTED],
    )
    await db_session.commit()

    response = await client.get("/api/v1/policy/stats")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_api_policy_suggestions(client, db_session: AsyncSession) -> None:
    """GET /api/v1/policy/suggestions should return policy analysis."""
    response = await client.get("/api/v1/policy/suggestions")
    assert response.status_code == 200
    data = response.json()
    assert "suggestions" in data
    assert "total_groups_analyzed" in data
    assert "suggestions_count" in data
    assert "summary" in data


@pytest.mark.asyncio
async def test_api_policy_current(client) -> None:
    """GET /api/v1/policy/current should return the current configuration."""
    response = await client.get("/api/v1/policy/current")
    assert response.status_code == 200
    data = response.json()
    assert "policies" in data
    assert "auto_approved" in data
    assert "requires_approval" in data
    assert "forbidden" in data
    assert "notify_owner" in data["auto_approved"]
    assert "retry_pipeline" in data["requires_approval"]
