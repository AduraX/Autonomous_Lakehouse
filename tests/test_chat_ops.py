"""Tests for the Conversational Operations Interface."""

from __future__ import annotations

from unittest.mock import AsyncMock

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from lakehouse.agents.chat_ops import ChatOpsAgent, ChatResponse
from lakehouse.llm.base import LLMResponse
from lakehouse.models.cost import CostBaseline
from lakehouse.models.events import PipelineEvent, PipelineStatus, Platform

# ---------------------------------------------------------------------------
# Intent detection tests
# ---------------------------------------------------------------------------


def test_intent_failure_keywords() -> None:
    """Should detect failure_analysis intent for error-related keywords."""
    agent = ChatOpsAgent()
    assert agent._detect_intent("Why did the pipeline fail?") == "failure_analysis"
    assert agent._detect_intent("Show me recent errors") == "failure_analysis"
    assert agent._detect_intent("Something broke last night") == "failure_analysis"


def test_intent_cost_keywords() -> None:
    """Should detect cost_inquiry intent for cost-related keywords."""
    agent = ChatOpsAgent()
    assert agent._detect_intent("What is the cost of running pipelines?") == "cost_inquiry"
    assert agent._detect_intent("Which pipeline is the most expensive?") == "cost_inquiry"
    assert agent._detect_intent("Why is pipeline X so slow?") == "cost_inquiry"
    assert agent._detect_intent("Show me duration stats") == "cost_inquiry"


def test_intent_action_keywords() -> None:
    """Should detect action_status intent for action-related keywords."""
    agent = ChatOpsAgent()
    assert agent._detect_intent("What actions are pending?") == "action_status"
    assert agent._detect_intent("Did anyone approve the retry?") == "action_status"
    assert agent._detect_intent("Reject the scaling proposal") == "action_status"
    assert agent._detect_intent("Execute the remediation") == "action_status"


def test_intent_pipeline_specific() -> None:
    """Should detect pipeline_specific intent when a pipeline name is mentioned."""
    agent = ChatOpsAgent()
    assert agent._detect_intent("Show me pipeline ingest_sales") == "pipeline_specific"
    assert agent._detect_intent('What about pipeline "daily_etl"?') == "pipeline_specific"


def test_intent_general_fallback() -> None:
    """Should fall back to general intent for unrecognized questions."""
    agent = ChatOpsAgent()
    assert agent._detect_intent("What is going on?") == "general"
    assert agent._detect_intent("Give me a summary") == "general"


# ---------------------------------------------------------------------------
# Database query + LLM tests
# ---------------------------------------------------------------------------


async def test_query_with_mock_llm(db_session: AsyncSession) -> None:
    """Should use LLM to generate a natural language answer."""
    # Seed a failed event
    event = PipelineEvent(
        external_run_id="run-1",
        pipeline_name="ingest_sales",
        platform=Platform.ADF,
        status=PipelineStatus.FAILED,
        error_message="Connection timeout to source DB",
    )
    db_session.add(event)
    await db_session.flush()

    # Mock LLM provider
    mock_provider = AsyncMock()
    mock_provider.provider_name = "mock"
    mock_provider.model_name = "mock-model"
    mock_provider.complete.return_value = LLMResponse(
        content=(
            "The ingest_sales pipeline failed due to a connection timeout to the source database."
        ),
        model="mock-model",
        provider="mock",
        usage={"input_tokens": 100, "output_tokens": 30},
    )

    agent = ChatOpsAgent(llm_provider=mock_provider)
    result = await agent.query("Why did the sales pipeline fail?", db_session)

    assert isinstance(result, ChatResponse)
    assert result.intent == "failure_analysis"
    assert result.llm_used is True
    assert "ingest_sales" in result.answer
    assert len(result.data["failed_events"]) == 1
    mock_provider.complete.assert_awaited_once()


async def test_query_without_llm_fallback(db_session: AsyncSession) -> None:
    """Should return structured data-only response when no LLM is available."""
    event = PipelineEvent(
        external_run_id="run-2",
        pipeline_name="transform_revenue",
        platform=Platform.FABRIC,
        status=PipelineStatus.FAILED,
        error_message="Schema mismatch",
    )
    db_session.add(event)
    await db_session.flush()

    agent = ChatOpsAgent(llm_provider=None)
    result = await agent.query("Show me errors", db_session)

    assert result.llm_used is False
    assert result.intent == "failure_analysis"
    assert "transform_revenue" in result.answer
    assert "Schema mismatch" in result.answer


async def test_query_empty_results(db_session: AsyncSession) -> None:
    """Should handle empty database gracefully."""
    agent = ChatOpsAgent(llm_provider=None)
    result = await agent.query("Why did things fail?", db_session)

    assert result.intent == "failure_analysis"
    assert result.llm_used is False
    assert "No recent failures" in result.answer
    assert result.data["failed_events"] == []


async def test_query_cost_inquiry(db_session: AsyncSession) -> None:
    """Should query cost baselines for cost-related questions."""
    baseline = CostBaseline(
        pipeline_name="heavy_etl",
        avg_duration=300.0,
        min_duration=200.0,
        max_duration=500.0,
        stddev_duration=50.0,
        total_runs=100,
        total_failures=5,
        total_successes=95,
        total_duration_seconds=30000.0,
    )
    db_session.add(baseline)
    await db_session.flush()

    agent = ChatOpsAgent(llm_provider=None)
    result = await agent.query("Which pipeline is the most expensive?", db_session)

    assert result.intent == "cost_inquiry"
    assert len(result.data["cost_baselines"]) == 1
    assert "heavy_etl" in result.answer


async def test_query_general_overview(db_session: AsyncSession) -> None:
    """Should return general overview for unrecognized questions."""
    for i in range(3):
        db_session.add(
            PipelineEvent(
                external_run_id=f"run-g-{i}",
                pipeline_name=f"pipeline_{i}",
                platform=Platform.MOCK,
                status=PipelineStatus.SUCCEEDED,
            )
        )
    await db_session.flush()

    agent = ChatOpsAgent(llm_provider=None)
    result = await agent.query("What is going on?", db_session)

    assert result.intent == "general"
    assert result.data["total_events"] == 3
    assert result.data["total_failures"] == 0


# ---------------------------------------------------------------------------
# API endpoint test
# ---------------------------------------------------------------------------


async def test_chat_api_endpoint(client: AsyncClient) -> None:
    """Should accept a question via POST and return a ChatResponse."""
    # Seed an event first
    await client.post(
        "/api/v1/events",
        json={
            "external_run_id": "run-api-1",
            "pipeline_name": "api_test_pipeline",
            "platform": "mock",
            "status": "failed",
            "error_message": "Disk full",
        },
    )

    # LLM is disabled in test env, so we expect data-only fallback
    response = await client.post(
        "/api/v1/chat",
        json={
            "question": "Why did api_test_pipeline fail?",
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["intent"] == "failure_analysis"
    assert isinstance(data["answer"], str)
    assert isinstance(data["data"], dict)
    assert data["llm_used"] is False
