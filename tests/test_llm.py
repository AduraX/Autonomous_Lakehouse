"""Tests for LLM integration.

Provider abstraction, diagnosis agent, summarizer,
hybrid coordinator. Uses a mock LLM provider that returns
canned JSON responses, so no real API calls or local model
servers are needed.
"""

import json
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from lakehouse.agents.coordinator import CoordinatorAgent
from lakehouse.agents.llm_diagnosis import LLMDiagnosisAgent
from lakehouse.agents.summarizer import IncidentSummarizer
from lakehouse.llm.base import LLMResponse
from lakehouse.models.base import Base
from lakehouse.models.diagnoses import FailureCategory
from lakehouse.models.events import PipelineEvent, PipelineStatus, Platform

# ============================================================
# Mock LLM Provider
# ============================================================


class MockLLMProvider:
    """Mock LLM provider for testing — returns configurable canned responses."""

    def __init__(
        self,
        response_content: str = "",
        should_fail: bool = False,
    ) -> None:
        self._response_content = response_content
        self._should_fail = should_fail
        self.call_count = 0

    @property
    def provider_name(self) -> str:
        return "mock"

    @property
    def model_name(self) -> str:
        return "mock-model-v1"

    async def complete(
        self,
        prompt: str,
        system: str | None = None,
        max_tokens: int = 1024,
        temperature: float = 0.0,
    ) -> LLMResponse:
        self.call_count += 1
        if self._should_fail:
            raise ConnectionError("Mock LLM connection failed")
        return LLMResponse(
            content=self._response_content,
            model="mock-model-v1",
            provider="mock",
            usage={"input_tokens": 100, "output_tokens": 50},
        )

    async def is_available(self) -> bool:
        return not self._should_fail


def _diagnosis_json(
    category: str = "timeout",
    confidence: float = 0.92,
    summary: str = "Connection timeout to source database",
) -> str:
    """Generate a valid LLM diagnosis JSON response."""
    return json.dumps(
        {
            "category": category,
            "confidence": confidence,
            "summary": summary,
            "reasoning": "The error message indicates a connection timeout to the source database.",
            "recommendations": [
                "Check source database availability",
                "Increase connection timeout settings",
            ],
        }
    )


# ============================================================
# Fixtures
# ============================================================


@pytest.fixture
async def session() -> AsyncGenerator[AsyncSession, None]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        yield s
    await engine.dispose()


async def _insert_event(
    session: AsyncSession,
    error_message: str = "Something completely unexpected happened",
    status: PipelineStatus = PipelineStatus.FAILED,
) -> PipelineEvent:
    event = PipelineEvent(
        external_run_id="llm-test-001",
        pipeline_name="ingest_sales",
        platform=Platform.MOCK,
        status=status,
        error_message=error_message,
    )
    session.add(event)
    await session.flush()
    await session.refresh(event)
    return event


# ============================================================
# LLM Provider Tests
# ============================================================


async def test_mock_provider_returns_response() -> None:
    """Mock provider should return the configured content."""
    provider = MockLLMProvider(response_content="hello world")
    response = await provider.complete("test prompt")
    assert response.content == "hello world"
    assert response.provider == "mock"
    assert response.usage["input_tokens"] == 100


async def test_mock_provider_tracks_calls() -> None:
    """Mock provider should track call count."""
    provider = MockLLMProvider(response_content="ok")
    await provider.complete("one")
    await provider.complete("two")
    assert provider.call_count == 2


async def test_mock_provider_fails_on_demand() -> None:
    """Mock provider should raise when configured to fail."""
    provider = MockLLMProvider(should_fail=True)
    with pytest.raises(ConnectionError):
        await provider.complete("test")


async def test_mock_provider_availability() -> None:
    """is_available should reflect failure configuration."""
    assert await MockLLMProvider().is_available() is True
    assert await MockLLMProvider(should_fail=True).is_available() is False


# ============================================================
# LLM Diagnosis Agent Tests
# ============================================================


async def test_llm_diagnosis_parses_valid_json() -> None:
    """Should parse a valid JSON diagnosis from the LLM."""
    provider = MockLLMProvider(response_content=_diagnosis_json())
    agent = LLMDiagnosisAgent(provider)

    finding = await agent.diagnose(
        pipeline_name="ingest_sales",
        platform="mock",
        error_message="Connection timeout after 300s",
    )

    assert finding.category == FailureCategory.TIMEOUT
    assert finding.confidence == 0.92
    assert "timeout" in finding.summary.lower()
    assert "llm_diagnosis" in finding.agent_name
    assert finding.details.get("recommendations") is not None


async def test_llm_diagnosis_handles_schema_mismatch() -> None:
    """Should correctly parse schema_mismatch diagnosis."""
    provider = MockLLMProvider(
        response_content=_diagnosis_json(
            category="schema_mismatch",
            confidence=0.95,
            summary="Column 'revenue' type changed from DECIMAL to VARCHAR",
        )
    )
    agent = LLMDiagnosisAgent(provider)

    finding = await agent.diagnose(
        pipeline_name="etl_daily",
        platform="adf",
        error_message="Column type mismatch",
    )

    assert finding.category == FailureCategory.SCHEMA_MISMATCH
    assert finding.confidence == 0.95


async def test_llm_diagnosis_handles_invalid_json() -> None:
    """Should fall back to UNKNOWN when LLM returns invalid JSON."""
    provider = MockLLMProvider(response_content="This is not JSON at all")
    agent = LLMDiagnosisAgent(provider)

    finding = await agent.diagnose(
        pipeline_name="test",
        platform="mock",
        error_message="error",
    )

    assert finding.category == FailureCategory.UNKNOWN
    assert finding.confidence < 0.5


async def test_llm_diagnosis_handles_markdown_fences() -> None:
    """Should strip markdown code fences from LLM response."""
    wrapped = f"```json\n{_diagnosis_json()}\n```"
    provider = MockLLMProvider(response_content=wrapped)
    agent = LLMDiagnosisAgent(provider)

    finding = await agent.diagnose(
        pipeline_name="test",
        platform="mock",
        error_message="timeout",
    )

    assert finding.category == FailureCategory.TIMEOUT


async def test_llm_diagnosis_handles_connection_failure() -> None:
    """Should return fallback finding when LLM is unreachable."""
    provider = MockLLMProvider(should_fail=True)
    agent = LLMDiagnosisAgent(provider)

    finding = await agent.diagnose(
        pipeline_name="test",
        platform="mock",
        error_message="some error",
    )

    assert finding.category == FailureCategory.UNKNOWN
    assert finding.details.get("fallback") is True


async def test_llm_diagnosis_handles_unknown_category() -> None:
    """Should default to UNKNOWN for invalid category values."""
    provider = MockLLMProvider(
        response_content=_diagnosis_json(
            category="not_a_real_category",
        )
    )
    agent = LLMDiagnosisAgent(provider)

    finding = await agent.diagnose(
        pipeline_name="test",
        platform="mock",
        error_message="error",
    )

    assert finding.category == FailureCategory.UNKNOWN


async def test_llm_diagnosis_clamps_confidence() -> None:
    """Confidence should be clamped to [0.0, 1.0]."""
    provider = MockLLMProvider(response_content=_diagnosis_json(confidence=1.5))
    agent = LLMDiagnosisAgent(provider)

    finding = await agent.diagnose(
        pipeline_name="test",
        platform="mock",
        error_message="error",
    )

    assert finding.confidence == 1.0


# ============================================================
# Incident Summarizer Tests
# ============================================================


async def test_summarizer_with_llm() -> None:
    """Summarizer should use LLM when available."""
    provider = MockLLMProvider(
        response_content="Pipeline ingest_sales failed due to a connection timeout."
    )
    summarizer = IncidentSummarizer(provider=provider)

    summary = await summarizer.summarize_incident(
        pipeline_name="ingest_sales",
        platform="adf",
        status="failed",
        error_message="Connection timeout",
        diagnosis_category="timeout",
        diagnosis_confidence=0.9,
        diagnosis_summary="Connection timeout to source",
        actions=[{"action_type": "retry_pipeline", "description": "Retry"}],
    )

    assert "ingest_sales" in summary
    assert provider.call_count == 1


async def test_summarizer_template_fallback() -> None:
    """Without LLM, summarizer should use template-based summary."""
    summarizer = IncidentSummarizer(provider=None)

    summary = await summarizer.summarize_incident(
        pipeline_name="ingest_sales",
        platform="adf",
        status="failed",
        error_message="timeout",
        diagnosis_category="timeout",
        diagnosis_confidence=0.9,
        diagnosis_summary="Timeout detected",
        actions=[{"action_type": "retry_pipeline", "description": "Retry"}],
    )

    assert "ingest_sales" in summary
    assert "timeout" in summary
    assert "90%" in summary


async def test_summarizer_llm_failure_falls_back() -> None:
    """Should fall back to template when LLM fails."""
    provider = MockLLMProvider(should_fail=True)
    summarizer = IncidentSummarizer(provider=provider)

    summary = await summarizer.summarize_incident(
        pipeline_name="test_pipeline",
        platform="mock",
        status="failed",
        error_message="error",
        diagnosis_category="unknown",
        diagnosis_confidence=0.4,
        diagnosis_summary="Unknown failure",
        actions=[],
    )

    assert "test_pipeline" in summary


async def test_cost_summary_with_llm() -> None:
    """Should generate cost anomaly summary via LLM."""
    provider = MockLLMProvider(
        response_content="Alert: Pipeline etl_daily took 500s, 150% over baseline."
    )
    summarizer = IncidentSummarizer(provider=provider)

    summary = await summarizer.summarize_cost_anomaly(
        pipeline_name="etl_daily",
        anomaly_type="duration_spike",
        severity="high",
        message="Duration spike detected",
        avg_duration=200.0,
        current_duration=500.0,
        recommendations=["Scale compute", "Review partitioning"],
    )

    assert "etl_daily" in summary


async def test_cost_summary_template_fallback() -> None:
    """Cost summary should fall back to template without LLM."""
    summarizer = IncidentSummarizer(provider=None)

    summary = await summarizer.summarize_cost_anomaly(
        pipeline_name="etl_daily",
        anomaly_type="duration_spike",
        severity="high",
        message="Duration spike",
        avg_duration=200.0,
        current_duration=500.0,
        recommendations=["Scale compute"],
    )

    assert "etl_daily" in summary
    assert "500" in summary
    assert "200" in summary


# ============================================================
# Hybrid Coordinator Tests
# ============================================================


async def test_hybrid_uses_rules_when_confident(
    session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When rules produce high confidence, LLM should NOT be called."""
    monkeypatch.setenv("LLM_ENABLED", "true")
    monkeypatch.setenv("LLM_CONFIDENCE_THRESHOLD", "0.7")
    from lakehouse.config import get_settings

    get_settings.cache_clear()

    event = await _insert_event(
        session,
        error_message="Schema mismatch on column 'amount': expected DECIMAL, got STRING",
    )
    llm_provider = MockLLMProvider(response_content=_diagnosis_json())
    adapter = AsyncMock()
    adapter.get_pipeline_logs = AsyncMock(return_value=[])
    adapter.get_pipeline_metadata = AsyncMock(return_value={})

    coordinator = CoordinatorAgent(
        adapter=adapter,
        session=session,
        llm_provider=llm_provider,
    )
    result = await coordinator.coordinate(event.id)

    # Rules should handle this with >0.7 confidence — LLM not called
    assert result.findings[0].category == FailureCategory.SCHEMA_MISMATCH
    assert llm_provider.call_count == 0


async def test_hybrid_escalates_to_llm_for_unknown(
    session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When rules produce UNKNOWN, LLM should be called."""
    monkeypatch.setenv("LLM_ENABLED", "true")
    monkeypatch.setenv("LLM_CONFIDENCE_THRESHOLD", "0.7")
    from lakehouse.config import get_settings

    get_settings.cache_clear()

    event = await _insert_event(
        session,
        error_message="SparkException: Job aborted due to stage failure",
    )
    llm_provider = MockLLMProvider(
        response_content=_diagnosis_json(
            category="resource_exhaustion",
            confidence=0.88,
            summary="Spark job failed due to executor OOM",
        )
    )
    adapter = AsyncMock()
    adapter.get_pipeline_logs = AsyncMock(return_value=[])
    adapter.get_pipeline_metadata = AsyncMock(return_value={})

    coordinator = CoordinatorAgent(
        adapter=adapter,
        session=session,
        llm_provider=llm_provider,
    )
    result = await coordinator.coordinate(event.id)

    # LLM should have been called and its result used
    assert llm_provider.call_count == 1
    assert result.findings[0].category == FailureCategory.RESOURCE_EXHAUSTION
    assert "llm_diagnosis" in result.findings[0].agent_name


async def test_hybrid_falls_back_when_llm_fails(
    session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When LLM fails, should fall back to rule-based result."""
    monkeypatch.setenv("LLM_ENABLED", "true")
    monkeypatch.setenv("LLM_CONFIDENCE_THRESHOLD", "0.7")
    from lakehouse.config import get_settings

    get_settings.cache_clear()

    event = await _insert_event(
        session,
        error_message="Completely novel error never seen before",
    )
    llm_provider = MockLLMProvider(should_fail=True)
    adapter = AsyncMock()
    adapter.get_pipeline_logs = AsyncMock(return_value=[])
    adapter.get_pipeline_metadata = AsyncMock(return_value={})

    coordinator = CoordinatorAgent(
        adapter=adapter,
        session=session,
        llm_provider=llm_provider,
    )
    result = await coordinator.coordinate(event.id)

    # Should still produce a result (rule-based fallback)
    assert len(result.findings) == 1
    assert result.findings[0].category == FailureCategory.UNKNOWN


async def test_hybrid_no_llm_when_disabled(
    session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When no LLM provider is set, should use rules only."""
    event = await _insert_event(
        session,
        error_message="Novel error with no pattern match",
    )
    adapter = AsyncMock()
    adapter.get_pipeline_logs = AsyncMock(return_value=[])
    adapter.get_pipeline_metadata = AsyncMock(return_value={})

    coordinator = CoordinatorAgent(
        adapter=adapter,
        session=session,
        llm_provider=None,
    )
    result = await coordinator.coordinate(event.id)

    assert len(result.findings) == 1
    assert result.findings[0].category == FailureCategory.UNKNOWN


# ============================================================
# LLM Factory Tests
# ============================================================


def test_factory_returns_none_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Factory should return None when LLM_ENABLED=false."""
    monkeypatch.setenv("LLM_ENABLED", "false")
    from lakehouse.config import get_settings

    get_settings.cache_clear()

    from lakehouse.llm.factory import get_llm_provider

    provider = get_llm_provider()
    assert provider is None


def test_factory_returns_claude_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    """Factory should return ClaudeProvider when configured."""
    monkeypatch.setenv("LLM_ENABLED", "true")
    monkeypatch.setenv("LLM_PROVIDER", "claude")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-fake-key")
    monkeypatch.setenv("LLM_MODEL", "claude-sonnet-4-20250514")
    from lakehouse.config import get_settings

    get_settings.cache_clear()

    from lakehouse.llm.factory import get_llm_provider

    provider = get_llm_provider()
    assert provider is not None
    assert provider.provider_name == "claude"
    assert provider.model_name == "claude-sonnet-4-20250514"


def test_factory_returns_local_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    """Factory should return LocalProvider when configured."""
    monkeypatch.setenv("LLM_ENABLED", "true")
    monkeypatch.setenv("LLM_PROVIDER", "local")
    monkeypatch.setenv("LLM_LOCAL_URL", "http://localhost:11434/v1")
    monkeypatch.setenv("LLM_LOCAL_MODEL", "llama3:8b")
    from lakehouse.config import get_settings

    get_settings.cache_clear()

    from lakehouse.llm.factory import get_llm_provider

    provider = get_llm_provider()
    assert provider is not None
    assert provider.provider_name == "local"
    assert provider.model_name == "llama3:8b"
