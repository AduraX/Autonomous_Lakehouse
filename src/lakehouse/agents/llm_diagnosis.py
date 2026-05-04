"""LLM-powered Diagnosis Agent — intelligent failure classification.

Uses an LLM (Claude or local model) to analyze error messages, logs, and
pipeline metadata to produce root-cause diagnoses. Handles novel failure
modes that the rule-based pattern matcher can't classify.

Design:
  - Structured prompt with JSON output schema
  - Parses LLM response into AgentFinding (same type as rule-based diagnosis)
  - Graceful fallback to UNKNOWN if LLM response is unparseable
  - Token usage tracked for cost monitoring
"""

import json

from lakehouse.llm.base import LLMProvider, LLMResponse
from lakehouse.logging import get_logger
from lakehouse.models.diagnoses import FailureCategory
from lakehouse.schemas.coordination import AgentFinding

logger = get_logger(__name__)

_VALID_CATEGORIES = {c.value for c in FailureCategory}

_SYSTEM_PROMPT = (
    "You are a data engineering incident diagnosis agent "
    "for an autonomous lakehouse operations platform.\n\n"
    "Your job is to analyze pipeline failure information "
    "and classify the root cause.\n\n"
    "You MUST respond with a valid JSON object matching "
    "this exact schema:\n"
    "{\n"
    '  "category": "<one of: schema_mismatch, '
    "source_unavailable, sink_unavailable, timeout, "
    "permission_denied, data_quality, resource_exhaustion, "
    'configuration_error, dependency_failure, unknown>",\n'
    '  "confidence": <float between 0.0 and 1.0>,\n'
    '  "summary": "<concise 1-2 sentence diagnosis>",\n'
    '  "reasoning": "<brief explanation of why you chose '
    'this category>",\n'
    '  "recommendations": ["<action 1>", "<action 2>"]\n'
    "}\n\n"
    "Rules:\n"
    "- Choose the MOST SPECIFIC category that fits. "
    'Avoid "unknown" unless truly unclassifiable.\n'
    "- Confidence should reflect how certain you are: "
    "0.9+ for clear-cut, 0.6-0.8 for probable, "
    "below 0.6 for guesses.\n"
    "- The summary should be actionable — an engineer "
    "reading it should know what to investigate.\n"
    "- Recommendations should be specific actions, "
    "not generic advice.\n"
    "- Respond with ONLY the JSON object. "
    "No markdown, no explanation outside the JSON."
)

_USER_PROMPT_TEMPLATE = (
    "Diagnose the following pipeline failure:\n\n"
    "**Pipeline:** {pipeline_name}\n"
    "**Platform:** {platform}\n"
    "**Error Message:** {error_message}\n"
    "**Error Code:** {error_code}\n\n"
    "**Recent Logs:**\n"
    "{logs}\n\n"
    "**Pipeline Metadata:**\n"
    "{metadata}\n\n"
    "Analyze the error message, logs, and metadata "
    "to determine the root cause. "
    "Respond with the JSON diagnosis."
)


class LLMDiagnosisAgent:
    """Uses an LLM to diagnose pipeline failures with structured output."""

    def __init__(self, provider: LLMProvider) -> None:
        self._provider = provider

    async def diagnose(
        self,
        pipeline_name: str,
        platform: str,
        error_message: str,
        error_code: str = "",
        logs: list[str] | None = None,
        metadata: dict[str, object] | None = None,
    ) -> AgentFinding:
        """Send failure context to the LLM and parse a structured diagnosis.

        Returns an AgentFinding compatible with the coordinator's output format.
        Falls back to UNKNOWN if the LLM response is unparseable.
        """
        log_text = "\n".join(logs or ["(no logs available)"])
        meta_text = json.dumps(metadata or {}, indent=2, default=str)

        prompt = _USER_PROMPT_TEMPLATE.format(
            pipeline_name=pipeline_name,
            platform=platform,
            error_message=error_message or "(no error message)",
            error_code=error_code or "(none)",
            logs=log_text[:3000],  # Truncate to avoid token limits
            metadata=meta_text[:1000],
        )

        try:
            response = await self._provider.complete(
                prompt=prompt,
                system=_SYSTEM_PROMPT,
                max_tokens=512,
                temperature=0.0,
            )
            finding = self._parse_response(response)

            logger.info(
                "llm_diagnosis_completed",
                provider=self._provider.provider_name,
                model=self._provider.model_name,
                category=finding.category.value,
                confidence=finding.confidence,
                input_tokens=response.usage.get("input_tokens", 0),
                output_tokens=response.usage.get("output_tokens", 0),
            )
            return finding

        except ConnectionError as e:
            logger.warning("llm_diagnosis_connection_error", error=str(e))
            return self._fallback_finding(error_message, str(e))

        except Exception as e:
            logger.error("llm_diagnosis_unexpected_error", error=str(e))
            return self._fallback_finding(error_message, str(e))

    def _parse_response(self, response: LLMResponse) -> AgentFinding:
        """Parse the LLM's JSON response into an AgentFinding."""
        content = response.content.strip()

        # Strip markdown code fences if present
        if content.startswith("```"):
            lines = content.split("\n")
            content = "\n".join(line for line in lines if not line.strip().startswith("```"))

        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            logger.warning("llm_response_parse_error", content=content[:200])
            return AgentFinding(
                agent_name=f"llm_diagnosis:{self._provider.provider_name}",
                category=FailureCategory.UNKNOWN,
                confidence=0.3,
                summary="LLM response was not valid JSON — falling back to unknown",
                details={"raw_response": content[:500], "model": response.model},
            )

        # Validate and extract fields
        raw_category = str(data.get("category", "unknown")).lower()
        category = (
            FailureCategory(raw_category)
            if raw_category in _VALID_CATEGORIES
            else FailureCategory.UNKNOWN
        )

        confidence = float(data.get("confidence", 0.5))
        confidence = max(0.0, min(1.0, confidence))

        summary = str(data.get("summary", "LLM diagnosis (no summary provided)"))
        reasoning = str(data.get("reasoning", ""))
        recommendations = data.get("recommendations", [])

        return AgentFinding(
            agent_name=f"llm_diagnosis:{self._provider.provider_name}",
            category=category,
            confidence=confidence,
            summary=summary,
            details={
                "reasoning": reasoning,
                "recommendations": recommendations,
                "model": response.model,
                "provider": response.provider,
                "tokens_used": response.usage,
            },
        )

    @staticmethod
    def _fallback_finding(error_message: str, error: str) -> AgentFinding:
        """Generate a fallback finding when the LLM is unavailable."""
        return AgentFinding(
            agent_name="llm_diagnosis:fallback",
            category=FailureCategory.UNKNOWN,
            confidence=0.2,
            summary=f"LLM unavailable — manual review needed. Error: {error_message[:200]}",
            details={"llm_error": error, "fallback": True},
        )
