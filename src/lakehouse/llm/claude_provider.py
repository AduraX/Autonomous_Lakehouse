"""Claude LLM provider — uses the Anthropic SDK for remote inference.

Supports Claude Opus, Sonnet, and Haiku models via the Anthropic API.

Requires:
  - ANTHROPIC_API_KEY environment variable
  - anthropic SDK installed (pip install anthropic)

Usage:
  provider = ClaudeProvider(model="claude-sonnet-4-20250514")
  response = await provider.complete("Diagnose this error: ...")
"""

from typing import Any

from lakehouse.config import get_settings
from lakehouse.llm.base import LLMResponse
from lakehouse.logging import get_logger

logger = get_logger(__name__)


class ClaudeProvider:
    """Anthropic Claude API provider."""

    def __init__(self, model: str = "claude-sonnet-4-20250514") -> None:
        settings = get_settings()
        self._api_key = settings.anthropic_api_key
        self._model = model

    @property
    def provider_name(self) -> str:
        return "claude"

    @property
    def model_name(self) -> str:
        return self._model

    async def complete(
        self,
        prompt: str,
        system: str | None = None,
        max_tokens: int = 1024,
        temperature: float = 0.0,
    ) -> LLMResponse:
        """Send a completion request to Claude via the Anthropic API."""
        try:
            import anthropic
        except ImportError as err:
            raise ImportError("anthropic SDK not installed. Run: pip install anthropic") from err

        client = anthropic.AsyncAnthropic(api_key=self._api_key)

        kwargs: dict[str, Any] = {
            "model": self._model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": [{"role": "user", "content": prompt}],
        }
        if system:
            kwargs["system"] = system

        try:
            response = await client.messages.create(**kwargs)

            content = ""
            for block in response.content:
                if block.type == "text":
                    content += block.text

            usage = {
                "input_tokens": response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
            }

            logger.info(
                "claude_completion",
                model=self._model,
                input_tokens=usage["input_tokens"],
                output_tokens=usage["output_tokens"],
            )

            return LLMResponse(
                content=content,
                model=self._model,
                provider="claude",
                usage=usage,
                raw={"id": response.id, "stop_reason": response.stop_reason},
            )

        except anthropic.APIConnectionError as e:
            logger.error("claude_connection_error", error=str(e))
            raise ConnectionError(f"Claude API unreachable: {e}") from e
        except anthropic.AuthenticationError as e:
            logger.error("claude_auth_error", error=str(e))
            raise ValueError(f"Claude API authentication failed: {e}") from e
        except anthropic.APIError as e:
            logger.error("claude_api_error", error=str(e))
            raise ConnectionError(f"Claude API error: {e}") from e

    async def is_available(self) -> bool:
        """Check if the Claude API is configured and reachable."""
        if not self._api_key:
            return False
        try:
            response = await self.complete(
                "Reply with OK",
                max_tokens=5,
                temperature=0.0,
            )
            return bool(response.content)
        except Exception:
            return False
