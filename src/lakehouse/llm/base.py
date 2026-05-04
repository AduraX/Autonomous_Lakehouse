"""LLM provider protocol — the contract all providers must implement.

Supports both remote APIs (Claude, OpenAI) and local models (Ollama, vLLM)
through a single interface. Providers handle authentication, serialization,
and error handling internally.
"""

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class LLMResponse:
    """Standardized response from any LLM provider."""

    content: str
    model: str
    provider: str
    usage: dict[str, int] = field(default_factory=dict)  # input_tokens, output_tokens
    raw: dict[str, Any] | None = None  # Original provider response for debugging


class LLMProvider(Protocol):
    """Interface for LLM providers.

    Every provider (Claude, OpenAI, Ollama, vLLM) implements this protocol.
    The diagnosis agent calls these methods without knowing which model is behind them.
    """

    @property
    def provider_name(self) -> str:
        """Human-readable provider name (e.g., 'claude', 'ollama')."""
        ...

    @property
    def model_name(self) -> str:
        """Model identifier (e.g., 'claude-sonnet-4-20250514', 'llama3:8b')."""
        ...

    async def complete(
        self,
        prompt: str,
        system: str | None = None,
        max_tokens: int = 1024,
        temperature: float = 0.0,
    ) -> LLMResponse:
        """Send a completion request to the model.

        Args:
            prompt: The user message / prompt text.
            system: Optional system prompt for role/behavior instructions.
            max_tokens: Maximum tokens in the response.
            temperature: Sampling temperature (0.0 = deterministic).

        Returns:
            Standardized LLMResponse.

        Raises:
            ConnectionError: If the provider is unreachable.
            ValueError: If the request is malformed.
        """
        ...

    async def is_available(self) -> bool:
        """Check if the provider is reachable and configured."""
        ...
