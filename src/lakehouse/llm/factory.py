"""LLM provider factory — selects the configured provider.

Reads LLM_PROVIDER from config to determine which provider to instantiate.
Supports: claude, local, none (disabled).
"""

from lakehouse.config import get_settings
from lakehouse.llm.base import LLMProvider
from lakehouse.logging import get_logger

logger = get_logger(__name__)


def get_llm_provider() -> LLMProvider | None:
    """Return the configured LLM provider, or None if LLM is disabled.

    Provider selection:
      - "claude" → ClaudeProvider (Anthropic API)
      - "local"  → LocalProvider (Ollama/vLLM/llama.cpp)
      - "none"   → None (LLM disabled, rule-based only)
    """
    settings = get_settings()

    if not settings.llm_enabled:
        logger.info("llm_disabled", reason="LLM_ENABLED=false")
        return None

    match settings.llm_provider:
        case "claude":
            from lakehouse.llm.claude_provider import ClaudeProvider

            provider: LLMProvider = ClaudeProvider(model=settings.llm_model)
            logger.info("llm_provider_loaded", provider="claude", model=settings.llm_model)
            return provider

        case "local":
            from lakehouse.llm.local_provider import LocalProvider

            provider = LocalProvider()
            logger.info(
                "llm_provider_loaded",
                provider="local",
                model=settings.llm_local_model,
                url=settings.llm_local_url,
            )
            return provider

        case _:
            logger.warning("llm_provider_unknown", provider=settings.llm_provider)
            return None
