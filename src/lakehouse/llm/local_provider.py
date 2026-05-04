"""Local LLM provider — connects to Ollama, vLLM, llama.cpp, or any
OpenAI-compatible local inference server.

Most local model servers expose an OpenAI-compatible API:
  - Ollama: http://localhost:11434/v1/chat/completions
  - vLLM: http://localhost:8000/v1/chat/completions
  - llama.cpp server: http://localhost:8080/v1/chat/completions
  - LM Studio: http://localhost:1234/v1/chat/completions

This provider talks to that API using httpx (no openai SDK dependency).

Requires:
  - LLM_LOCAL_URL environment variable (e.g., http://localhost:11434/v1)
  - LLM_LOCAL_MODEL environment variable (e.g., llama3:8b, mistral:7b)
  - A running local inference server

Usage:
  provider = LocalProvider()
  response = await provider.complete("Diagnose this error: ...")
"""

import httpx

from lakehouse.config import get_settings
from lakehouse.llm.base import LLMResponse
from lakehouse.logging import get_logger

logger = get_logger(__name__)


class LocalProvider:
    """OpenAI-compatible local model provider (Ollama, vLLM, llama.cpp, etc.)."""

    def __init__(
        self,
        base_url: str | None = None,
        model: str | None = None,
        api_key: str | None = None,
    ) -> None:
        settings = get_settings()
        self._base_url = (base_url or settings.llm_local_url).rstrip("/")
        self._model = model or settings.llm_local_model
        # Some servers require an API key, others don't
        self._api_key = api_key or settings.llm_local_api_key

    @property
    def provider_name(self) -> str:
        return "local"

    @property
    def model_name(self) -> str:
        return self._model

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        return headers

    async def complete(
        self,
        prompt: str,
        system: str | None = None,
        max_tokens: int = 1024,
        temperature: float = 0.0,
    ) -> LLMResponse:
        """Send a chat completion request to the local model server."""
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": self._model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "stream": False,
        }

        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                resp = await client.post(
                    f"{self._base_url}/chat/completions",
                    json=payload,
                    headers=self._headers(),
                )
                resp.raise_for_status()
                data = resp.json()
        except httpx.ConnectError as e:
            logger.error("local_llm_connection_error", url=self._base_url, error=str(e))
            raise ConnectionError(f"Local LLM server unreachable at {self._base_url}: {e}") from e
        except httpx.HTTPStatusError as e:
            logger.error("local_llm_http_error", status=e.response.status_code, error=str(e))
            raise ConnectionError(f"Local LLM server error ({e.response.status_code}): {e}") from e

        # Parse OpenAI-compatible response
        choices = data.get("choices", [])
        content = choices[0]["message"]["content"] if choices else ""

        usage_data = data.get("usage", {})
        usage = {
            "input_tokens": usage_data.get("prompt_tokens", 0),
            "output_tokens": usage_data.get("completion_tokens", 0),
        }

        logger.info(
            "local_llm_completion",
            model=self._model,
            input_tokens=usage["input_tokens"],
            output_tokens=usage["output_tokens"],
        )

        return LLMResponse(
            content=content,
            model=self._model,
            provider="local",
            usage=usage,
            raw=data,
        )

    async def is_available(self) -> bool:
        """Check if the local model server is reachable."""
        if not self._base_url:
            return False
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                # Try OpenAI-compatible models endpoint
                resp = await client.get(
                    f"{self._base_url}/models",
                    headers=self._headers(),
                )
                return resp.status_code == 200
        except Exception:
            return False
