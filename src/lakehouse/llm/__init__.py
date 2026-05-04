"""LLM provider abstraction — supports local and remote models."""

from lakehouse.llm.base import LLMProvider, LLMResponse
from lakehouse.llm.factory import get_llm_provider

__all__ = ["LLMProvider", "LLMResponse", "get_llm_provider"]
