"""Pydantic schemas for the Conversational Operations Interface."""

from typing import Any

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    """Incoming natural language question."""

    question: str = Field(
        ...,
        min_length=1,
        max_length=2000,
        examples=["Why did the sales pipeline fail?"],
    )


class ChatResponseSchema(BaseModel):
    """Response from the ChatOps agent."""

    answer: str
    intent: str
    data: dict[str, Any]
    llm_used: bool
