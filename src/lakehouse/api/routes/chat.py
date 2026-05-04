"""Chat Operations API route — conversational interface to the lakehouse."""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from lakehouse.agents.chat_ops import ChatOpsAgent
from lakehouse.api.dependencies import get_db
from lakehouse.llm.factory import get_llm_provider
from lakehouse.logging import get_logger
from lakehouse.schemas.chat import ChatRequest, ChatResponseSchema

router = APIRouter()
logger = get_logger(__name__)


@router.post("/chat", response_model=ChatResponseSchema)
async def chat(
    payload: ChatRequest,
    session: AsyncSession = Depends(get_db),
) -> ChatResponseSchema:
    """Answer a natural language question about the lakehouse.

    Uses the configured LLM provider when available. Falls back to
    structured data-only responses if no LLM is configured.
    """
    provider = get_llm_provider()
    agent = ChatOpsAgent(llm_provider=provider)
    result = await agent.query(payload.question, session)

    logger.info(
        "chat_query",
        intent=result.intent,
        llm_used=result.llm_used,
        question_length=len(payload.question),
    )

    return ChatResponseSchema(
        answer=result.answer,
        intent=result.intent,
        data=result.data,
        llm_used=result.llm_used,
    )
