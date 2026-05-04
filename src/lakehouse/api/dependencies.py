"""Shared FastAPI dependencies."""

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

from lakehouse.database import get_session


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Database session dependency — use in route functions."""
    async for session in get_session():
        yield session
