"""Async database engine and session management.

Uses SQLAlchemy 2.0 async API with asyncpg driver.
"""

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from lakehouse.config import get_settings

settings = get_settings()

engine = create_async_engine(
    settings.database_url,
    echo=settings.is_development,
    pool_size=20,
    max_overflow=10,
    pool_pre_ping=True,
)

async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Dependency that yields an async database session.

    Usage in FastAPI:
        @router.get("/items")
        async def list_items(session: AsyncSession = Depends(get_session)):
            ...
    """
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
