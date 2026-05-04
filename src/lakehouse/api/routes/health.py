"""Health check endpoints."""

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from lakehouse.api.dependencies import get_db
from lakehouse.config import get_settings

router = APIRouter()


@router.get("/health")
async def health_check() -> dict[str, str]:
    """Basic liveness probe — returns 200 if the process is running."""
    return {"status": "healthy"}


@router.get("/health/ready")
async def readiness_check(
    session: AsyncSession = Depends(get_db),
) -> dict[str, str]:
    """Readiness probe — verifies database connectivity."""
    await session.execute(text("SELECT 1"))
    settings = get_settings()
    return {
        "status": "ready",
        "env": settings.app_env.value,
        "platform": settings.platform_adapter.value,
    }
