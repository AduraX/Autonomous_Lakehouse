"""Tests for health check endpoints."""

from httpx import AsyncClient


async def test_health_check(client: AsyncClient) -> None:
    """Liveness probe should return healthy."""
    response = await client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"


async def test_readiness_check(client: AsyncClient) -> None:
    """Readiness probe should return ready with environment info."""
    response = await client.get("/health/ready")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ready"
    assert "env" in data
    assert "platform" in data
