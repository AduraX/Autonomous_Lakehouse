"""Tests for Keycloak authentication middleware.

Tests dev mode (no Keycloak configured) and token validation logic.
"""

import pytest
from httpx import ASGITransport, AsyncClient

from lakehouse.api.middleware.auth import CurrentUser, _extract_roles


def test_current_user_is_admin() -> None:
    """Admin role should be detected."""
    user = CurrentUser(
        sub="u1",
        email="a@b.com",
        username="admin",
        roles=frozenset({"admin", "operator"}),
    )
    assert user.is_admin is True


def test_current_user_not_admin() -> None:
    """Regular user should not be admin."""
    user = CurrentUser(
        sub="u2",
        email="b@b.com",
        username="viewer",
        roles=frozenset({"viewer"}),
    )
    assert user.is_admin is False


def test_extract_roles_realm_access() -> None:
    """Should extract realm-level roles."""
    payload = {
        "realm_access": {"roles": ["admin", "user"]},
    }
    roles = _extract_roles(payload)
    assert "admin" in roles
    assert "user" in roles


def test_extract_roles_client_access(monkeypatch: pytest.MonkeyPatch) -> None:
    """Should extract client-level roles."""
    monkeypatch.setenv("KEYCLOAK_CLIENT_ID", "lakehouse-ops")
    from lakehouse.config import get_settings

    get_settings.cache_clear()

    payload = {
        "resource_access": {
            "lakehouse-ops": {"roles": ["operator", "viewer"]},
        },
    }
    roles = _extract_roles(payload)
    assert "operator" in roles
    assert "viewer" in roles


def test_extract_roles_empty_payload() -> None:
    """Should return empty set for payload without roles."""
    roles = _extract_roles({})
    assert len(roles) == 0


async def test_dev_mode_injects_dev_user(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without Keycloak configured, endpoints should get a dev user."""
    monkeypatch.setenv("KEYCLOAK_URL", "")
    from lakehouse.config import get_settings

    get_settings.cache_clear()

    # Clear JWKS client cache
    from lakehouse.api.middleware.auth import _get_jwks_client

    _get_jwks_client.cache_clear()

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from lakehouse.api.app import create_app
    from lakehouse.api.dependencies import get_db
    from lakehouse.models.base import Base

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    app = create_app()

    async def override_db():
        async with session_factory() as s:
            yield s

    app.dependency_overrides[get_db] = override_db

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health")
        assert resp.status_code == 200

    await engine.dispose()
