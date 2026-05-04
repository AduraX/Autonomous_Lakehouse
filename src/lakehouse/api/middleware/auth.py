"""Keycloak JWT authentication middleware.

Validates Bearer tokens issued by the KF4X Keycloak instance.
Extracts user identity and roles for RBAC enforcement.

Flow:
  1. Client sends request with `Authorization: Bearer <token>`
  2. Middleware fetches Keycloak's JWKS (cached) to get signing keys
  3. Decodes and validates the JWT (signature, expiry, issuer, audience)
  4. Extracts user info and roles from token claims
  5. Injects `CurrentUser` into the request state for route handlers

In development mode (KEYCLOAK_URL not set), authentication is skipped
and a default dev user is injected.
"""

from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

import jwt as pyjwt
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from lakehouse.config import get_settings
from lakehouse.logging import get_logger

logger = get_logger(__name__)

bearer_scheme = HTTPBearer(auto_error=False)


@dataclass(frozen=True)
class CurrentUser:
    """Authenticated user extracted from a Keycloak JWT."""

    sub: str  # Keycloak user ID
    email: str
    username: str
    roles: frozenset[str] = field(default_factory=frozenset)

    @property
    def is_admin(self) -> bool:
        return "admin" in self.roles


# Default user for development mode (no Keycloak configured)
_DEV_USER = CurrentUser(
    sub="dev-user-001",
    email="dev@localhost",
    username="dev",
    roles=frozenset({"admin", "operator"}),
)


@lru_cache(maxsize=1)
def _get_jwks_client() -> pyjwt.PyJWKClient | None:
    """Create a cached JWKS client for Keycloak token verification.

    Returns None if Keycloak is not configured (development mode).
    """
    settings = get_settings()
    if not settings.keycloak_url:
        return None

    jwks_url = (
        f"{settings.keycloak_url.rstrip('/')}"
        f"/realms/{settings.keycloak_realm}"
        f"/protocol/openid-connect/certs"
    )
    return pyjwt.PyJWKClient(jwks_url, cache_keys=True)


def _extract_roles(token_payload: dict[str, object]) -> frozenset[str]:
    """Extract roles from Keycloak token claims.

    Keycloak puts roles in:
      - realm_access.roles (realm-level roles)
      - resource_access.<client_id>.roles (client-level roles)
    """
    roles: set[str] = set()

    # Realm roles
    realm_access = token_payload.get("realm_access")
    if isinstance(realm_access, dict):
        realm_roles = realm_access.get("roles")
        if isinstance(realm_roles, list):
            roles.update(realm_roles)

    # Client roles
    settings = get_settings()
    resource_access = token_payload.get("resource_access")
    if isinstance(resource_access, dict):
        client_access = resource_access.get(settings.keycloak_client_id)
        if isinstance(client_access, dict):
            client_roles = client_access.get("roles")
            if isinstance(client_roles, list):
                roles.update(client_roles)

    return frozenset(roles)


async def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> CurrentUser:
    """FastAPI dependency — extracts and validates the authenticated user.

    Usage:
        @router.get("/protected")
        async def protected_route(user: CurrentUser = Depends(get_current_user)):
            ...
    """
    settings = get_settings()
    jwks_client = _get_jwks_client()

    # Development mode: skip auth when Keycloak is not configured
    if not jwks_client:
        logger.debug("auth_skipped", reason="keycloak not configured (dev mode)")
        return _DEV_USER

    # Require token in production
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials

    try:
        signing_key = jwks_client.get_signing_key_from_jwt(token)

        payload = pyjwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience=settings.keycloak_client_id,
            issuer=(f"{settings.keycloak_url.rstrip('/')}/realms/{settings.keycloak_realm}"),
            options={
                "verify_exp": True,
                "verify_aud": True,
                "verify_iss": True,
            },
        )
    except pyjwt.ExpiredSignatureError as err:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
        ) from err
    except pyjwt.InvalidTokenError as err:
        logger.warning("auth_token_invalid", error=str(err))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token",
        ) from err

    user = CurrentUser(
        sub=str(payload.get("sub", "")),
        email=str(payload.get("email", "")),
        username=str(payload.get("preferred_username", payload.get("sub", ""))),
        roles=_extract_roles(payload),
    )

    logger.debug("auth_success", user=user.username, roles=list(user.roles))
    return user


def require_role(role: str) -> Any:
    """Dependency factory — require a specific role for access.

    Usage:
        @router.post("/admin-only", dependencies=[Depends(require_role("admin"))])
        async def admin_route():
            ...
    """

    async def _check_role(user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
        if role not in user.roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Required role: {role}",
            )
        return user

    return _check_role
