"""FastAPI application factory.

Uses the factory pattern so the app can be configured differently
for production, testing, and development.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from lakehouse.config import get_settings
from lakehouse.logging import get_logger, setup_logging
from lakehouse.telemetry import setup_tracing

logger = get_logger(__name__)

limiter = Limiter(key_func=get_remote_address, default_limits=["120/minute"])


def _rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> JSONResponse:
    return JSONResponse(
        status_code=429,
        content={"detail": f"Rate limit exceeded: {exc.detail}"},
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Startup and shutdown lifecycle hooks."""
    settings = get_settings()
    setup_logging(settings.app_log_level)
    setup_tracing()
    logger.info("application_starting", env=settings.app_env.value)
    yield
    logger.info("application_shutting_down")


def create_app() -> FastAPI:
    """Build and return the FastAPI application."""
    settings = get_settings()

    app = FastAPI(
        title="Autonomous Lakehouse Operations Platform",
        description="AI control plane for self-healing data pipelines",
        version="0.1.0",
        debug=settings.is_development,
        lifespan=lifespan,
    )

    # --- Rate limiting ---
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]

    # --- OpenTelemetry FastAPI instrumentation ---
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app)
    except Exception:
        logger.debug("opentelemetry_instrumentation_unavailable")

    # --- Mount Prometheus metrics endpoint ---
    metrics_app = make_asgi_app()
    app.mount("/metrics", metrics_app)

    # --- Register API routes ---
    from lakehouse.api.routes.actions import router as actions_router
    from lakehouse.api.routes.chat import router as chat_router
    from lakehouse.api.routes.coordination import router as coordination_router
    from lakehouse.api.routes.cost import router as cost_router
    from lakehouse.api.routes.diagnoses import router as diagnoses_router
    from lakehouse.api.routes.events import router as events_router
    from lakehouse.api.routes.health import router as health_router
    from lakehouse.api.routes.policy import router as policy_router
    from lakehouse.api.routes.polling import router as polling_router
    from lakehouse.api.routes.quality import router as quality_router

    app.include_router(health_router, tags=["Health"])
    app.include_router(events_router, prefix="/api/v1", tags=["Pipeline Events"])
    app.include_router(diagnoses_router, prefix="/api/v1", tags=["Diagnoses"])
    app.include_router(actions_router, prefix="/api/v1", tags=["Actions"])
    app.include_router(coordination_router, prefix="/api/v1", tags=["Coordination"])
    app.include_router(polling_router, prefix="/api/v1", tags=["Polling"])
    app.include_router(quality_router, prefix="/api/v1", tags=["Data Quality"])
    app.include_router(cost_router, prefix="/api/v1", tags=["Cost Optimization"])
    app.include_router(policy_router, prefix="/api/v1", tags=["Adaptive Policy"])
    app.include_router(chat_router, prefix="/api/v1", tags=["Chat Operations"])

    return app
