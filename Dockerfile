# ============================================================
# Stage 1: Build — install dependencies
# ============================================================
FROM python:3.12-slim AS builder

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml README.md ./
RUN uv pip install --system -e "."

# ============================================================
# Stage 2: Runtime — minimal production image
# ============================================================
FROM python:3.12-slim AS runtime

WORKDIR /app

# Install only runtime system deps (no gcc)
RUN apt-get update && \
    apt-get install -y --no-install-recommends libpq5 && \
    rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application source
COPY src/ ./src/
COPY alembic.ini ./
COPY alembic/ ./alembic/

ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

# Run as non-root
RUN useradd --create-home appuser
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import httpx; r = httpx.get('http://localhost:8000/health'); r.raise_for_status()"

CMD ["uvicorn", "lakehouse.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
