# Autonomous Lakehouse Operations Platform — Claude Notes

> This file captures all conversations, recommendations, bash commands, and decisions made throughout the build process.

---

## Session 1 — 2026-04-28

### Project Summary

The **Autonomous Lakehouse Operations Platform** is a production-grade AI control plane for lakehouse platforms (Databricks, Microsoft Fabric, Azure Data Factory) that can:

- Detect pipeline failures and diagnose root causes
- Validate data quality (schema drift, freshness, nulls, volume anomalies)
- Recommend or execute recovery actions (self-healing pipelines)
- Monitor cost and performance, recommend optimizations
- Expose everything through observability dashboards (Prometheus, Grafana, OpenTelemetry)

### Current State

The project is in the **planning/specification phase**. Only the architectural blueprint (`agentic_orchestrator.md`) exists. No code has been implemented yet.

---

### Architecture Overview

```
Lakehouse Platform (Fabric / ADF / Databricks)
        |
Event + Log Collector
        |
Agentic Control Plane (Coordinator)
        |
Specialist Agents (parallel processing)
  - Pipeline Recovery Agent
  - Data Quality Agent
  - Cost Optimization Agent
  - Observability Agent
        |
Action Engine
        |
Human Approval / Auto Remediation
```

### Tech Stack (Recommended)

| Layer              | Technology                                      |
|--------------------|--------------------------------------------------|
| API Framework      | FastAPI                                          |
| Database           | PostgreSQL + Alembic (migrations)                |
| Task Queue         | Celery or arq (Redis-backed)                     |
| Platform Adapters  | ADF, Microsoft Fabric, Databricks                |
| Data Quality       | Great Expectations, Soda Core, Deequ, custom SQL |
| Observability      | Prometheus, Grafana, OpenTelemetry               |
| Containerization   | Docker, Docker Compose                           |
| Orchestration      | Kubernetes + Helm                                |
| Logging            | Structured JSON logs                             |

### Directory Structure (Planned)

```
autonomous-lakehouse-ops/
|
├── apps/
│   ├── api/              # FastAPI REST endpoints
│   ├── worker/           # Async task processor (Celery/arq)
│   └── dashboard/        # Grafana dashboards
|
├── packages/
│   ├── agents/           # All specialist agents
│   │   ├── coordinator/
│   │   ├── pipeline_recovery/
│   │   ├── data_quality/
│   │   ├── cost_optimizer/
│   │   └── observability/
│   │
│   ├── platform_adapters/
│   │   ├── fabric/
│   │   ├── adf/
│   │   └── databricks/
│   │
│   ├── action_engine/    # Executes remediation actions
│   ├── memory/           # Persistent state/policies
│   ├── policies/         # Business rules
│   └── telemetry/        # Observability instrumentation
|
├── infra/
│   ├── docker/
│   ├── kubernetes/
│   ├── helm/
│   └── grafana/
|
├── examples/             # Reference scenarios
│   ├── pipeline_failure/
│   ├── schema_drift/
│   ├── cost_spike/
│   └── dq_failure/
|
└── README.md
```

---

### Implementation Plan — Phased Build Order

#### Phase 0 + 1: Foundation & Core Domain
- Project scaffolding (monorepo, pyproject.toml, Docker Compose)
- Configuration management (Pydantic settings, env-based config)
- Database layer (PostgreSQL + Alembic migrations)
- Async event processing (FastAPI + background workers)
- Structured JSON logging with correlation IDs
- Testing infrastructure (pytest, fixtures, CI-ready)
- Core domain models: Pipeline Event, Diagnosis, Action, Audit Trail

#### Phase 2: Platform Adapters
- Common `PlatformAdapter` protocol (Python Protocol class)
- ADF adapter first (mature REST APIs)
- Fabric adapter second
- Databricks adapter third
- Each handles: auth, event polling, action execution, metadata retrieval

#### Phase 3: Pipeline Recovery Agent
- Failure classification (timeout, schema mismatch, source/sink unavailable)
- Retry logic with exponential backoff
- Root-cause diagnosis with confidence scores
- Propose-action flow with human approval gate

#### Phase 4: Data Quality Agent
- Schema drift detection
- Freshness checks
- Null/missing value checks
- Volume anomaly detection
- Integration with Great Expectations / Soda Core

#### Phase 5: Cost Optimization Agent
- Run duration baseline tracking
- Compute/cost anomaly detection
- Optimization recommendations (parallelism, partitioning, caching)

#### Phase 6: Observability & Deployment
- Prometheus metrics in every agent
- Grafana dashboards for KPIs
- OpenTelemetry distributed tracing
- Kubernetes manifests + Helm charts

---

### Best Practices Enforced

| Area             | Practice                                                                 |
|------------------|--------------------------------------------------------------------------|
| Architecture     | Clean separation — adapters, agents, API are independent packages        |
| Type Safety      | Pydantic models everywhere, strict typing, no `Any` types               |
| Testing          | Unit + integration tests, real Postgres via testcontainers, no DB mocks  |
| Security         | No secrets in code, Azure AD / service principal auth, RBAC on actions   |
| Resilience       | Exponential backoff retries, circuit breakers, dead-letter queues         |
| Observability    | Structured logs, metrics, traces from day one                            |
| API Design       | Versioned REST endpoints, auto-generated OpenAPI spec, pagination        |
| Database         | All changes via Alembic migrations, no raw SQL in app code               |
| CI/CD            | ruff (lint), black (format), mypy (types), pytest — all in pre-commit    |

---

### Decisions Made

- **Python version:** 3.11+ (agreed)
- **Package manager:** `uv` (user preference)
- **Azure credentials:** Not ready — building with mock adapters first for local dev

---

## Phase 0 + 1 Build Log — 2026-04-28

### What Was Built

Phase 0 (Foundation) and Phase 1 (Core Domain) were completed in a single session. The project now has a fully running API with tests.

### Files Created

```
Autonomous_Lakehouse/
├── pyproject.toml                          # Project config, dependencies, tool settings
├── .gitignore                              # Python/Docker/IDE ignores
├── .env.example                            # Environment variable template
├── README.md                               # Project description
├── Dockerfile                              # Production container (Python 3.11 + uv)
├── docker-compose.yml                      # Local dev: Postgres, Redis, Prometheus, Grafana
├── alembic.ini                             # Alembic migration config
├── alembic/
│   ├── env.py                              # Migration env — loads settings, uses Base metadata
│   ├── script.py.mako                      # Migration template
│   └── versions/                           # (migration files go here)
├── infra/
│   └── prometheus/
│       └── prometheus.yml                  # Prometheus scrape config
├── src/lakehouse/
│   ├── __init__.py
│   ├── config.py                           # Pydantic Settings — all config from env vars
│   ├── logging.py                          # Structured JSON logging via structlog
│   ├── database.py                         # SQLAlchemy async engine + session factory
│   ├── api/
│   │   ├── __init__.py
│   │   ├── app.py                          # FastAPI app factory with lifespan hooks
│   │   ├── dependencies.py                 # Shared DI (database session)
│   │   └── routes/
│   │       ├── __init__.py
│   │       ├── health.py                   # GET /health, GET /health/ready
│   │       ├── events.py                   # POST/GET /api/v1/events
│   │       ├── diagnoses.py               # POST/GET /api/v1/diagnoses
│   │       └── actions.py                  # POST/GET/PATCH /api/v1/actions
│   ├── models/
│   │   ├── __init__.py                     # Re-exports all models
│   │   ├── base.py                         # SQLAlchemy Base with id, created_at, updated_at
│   │   ├── events.py                       # PipelineEvent model
│   │   ├── diagnoses.py                    # Diagnosis model
│   │   ├── actions.py                      # Action model
│   │   └── audit.py                        # AuditLog model
│   ├── schemas/
│   │   ├── __init__.py
│   │   ├── events.py                       # Pydantic request/response schemas
│   │   ├── diagnoses.py
│   │   ├── actions.py
│   │   └── audit.py
│   ├── adapters/
│   │   ├── __init__.py
│   │   ├── base.py                         # PlatformAdapter protocol
│   │   └── mock.py                         # Mock adapter for local dev
│   ├── agents/
│   │   └── __init__.py                     # (agents built in later phases)
│   └── worker/
│       └── __init__.py                     # (background worker built later)
└── tests/
    ├── __init__.py
    ├── conftest.py                         # Fixtures: in-memory SQLite, test client
    ├── test_health.py                      # Health endpoint tests
    ├── test_events.py                      # Event CRUD + pagination + filtering tests
    └── test_actions.py                     # Full workflow test: event→diagnosis→action→approve
```

### Bash Commands Used

```bash
# 1. Create virtual environment with Python 3.11
uv venv --python 3.11
# Creates .venv/ directory with isolated Python 3.11 environment

# 2. Install all project dependencies (including dev tools and SQLite async driver)
uv pip install -e ".[dev]" aiosqlite
# -e ".[dev]" installs the project in editable mode with dev dependencies
# aiosqlite is needed for in-memory SQLite testing (not a prod dependency)

# 3. Run the test suite
PYTHONPATH=src .venv/bin/python -m pytest tests/ -v
# PYTHONPATH=src ensures Python can find the lakehouse package
# -v gives verbose output showing each test name and result
```

### Test Results

```
10 passed in 0.56s

tests/test_actions.py::test_full_workflow PASSED
tests/test_actions.py::test_list_event_diagnoses PASSED
tests/test_actions.py::test_list_diagnosis_actions PASSED
tests/test_events.py::test_ingest_event PASSED
tests/test_events.py::test_get_event PASSED
tests/test_events.py::test_get_event_not_found PASSED
tests/test_events.py::test_list_events_pagination PASSED
tests/test_events.py::test_list_events_filter_by_status PASSED
tests/test_health.py::test_health_check PASSED
tests/test_health.py::test_readiness_check PASSED
```

### API Endpoints Available

| Method | Endpoint                              | Description                        |
|--------|---------------------------------------|------------------------------------|
| GET    | `/health`                             | Liveness probe                     |
| GET    | `/health/ready`                       | Readiness probe (checks DB)        |
| GET    | `/metrics`                            | Prometheus metrics                 |
| POST   | `/api/v1/events`                      | Ingest a pipeline event            |
| GET    | `/api/v1/events`                      | List events (paginated, filtered)  |
| GET    | `/api/v1/events/{id}`                 | Get single event                   |
| POST   | `/api/v1/diagnoses`                   | Create a diagnosis                 |
| GET    | `/api/v1/diagnoses/{id}`              | Get single diagnosis               |
| GET    | `/api/v1/events/{id}/diagnoses`       | List diagnoses for an event        |
| POST   | `/api/v1/actions`                     | Propose an action                  |
| GET    | `/api/v1/actions/{id}`                | Get single action                  |
| PATCH  | `/api/v1/actions/{id}`                | Update action status               |
| GET    | `/api/v1/diagnoses/{id}/actions`      | List actions for a diagnosis       |

### Domain Models

**PipelineEvent** — A pipeline run from any platform (ADF/Fabric/Databricks/Mock)
- Tracks: external_run_id, pipeline_name, platform, status, error info, timing

**Diagnosis** — Root-cause classification for a failed event
- Categories: schema_mismatch, source_unavailable, timeout, permission_denied, data_quality, etc.
- Includes: confidence score (0.0–1.0), summary, agent attribution

**Action** — Proposed or executed remediation
- Types: retry_pipeline, block_downstream, scale_compute, notify_owner, etc.
- Lifecycle: proposed → approved/rejected → executing → succeeded/failed

**AuditLog** — Immutable record of every system decision
- Tracks: event_type, actor, resource, correlation_id

### Key Design Decisions

1. **App Factory pattern** — `create_app()` allows different configs for prod/test/dev
2. **Async everywhere** — SQLAlchemy 2.0 async API with asyncpg driver
3. **In-memory SQLite for tests** — Fast unit tests without Docker dependency
4. **Dependency injection** — Database sessions injected via FastAPI `Depends()`
5. **Protocol-based adapters** — `PlatformAdapter` protocol allows swapping mock/real adapters
6. **Pydantic v2 schemas** — Strict validation, `from_attributes=True` for ORM compatibility
7. **Structured logging** — structlog with JSON output, correlation ID support

### How to Run Locally

```bash
# Start infrastructure (Postgres, Redis, Prometheus, Grafana)
docker compose up -d postgres redis prometheus grafana

# Run migrations (once Alembic migration is generated)
PYTHONPATH=src .venv/bin/python -m alembic upgrade head

# Start the API server
PYTHONPATH=src .venv/bin/python -m uvicorn lakehouse.api.app:create_app --factory --reload

# Run tests
PYTHONPATH=src .venv/bin/python -m pytest tests/ -v

# View API docs
# Open http://localhost:8000/docs (Swagger UI)
```

---

---

## KF4X Integration Analysis — 2026-04-28

### What is KF4X?

**Kubeflow4X (KF4X)** is an enterprise ML platform deployed on Kubernetes with 6 phases:

| Phase | Component | Purpose |
|-------|-----------|---------|
| 1 | Kubeflow + Keycloak | ML pipelines + SSO authentication |
| 2 | SeaweedFS | S3-compatible object storage |
| 3 | MLflow + Feast | Experiment tracking + feature store |
| 4 | Superset + DuckDB | BI dashboards + analytical queries on S3 |
| 5 | Iceberg REST Catalog | Data lakehouse (schema evolution, time-travel, partitions) |
| 6 | ArgoCD | GitOps deployment (push to Git → cluster updates) |

All services share **Keycloak SSO**. Located at:
`/home/oyex/wkspace/claude/kubeflowDir/kubeflowkeycloak/Kubeflow4X/`

### Why Integrate?

The Autonomous Lakehouse becomes the **operations brain** for KF4X. Instead of only monitoring ADF/Fabric/Databricks, it also monitors Kubeflow pipelines, validates Iceberg table quality, and leverages existing KF4X infrastructure rather than duplicating it.

### Integration Points (8 total)

#### 1. Kubeflow Pipelines as a Platform Adapter
Kubeflow has its own pipeline orchestration. We add a **5th adapter** using the Kubeflow Pipelines SDK:

```
src/lakehouse/adapters/
├── base.py          # PlatformAdapter protocol (already built)
├── mock.py          # Already built
├── kubeflow.py      # NEW — monitors Kubeflow Pipeline runs
```

The Kubeflow Pipelines SDK exposes run status, logs, and failure details — mapping directly to our `fetch_recent_events()`, `get_pipeline_logs()`, and `retry_pipeline()` methods.

#### 2. Iceberg Lakehouse — Data Quality Target
KF4X Phase 5 deploys an **Iceberg REST Catalog** with tables in SeaweedFS S3. The Data Quality Agent monitors:
- **Schema drift** — Iceberg tracks schema evolution natively
- **Freshness** — via Iceberg snapshot timestamps
- **Volume anomalies** — via Iceberg partition metrics
- **Queries** — via DuckDB + Iceberg (already in KF4X)

#### 3. Shared PostgreSQL
KF4X runs Postgres for MLflow + Feast. The Autonomous Lakehouse uses Postgres for events/diagnoses/actions/audit. Can share the same instance (separate databases).

#### 4. Keycloak SSO
All KF4X services authenticate through Keycloak. The Autonomous Lakehouse API joins via oauth2-proxy:

| Service | Auth Method |
|---------|-------------|
| Kubeflow | Keycloak via oauth2-proxy |
| MLflow | Keycloak via oauth2-proxy |
| Superset | Keycloak OAuth (built-in) |
| ArgoCD | Keycloak OIDC (built-in) |
| **Lakehouse Ops API** | **Keycloak via oauth2-proxy (new)** |

#### 5. ArgoCD GitOps Deployment
KF4X Phase 6 deploys via ArgoCD. The Autonomous Lakehouse's Kubernetes manifests and Helm charts are managed by the same ArgoCD instance — push to Git, platform updates automatically.

#### 6. MLflow for Agent Model Tracking
If agents evolve to use ML models for failure classification or anomaly detection, MLflow tracks experiments, logs models, and serves them. Agent confidence scores can be backed by models registered in MLflow.

#### 7. Observability Gap Fill
KF4X intentionally does **not** ship Prometheus/Grafana. The Autonomous Lakehouse **does**. This fills the gap — our observability layer monitors both KF4X components and lakehouse operations.

#### 8. Superset BI Dashboards
KF4X's Superset + DuckDB can query the Autonomous Lakehouse's Postgres tables directly — BI dashboards over pipeline health, failure trends, cost metrics, and agent performance without a separate dashboard layer.

### Combined Architecture

*(See README.md for Mermaid architecture diagrams)*

### Implementation Order for KF4X Integration

1. **Kubeflow adapter** — `adapters/kubeflow.py` (monitor KF4X pipelines)
2. **Iceberg DQ integration** — Data Quality Agent validates Iceberg tables
3. **Keycloak auth middleware** — Secure the FastAPI app with Keycloak SSO

### Config Changes Required

New environment variables added for KF4X integration:

```bash
# --- Kubeflow (when PLATFORM_ADAPTER=kubeflow) ---
# KUBEFLOW_HOST=https://kubeflow.example.com
# KUBEFLOW_NAMESPACE=kubeflow-user
# KUBEFLOW_SA_TOKEN=                    # ServiceAccount token for API access

# --- Iceberg (for Data Quality Agent) ---
# ICEBERG_REST_CATALOG_URL=http://iceberg-rest.iceberg.svc:8181
# ICEBERG_WAREHOUSE=s3://iceberg-warehouse
# ICEBERG_S3_ENDPOINT=http://seaweedfs-s3.seaweedfs.svc:8333
# ICEBERG_S3_ACCESS_KEY=
# ICEBERG_S3_SECRET_KEY=

# --- Keycloak (for API authentication) ---
# KEYCLOAK_URL=https://keycloak.example.com
# KEYCLOAK_REALM=kubeflow
# KEYCLOAK_CLIENT_ID=lakehouse-ops
# KEYCLOAK_CLIENT_SECRET=
```

### Models Updated

`Platform` enum in `models/events.py` needs a new value:

```python
class Platform(str, Enum):
    ADF = "adf"
    FABRIC = "fabric"
    DATABRICKS = "databricks"
    KUBEFLOW = "kubeflow"    # NEW
    MOCK = "mock"
```

---

## Phase 2 Build Log — KF4X Integration — 2026-04-28

### What Was Built

All three KF4X integration components completed and tested.

### Files Created / Modified

```
# NEW FILES
src/lakehouse/adapters/kubeflow.py          # Kubeflow Pipelines v2 adapter
src/lakehouse/agents/iceberg_quality.py     # Iceberg DQ agent (schema drift, freshness, volume)
src/lakehouse/api/middleware/__init__.py
src/lakehouse/api/middleware/auth.py        # Keycloak JWT auth + RBAC middleware
tests/test_kubeflow_adapter.py             # 7 tests — mock Kubeflow API responses
tests/test_iceberg_quality.py              # 10 tests — mock Iceberg REST Catalog
tests/test_auth.py                         # 6 tests — JWT validation, role extraction, dev mode

# MODIFIED FILES
src/lakehouse/config.py                    # Added KUBEFLOW to PlatformType enum
                                           # Added kubeflow_*, iceberg_*, keycloak_* settings
src/lakehouse/models/events.py             # Added KUBEFLOW to Platform enum
.env.example                               # Added kubeflow, iceberg, keycloak env vars
pyproject.toml                             # Added PyJWT[crypto] dependency
```

### Kubeflow Adapter (`adapters/kubeflow.py`)

Implements the `PlatformAdapter` protocol using the **Kubeflow Pipelines v2 REST API**:

| Method | What it does |
|--------|-------------|
| `fetch_recent_events()` | Polls `/apis/v2beta1/runs` for recent runs, maps Kubeflow states to `PipelineStatus` |
| `retry_pipeline()` | Fetches original run config, creates a new run with same pipeline spec |
| `cancel_pipeline()` | Calls `:terminate` on a running pipeline |
| `get_pipeline_logs()` | Retrieves run + task details for log output |
| `check_source_availability()` | Hits `/healthz` to verify Kubeflow is reachable |
| `get_pipeline_metadata()` | Searches pipeline catalog by name |

Kubeflow state mapping:
```
SUCCEEDED → succeeded    FAILED/ERROR → failed
RUNNING/PENDING/PAUSED → running    CANCELED/CANCELING/SKIPPED → cancelled
```

### Iceberg Data Quality Agent (`agents/iceberg_quality.py`)

Connects to the **Iceberg REST Catalog** (KF4X Phase 5) and runs three checks:

| Check | What it validates |
|-------|------------------|
| `check_schema_drift()` | Compares current schema against expected columns or previous schema version |
| `check_freshness()` | Checks if latest snapshot timestamp is within `max_age_hours` threshold |
| `check_snapshot_volume()` | Compares `added-records` between last two snapshots for anomalies |

Each check returns a `QualityCheckResult` with status: `PASSED`, `WARNING`, `FAILED`, or `ERROR`.

Also provides:
- `run_all_checks()` — runs all 3 checks on a single table
- `scan_namespace()` — runs all checks on every table in a namespace

### Keycloak Auth Middleware (`api/middleware/auth.py`)

JWT-based authentication using Keycloak's JWKS endpoint:

| Component | Purpose |
|-----------|---------|
| `CurrentUser` | Dataclass with sub, email, username, roles |
| `get_current_user()` | FastAPI dependency — validates JWT, extracts user |
| `require_role()` | Dependency factory — `Depends(require_role("admin"))` |
| Dev mode | When `KEYCLOAK_URL` is empty, injects a dev user with admin+operator roles |

Token validation flow:
1. Extract Bearer token from `Authorization` header
2. Fetch Keycloak JWKS (cached) for signing keys
3. Decode JWT with RS256, verify signature/expiry/issuer/audience
4. Extract roles from `realm_access.roles` and `resource_access.<client>.roles`

### Bash Commands Used

```bash
# 1. Reinstall dependencies (adds PyJWT[crypto] for JWT validation)
uv pip install -e ".[dev]" aiosqlite
# PyJWT[crypto] brings in cryptography for RS256 signature verification

# 2. Run full test suite (now 33 tests)
PYTHONPATH=src .venv/bin/python -m pytest tests/ -v
# Runs all tests including new KF4X integration tests
```

### Test Results

```
33 passed in 0.63s

tests/test_actions.py             3 passed    (unchanged)
tests/test_auth.py                6 passed    (NEW)
tests/test_events.py              5 passed    (unchanged)
tests/test_health.py              2 passed    (unchanged)
tests/test_iceberg_quality.py    10 passed    (NEW)
tests/test_kubeflow_adapter.py    7 passed    (NEW)
```

### Key Design Decisions (Phase 2)

1. **httpx for all adapters** — consistent async HTTP client across Kubeflow, Iceberg, and Keycloak
2. **Mock transports in tests** — `httpx.AsyncBaseTransport` subclasses simulate API responses without network calls
3. **Dev mode bypass** — when `KEYCLOAK_URL` is empty, auth is skipped with a dev user (prevents auth from blocking local development)
4. **JWKS caching** — `@lru_cache` on the JWKS client avoids hitting Keycloak on every request
5. **Iceberg REST Catalog API** — uses the standard spec, compatible with any Iceberg catalog (not just KF4X's)
6. **Kubeflow Pipelines v2** — targets the `v2beta1` API for forward compatibility

---

---

## User Additions — Ported from agentic-orch — 2026-04-28

The user independently added 7 files ported from the agentic-orch project:

| File | Purpose |
|------|---------|
| `agents/coordinator.py` | Orchestrates event → diagnose → propose action pipeline |
| `policies.py` | Static 3-tier policy engine (auto-approve / require approval / forbidden) |
| `adapters/adf.py` | Azure Data Factory adapter stub (mock data, real endpoints commented) |
| `adapters/fabric.py` | Microsoft Fabric adapter stub (mock data, real endpoints commented) |
| `adapters/__init__.py` | Adapter factory — `get_adapter()` with `match/case` on config |
| `api/routes/coordination.py` | `POST /api/v1/coordinate` endpoint |
| `schemas/coordination.py` | Request/response models for coordination |

Also added tests: `test_adf_adapter.py`, `test_fabric_adapter.py`, `test_coordinator.py` (API-level), `test_policies.py`, `test_coordination_api.py`.

### Assessment

All additions are architecturally sound and integrate cleanly:

- **Coordinator Agent** — correct pattern (load event → fetch context → diagnose → propose → persist)
- **Policy Engine** — right for this stage: only NOTIFY_OWNER auto-executes, everything else needs approval
- **ADF/Fabric stubs** — pragmatic: mock data now, swap to real Azure calls via config + credentials later
- **Adapter factory** — clean `match/case` with lazy imports

---

## Coordinator Improvements — 2026-04-28

### What Was Improved

Three changes to the coordinator agent:

#### 1. Error Resilience (`_fetch_adapter_context()`)

Adapter calls (`get_pipeline_logs`, `get_pipeline_metadata`) are now wrapped in try/except. If either fails (network timeout, API error), the coordinator continues with degraded context instead of failing the entire workflow.

```python
# Before: if adapter.get_pipeline_logs() fails, entire coordination fails
logs, metadata = await asyncio.gather(
    self._adapter.get_pipeline_logs(event.external_run_id),
    self._adapter.get_pipeline_metadata(event.pipeline_name),
)

# After: graceful degradation — empty logs/metadata on failure
async def _fetch_adapter_context(self, event):
    logs, metadata = [], {}
    try:
        logs = await self._adapter.get_pipeline_logs(event.external_run_id)
    except Exception as e:
        logger.warning("adapter_logs_failed", ...)
    try:
        metadata = await self._adapter.get_pipeline_metadata(event.pipeline_name)
    except Exception as e:
        logger.warning("adapter_metadata_failed", ...)
    return logs, metadata
```

#### 2. Extracted Persistence Methods

`_persist_diagnosis()` and `_persist_actions()` are now separate methods, making the coordinator more testable and the `coordinate()` method easier to read.

#### 3. Comprehensive Test Coverage (20 new tests)

Added unit-level coordinator tests with mock adapter + in-memory DB:

| Test Category | Tests | What's covered |
|---------------|-------|----------------|
| Workflow | 5 | Full coordination, skip succeeded/running, missing event, persistence |
| Error resilience | 3 | Logs failure, metadata failure, both failures |
| Diagnosis patterns | 7 | Schema, timeout, permission, OOM, unknown, log fallback, duplicate key, dependency |
| Policy tests | 2 | All action types classifiable, forbidden raises error |

### Bash Commands Used

```bash
# Run full test suite (now 82 tests)
PYTHONPATH=src .venv/bin/python -m pytest tests/ -v
# Includes all: health, events, actions, auth, kubeflow, iceberg, ADF, fabric,
# coordinator (unit + API), policies, coordination API
```

### Test Results

```
82 passed in 1.40s

tests/test_actions.py              3 passed
tests/test_adf_adapter.py          6 passed    (user-added)
tests/test_auth.py                 6 passed
tests/test_coordination_api.py     3 passed    (user-added)
tests/test_coordinator.py         20 passed    (7 user-added API tests + 13 NEW unit tests)
tests/test_events.py               5 passed
tests/test_fabric_adapter.py       6 passed    (user-added)
tests/test_health.py               2 passed
tests/test_iceberg_quality.py     10 passed
tests/test_kubeflow_adapter.py     7 passed
tests/test_policies.py            14 passed    (7 user-added + 2 NEW)
```

### Key Design Decisions

1. **Graceful degradation over hard failure** — adapter errors are logged as warnings, not exceptions. The coordinator still diagnoses from the error_message alone.
2. **Two-pass diagnosis** — error_message first (high signal), then error_message + logs combined (broader context). This means a failed `get_pipeline_logs()` only costs the second pass.
3. **Unit tests use `AsyncMock(spec=PlatformAdapter)`** — validates that only protocol methods are called, catches method name typos at test time.

---

---

## Phase 3 Completion — 2026-04-28

### What Was Built

Three remaining Phase 3 components completed:

#### 1. Action Executor (`agents/action_executor.py`)

Executes approved remediation actions via the platform adapter with production-grade retry:

| Feature | Implementation |
|---------|---------------|
| **Exponential backoff** | Base 1s, factor 2x: delays of 1s → 2s → 4s between retries |
| **Max retries** | 3 attempts by default (configurable) |
| **Permanent error detection** | `PermissionError`, `ValueError`, `KeyError` fail immediately — no retries |
| **Transient error retry** | `ConnectionError`, `TimeoutError`, etc. trigger retry |
| **Action dispatch** | Routes `ActionType` to correct adapter method (`retry_pipeline`, `cancel_pipeline`, etc.) |
| **Audit logging** | Every execution (success or failure) writes an `AuditLog` entry |
| **Status lifecycle** | `APPROVED → EXECUTING → SUCCEEDED/FAILED` |

#### 2. Background Worker (`worker/tasks.py`)

arq (Redis-backed) worker with three task types:

| Task | What it does |
|------|-------------|
| `coordinate_event` | Run coordinator on a single event (enqueued when events fail) |
| `execute_action` | Execute an approved action with retry |
| `poll_events` | Poll adapter for new events, ingest them, enqueue coordination for failures |

Worker configuration:
- `max_jobs = 10` — concurrent task limit
- `job_timeout = 300` — 5 minute timeout per task
- Start with: `arq lakehouse.worker.tasks.WorkerSettings`

#### 3. New API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/actions/{id}/execute` | Execute an approved action synchronously |
| POST | `/api/v1/poll` | Poll adapter, ingest events, coordinate failures |

### Files Created / Modified

```
# NEW FILES
src/lakehouse/agents/action_executor.py     # Action execution with exponential backoff retry
src/lakehouse/worker/tasks.py               # arq background worker (3 task types)
src/lakehouse/api/routes/polling.py         # POST /api/v1/poll endpoint
tests/test_action_executor.py               # 8 tests — retry, backoff, permanent errors, audit
tests/test_execute_endpoint.py              # 4 tests — API execution flow
tests/test_polling.py                       # 3 tests — poll + coordinate integration

# MODIFIED FILES
src/lakehouse/api/routes/actions.py         # Added POST /api/v1/actions/{id}/execute
src/lakehouse/api/app.py                    # Registered polling router
```

### Bash Commands Used

```bash
# Run full test suite (now 97 tests)
PYTHONPATH=src .venv/bin/python -m pytest tests/ -v
# All 97 tests pass in ~2 seconds

# Start the background worker (requires Redis running)
PYTHONPATH=src .venv/bin/python -m arq lakehouse.worker.tasks.WorkerSettings
# Connects to Redis, processes coordinate_event, execute_action, poll_events tasks

# Start infrastructure for worker
docker compose up -d redis
```

### Test Results

```
97 passed in 2.07s

tests/test_action_executor.py      8 passed    (NEW)
tests/test_actions.py              3 passed
tests/test_adf_adapter.py          6 passed
tests/test_auth.py                 6 passed
tests/test_coordination_api.py     3 passed
tests/test_coordinator.py         20 passed
tests/test_events.py               5 passed
tests/test_execute_endpoint.py     4 passed    (NEW)
tests/test_fabric_adapter.py       6 passed
tests/test_health.py               2 passed
tests/test_iceberg_quality.py     10 passed
tests/test_kubeflow_adapter.py     7 passed
tests/test_policies.py            14 passed
tests/test_polling.py              3 passed    (NEW)
```

### Full API Endpoint List (Phase 3 Complete)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Liveness probe |
| GET | `/health/ready` | Readiness probe (checks DB) |
| GET | `/metrics` | Prometheus metrics |
| POST | `/api/v1/events` | Ingest a pipeline event |
| GET | `/api/v1/events` | List events (paginated, filtered) |
| GET | `/api/v1/events/{id}` | Get single event |
| GET | `/api/v1/events/{id}/diagnoses` | List diagnoses for an event |
| POST | `/api/v1/diagnoses` | Create a diagnosis |
| GET | `/api/v1/diagnoses/{id}` | Get single diagnosis |
| GET | `/api/v1/diagnoses/{id}/actions` | List actions for a diagnosis |
| POST | `/api/v1/actions` | Propose an action (policy-gated) |
| GET | `/api/v1/actions/{id}` | Get single action |
| PATCH | `/api/v1/actions/{id}` | Update action status (approve/reject) |
| POST | `/api/v1/actions/{id}/execute` | Execute an approved action |
| POST | `/api/v1/coordinate` | Trigger coordinator for an event |
| POST | `/api/v1/poll` | Poll adapter, ingest, coordinate |

### End-to-End Flow (Phase 3)

```
1. POST /api/v1/poll
   → Adapter fetches recent pipeline runs
   → Failed events ingested to DB
   → Coordinator diagnoses each failure (pattern matching on error + logs)
   → Proposes actions (mapped by failure category)
   → Policy engine classifies: auto-approve (NOTIFY_OWNER) or require approval

2. PATCH /api/v1/actions/{id}  {"status": "approved"}
   → Human approves a proposed action

3. POST /api/v1/actions/{id}/execute
   → Executor runs the action via adapter
   → Exponential backoff on transient failures (1s → 2s → 4s)
   → Permanent errors fail immediately
   → Audit log entry written
   → Action marked SUCCEEDED or FAILED
```

### Phase 3 Status: COMPLETE

All Pipeline Recovery Agent functionality is implemented:
- Failure classification (13 patterns)
- Root-cause diagnosis with confidence scores
- Action proposal with policy gating
- Retry execution with exponential backoff
- Background worker for async processing
- Full audit trail
- End-to-end API flow

---

---

## Phase 4: Data Quality Agent — 2026-04-28

### What Was Built

The Iceberg DQ agent (built in Phase 2) is now fully integrated into the platform — exposed via REST API, connected to the remediation pipeline, and wired into the coordinator for post-success checks.

#### 1. Quality API Routes (`api/routes/quality.py`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/quality/namespaces` | List Iceberg catalog namespaces |
| GET | `/api/v1/quality/namespaces/{ns}/tables` | List tables in a namespace |
| POST | `/api/v1/quality/check` | Run DQ checks on a single table |
| POST | `/api/v1/quality/scan` | Scan all tables in a namespace |

Returns 503 if `ICEBERG_REST_CATALOG_URL` is not configured.

#### 2. Quality Coordinator (`agents/quality_coordinator.py`)

Bridges DQ failures into the remediation pipeline:

```
DQ Check Fails → Create PipelineEvent (status=failed, pipeline="dq_check:ns.table")
               → Create Diagnosis (category=DATA_QUALITY, confidence=0.95)
               → Propose Actions (based on check type):
                   schema_drift  → BLOCK_DOWNSTREAM + NOTIFY_OWNER
                   freshness     → RETRY_PIPELINE + NOTIFY_OWNER
                   volume_anomaly → BLOCK_DOWNSTREAM + NOTIFY_OWNER
               → Apply Policy (NOTIFY_OWNER auto-approved, others need approval)
```

DQ failures flow through the same audit trail, approval gates, and execution pipeline as pipeline failures.

#### 3. Coordinator Post-Success DQ Checks

The coordinator now accepts an optional `quality_agent` parameter. When set:
- **Failed events** → diagnosed and remediated (unchanged)
- **Succeeded events** → DQ checks run on referenced Iceberg tables
- **Running events** → skipped (unchanged)

Table references extracted from event metadata (`iceberg_namespace` + `iceberg_table` keys) or pipeline name format (`dq_check:namespace.table`).

#### 4. Quality Pydantic Schemas (`schemas/quality.py`)

| Schema | Purpose |
|--------|---------|
| `QualityCheckRequest` | Single table check request (namespace, table, expected_columns, max_age_hours) |
| `QualityScanRequest` | Namespace scan request |
| `QualityCheckResultResponse` | Individual check result |
| `QualityTableReport` | Per-table summary (passed/warnings/failed/errors) |
| `QualityCheckResponse` | Single table check response with actions_proposed count |
| `QualityScanResponse` | Namespace scan response with per-table reports |

### Files Created / Modified

```
# NEW FILES
src/lakehouse/schemas/quality.py             # Quality API request/response schemas
src/lakehouse/agents/quality_coordinator.py  # DQ → remediation pipeline bridge
src/lakehouse/api/routes/quality.py          # Quality REST endpoints
tests/test_quality_api.py                    # 6 tests — API endpoints + 503 handling
tests/test_quality_coordinator.py            # 7 tests — DQ→actions + deduplication

# MODIFIED FILES
src/lakehouse/agents/coordinator.py          # Added quality_agent param, _handle_success(), _extract_table_ref()
src/lakehouse/api/app.py                     # Registered quality router
```

### Bash Commands Used

```bash
# Run full test suite (now 110 tests)
PYTHONPATH=src .venv/bin/python -m pytest tests/ -v
# All 110 tests pass in ~2.4 seconds
```

### Test Results

```
110 passed in 2.39s

tests/test_action_executor.py       8 passed
tests/test_actions.py               3 passed
tests/test_adf_adapter.py           6 passed
tests/test_auth.py                  6 passed
tests/test_coordination_api.py      3 passed
tests/test_coordinator.py          20 passed
tests/test_events.py                5 passed
tests/test_execute_endpoint.py      4 passed
tests/test_fabric_adapter.py        6 passed
tests/test_health.py                2 passed
tests/test_iceberg_quality.py      10 passed
tests/test_kubeflow_adapter.py      7 passed
tests/test_policies.py             14 passed
tests/test_polling.py               3 passed
tests/test_quality_api.py           6 passed    (NEW)
tests/test_quality_coordinator.py   7 passed    (NEW)
```

### End-to-End DQ Flow

```
1. POST /api/v1/quality/check
   {"namespace": "lakehouse", "table": "sales", "expected_columns": ["id", "name", "amount"]}

2. IcebergQualityAgent runs 3 checks:
   ✓ schema_drift — compares current schema vs expected columns
   ✓ freshness — checks last snapshot age vs threshold
   ✓ volume_anomaly — compares added-records between last 2 snapshots

3. If checks fail → quality_coordinator creates:
   → PipelineEvent (pipeline="dq_check:lakehouse.sales", status=failed)
   → Diagnosis (category=DATA_QUALITY, confidence=0.95)
   → Actions (BLOCK_DOWNSTREAM=proposed, NOTIFY_OWNER=approved)

4. Response includes:
   {"table": "lakehouse.sales", "passed": 1, "failed": 2, "actions_proposed": 3}

5. Same approval + execution flow as pipeline failures:
   PATCH /api/v1/actions/{id}  →  POST /api/v1/actions/{id}/execute
```

### Phase 4 Status: COMPLETE

All Data Quality Agent functionality is implemented:
- Schema drift detection (expected columns or schema evolution)
- Table freshness checks (snapshot timestamp vs threshold)
- Volume anomaly detection (snapshot record counts)
- REST API for single-table checks and namespace scans
- DQ failures auto-create events, diagnoses, and actions
- Post-success DQ checks in coordinator
- Policy gating on all proposed actions
- 503 when Iceberg not configured

---

---

## Phase 5: Cost Optimization Agent — 2026-04-28

### What Was Built

Full cost optimization pipeline: baseline tracking, anomaly detection, recommendations, and REST API.

#### 1. CostBaseline Model (`models/cost.py`)

Per-pipeline rolling statistics, maintained via Welford's online algorithm:

| Field | Purpose |
|-------|---------|
| `avg_duration` | Running mean of run durations |
| `stddev_duration` | Running standard deviation (for z-score anomaly detection) |
| `min_duration` / `max_duration` | Range tracking |
| `total_runs` / `total_failures` / `total_successes` | Counters |
| `total_duration_seconds` | Cumulative compute cost proxy |
| `consecutive_anomalies` | Tracks streaks of anomalous runs |
| `failure_rate` | Computed property (failures / total runs) |

#### 2. Cost Optimization Agent (`agents/cost_optimizer.py`)

| Method | What it does |
|--------|-------------|
| `update_baseline()` | Updates rolling stats using Welford's algorithm — O(1) per event, no history scan |
| `detect_duration_anomaly()` | Z-score detection: `z = (duration - avg) / stddev`. Flags if z > 2.0 (configurable) |
| `detect_failure_rate_anomaly()` | Flags pipelines with >30% failure rate |
| `analyze_event()` | Runs all anomaly checks on a single event |
| `analyze_and_coordinate()` | Runs checks + creates diagnoses and actions for anomalies |
| `get_top_cost_pipelines()` | Returns most expensive pipelines by total duration |

Anomaly detection:
- **Minimum 5 runs** before flagging (avoids false positives on new pipelines)
- **Z-score threshold**: default 2.0 (configurable)
- **Severity**: low (<3σ), medium (3-4σ), high (>4σ)
- **Consecutive tracking**: resets on normal run, increments on anomaly
- **Recommendations**: context-aware (e.g., "3+ consecutive slow runs → investigate infrastructure")

#### 3. Cost API Routes (`api/routes/cost.py`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/cost/baselines` | List all pipeline baselines (by total cost) |
| GET | `/api/v1/cost/baselines/{name}` | Get baseline for a specific pipeline |
| POST | `/api/v1/cost/analyze` | Analyze an event for cost anomalies |
| GET | `/api/v1/cost/summary` | Aggregated cost dashboard data |

Cost summary includes: top cost pipelines, high failure rate pipelines, active anomaly pipelines.

#### 4. Polling Integration

The `POST /api/v1/poll` endpoint now runs cost analysis on every completed event:
- Updates baseline on every non-running event with a duration
- Detects anomalies and creates actions automatically
- Returns `cost_anomalies` count in response

### Files Created / Modified

```
# NEW FILES
src/lakehouse/models/cost.py                # CostBaseline SQLAlchemy model
src/lakehouse/agents/cost_optimizer.py      # Cost agent — baselines + anomaly detection
src/lakehouse/schemas/cost.py               # Cost API request/response schemas
src/lakehouse/api/routes/cost.py            # Cost REST endpoints
tests/test_cost_optimizer.py                # 13 tests — baselines, anomalies, coordination
tests/test_cost_api.py                      # 7 tests — API endpoints

# MODIFIED FILES
src/lakehouse/models/__init__.py            # Added CostBaseline export
src/lakehouse/api/app.py                    # Registered cost router
src/lakehouse/api/routes/polling.py         # Added cost analysis to poll loop
```

### Bash Commands Used

```bash
# Run full test suite (now 128 tests)
PYTHONPATH=src .venv/bin/python -m pytest tests/ -v
# All 128 tests pass in ~3.4 seconds
```

### Test Results

```
128 passed in 3.44s

tests/test_action_executor.py       8 passed
tests/test_actions.py               3 passed
tests/test_adf_adapter.py           6 passed
tests/test_auth.py                  6 passed
tests/test_coordination_api.py      3 passed
tests/test_coordinator.py          20 passed
tests/test_cost_api.py              7 passed    (NEW)
tests/test_cost_optimizer.py       13 passed    (NEW)
tests/test_events.py                5 passed
tests/test_execute_endpoint.py      4 passed
tests/test_fabric_adapter.py        6 passed
tests/test_health.py                2 passed
tests/test_iceberg_quality.py      10 passed
tests/test_kubeflow_adapter.py      7 passed
tests/test_policies.py             14 passed
tests/test_polling.py               3 passed
tests/test_quality_api.py           6 passed
tests/test_quality_coordinator.py   7 passed
```

### Phase 5 Status: COMPLETE

All Cost Optimization Agent functionality is implemented:
- Rolling baseline tracking per pipeline (Welford's online algorithm)
- Z-score duration anomaly detection (configurable threshold)
- Failure rate anomaly detection (>30%)
- Consecutive anomaly streak tracking
- Context-aware optimization recommendations
- REST API for baselines, analysis, and summary
- Cost analysis wired into event polling
- Anomalies auto-create diagnoses and proposed actions

---

### Full API Endpoint List (Phases 0–5)

| # | Method | Endpoint | Tag |
|---|--------|----------|-----|
| 1 | GET | `/health` | Health |
| 2 | GET | `/health/ready` | Health |
| 3 | GET | `/metrics` | Health |
| 4 | POST | `/api/v1/events` | Pipeline Events |
| 5 | GET | `/api/v1/events` | Pipeline Events |
| 6 | GET | `/api/v1/events/{id}` | Pipeline Events |
| 7 | GET | `/api/v1/events/{id}/diagnoses` | Diagnoses |
| 8 | POST | `/api/v1/diagnoses` | Diagnoses |
| 9 | GET | `/api/v1/diagnoses/{id}` | Diagnoses |
| 10 | GET | `/api/v1/diagnoses/{id}/actions` | Actions |
| 11 | POST | `/api/v1/actions` | Actions |
| 12 | GET | `/api/v1/actions/{id}` | Actions |
| 13 | PATCH | `/api/v1/actions/{id}` | Actions |
| 14 | POST | `/api/v1/actions/{id}/execute` | Actions |
| 15 | POST | `/api/v1/coordinate` | Coordination |
| 16 | POST | `/api/v1/poll` | Polling |
| 17 | GET | `/api/v1/quality/namespaces` | Data Quality |
| 18 | GET | `/api/v1/quality/namespaces/{ns}/tables` | Data Quality |
| 19 | POST | `/api/v1/quality/check` | Data Quality |
| 20 | POST | `/api/v1/quality/scan` | Data Quality |
| 21 | GET | `/api/v1/cost/baselines` | Cost Optimization |
| 22 | GET | `/api/v1/cost/baselines/{name}` | Cost Optimization |
| 23 | POST | `/api/v1/cost/analyze` | Cost Optimization |
| 24 | GET | `/api/v1/cost/summary` | Cost Optimization |

---

---

## Phase 6: Observability & Deployment — 2026-04-28

### What Was Built

Full observability stack and production deployment infrastructure.

#### 1. OpenTelemetry Tracing (`telemetry.py`)

- **Tracer provider** with resource attributes (service name, version, environment)
- **ConsoleSpanExporter** in dev, **OTLP gRPC exporter** in production
- **FastAPI auto-instrumentation** via `FastAPIInstrumentor`
- Idempotent `setup_tracing()` — safe to call multiple times
- `get_tracer(name)` for creating spans in any module

#### 2. Custom Prometheus Metrics (`telemetry.py`)

18 metrics across 6 categories:

| Category | Metrics |
|----------|---------|
| **Events** | `events_ingested_total` (by platform, status) |
| **Coordination** | `coordinations_total`, `coordination_duration_seconds`, `active_coordinations` |
| **Diagnosis** | `diagnoses_created_total` (by category, agent) |
| **Actions** | `actions_proposed_total`, `actions_executed_total`, `action_execution_duration_seconds`, `action_retries_total` |
| **Data Quality** | `dq_checks_total`, `dq_scan_duration_seconds` |
| **Cost** | `cost_anomalies_total`, `pipeline_duration_seconds` |
| **Adapters** | `adapter_calls_total`, `adapter_call_duration_seconds` |

All metrics use the `lakehouse_` prefix for Prometheus namespace consistency.

#### 3. Grafana Dashboards (auto-provisioned)

Two dashboards auto-loaded on startup:

**Pipeline Health** (`pipeline-health.json`):
- Events ingested rate (by status)
- Success vs failure pie chart
- Events by platform
- Pipeline duration distribution
- Coordination duration (p50, p95)
- Active coordinations gauge
- Diagnoses by category

**Agent Performance** (`agent-performance.json`):
- Actions proposed rate (by type)
- Actions executed — success vs failure
- Action execution duration (p95)
- Retry attempts
- Adapter call latency (p95)
- Adapter call errors
- Data quality checks (by name, status)
- Cost anomalies detected (by type, severity)

Grafana provisioning:
- Prometheus datasource auto-configured
- Dashboard folder "Lakehouse Operations"
- Dashboards loaded from `/var/lib/grafana/dashboards`

#### 4. Helm Chart (`infra/helm/lakehouse-ops/`)

Production-ready Kubernetes deployment:

| Template | What it creates |
|----------|----------------|
| `deployment-api.yaml` | API pods with liveness/readiness probes, Prometheus annotations |
| `deployment-worker.yaml` | arq worker pods |
| `service.yaml` | ClusterIP service |
| `ingress.yaml` | Nginx ingress (optional, toggled via values) |
| `hpa.yaml` | Horizontal Pod Autoscaler (2–8 replicas, 70% CPU target) |
| `configmap.yaml` | Non-secret config (env, platform adapter, OTEL, Keycloak, Iceberg) |
| `secret.yaml` | Sensitive config (DATABASE_URL, Keycloak client secret) |
| `serviceaccount.yaml` | Dedicated service account |

Deploy with:
```bash
helm install lakehouse-ops ./infra/helm/lakehouse-ops \
  --set database.url="postgresql+asyncpg://user:pass@host:5432/lakehouse" \
  --set redis.url="redis://redis:6379/0" \
  --set ingress.host="lakehouse-ops.example.com"
```

#### 5. Production Dockerfile (multi-stage)

- **Builder stage**: installs dependencies with uv
- **Runtime stage**: minimal image (no gcc), non-root user, healthcheck
- `PYTHONUNBUFFERED=1` for real-time log output
- Docker HEALTHCHECK hitting `/health` endpoint

#### 6. Docker Compose Updates

- Grafana now mounts provisioning and dashboard volumes
- Added `worker` service running arq background worker

#### 7. Pre-commit Config (`.pre-commit-config.yaml`)

| Hook | Purpose |
|------|---------|
| `ruff` | Linting + auto-fix |
| `ruff-format` | Code formatting |
| `mypy` | Type checking (with Pydantic/SQLAlchemy plugins) |
| `trailing-whitespace` | Clean whitespace |
| `end-of-file-fixer` | Consistent EOF |
| `check-yaml` / `check-json` | Config file validation |
| `check-added-large-files` | Block files >500KB |
| `no-commit-to-branch` | Prevent direct commits to main |

### Files Created / Modified

```
# NEW FILES
src/lakehouse/telemetry.py                                    # OTel tracing + 18 Prometheus metrics
infra/grafana/provisioning/datasources/prometheus.yml         # Auto-configure Prometheus datasource
infra/grafana/provisioning/dashboards/dashboards.yml          # Dashboard provider config
infra/grafana/dashboards/pipeline-health.json                 # Pipeline health dashboard (7 panels)
infra/grafana/dashboards/agent-performance.json               # Agent performance dashboard (8 panels)
infra/helm/lakehouse-ops/Chart.yaml                           # Helm chart metadata
infra/helm/lakehouse-ops/values.yaml                          # Default values
infra/helm/lakehouse-ops/templates/deployment-api.yaml        # API deployment
infra/helm/lakehouse-ops/templates/deployment-worker.yaml     # Worker deployment
infra/helm/lakehouse-ops/templates/service.yaml               # ClusterIP service
infra/helm/lakehouse-ops/templates/ingress.yaml               # Nginx ingress
infra/helm/lakehouse-ops/templates/hpa.yaml                   # Horizontal Pod Autoscaler
infra/helm/lakehouse-ops/templates/configmap.yaml             # Non-secret config
infra/helm/lakehouse-ops/templates/secret.yaml                # Secrets
infra/helm/lakehouse-ops/templates/serviceaccount.yaml        # Service account
.pre-commit-config.yaml                                       # Linting + formatting hooks
tests/test_telemetry.py                                       # 6 tests — tracing + metrics

# MODIFIED FILES
src/lakehouse/api/app.py                                      # Added tracing init + FastAPI instrumentation
Dockerfile                                                    # Multi-stage build, non-root, healthcheck
docker-compose.yml                                            # Grafana provisioning + worker service
```

### Bash Commands Used

```bash
# Run full test suite (now 134 tests)
PYTHONPATH=src .venv/bin/python -m pytest tests/ -v
# All 134 tests pass in ~3.5 seconds

# Deploy with Helm
helm install lakehouse-ops ./infra/helm/lakehouse-ops \
  --set database.url="postgresql+asyncpg://user:pass@host:5432/lakehouse" \
  --set redis.url="redis://redis:6379/0"

# Start full stack locally
docker compose up -d
# API at http://localhost:8000
# Grafana at http://localhost:3000 (admin/admin)
# Prometheus at http://localhost:9090

# Install pre-commit hooks
pre-commit install
```

### Test Results

```
134 passed in 3.50s

tests/test_action_executor.py       8 passed
tests/test_actions.py               3 passed
tests/test_adf_adapter.py           6 passed
tests/test_auth.py                  6 passed
tests/test_coordination_api.py      3 passed
tests/test_coordinator.py          20 passed
tests/test_cost_api.py              7 passed
tests/test_cost_optimizer.py       13 passed
tests/test_events.py                5 passed
tests/test_execute_endpoint.py      4 passed
tests/test_fabric_adapter.py        6 passed
tests/test_health.py                2 passed
tests/test_iceberg_quality.py      10 passed
tests/test_kubeflow_adapter.py      7 passed
tests/test_policies.py             14 passed
tests/test_polling.py               3 passed
tests/test_quality_api.py           6 passed
tests/test_quality_coordinator.py   7 passed
tests/test_telemetry.py             6 passed    (NEW)
```

### Phase 6 Status: COMPLETE

---

## ALL PHASES COMPLETE — Project Summary

### Platform Statistics

| Metric | Count |
|--------|-------|
| **API Endpoints** | 24 |
| **Tests** | 134 (all passing) |
| **Source Files** | 40+ |
| **Specialist Agents** | 4 (Coordinator, DQ, Cost, Executor) |
| **Platform Adapters** | 5 (Mock, ADF, Fabric, Kubeflow, Databricks placeholder) |
| **Grafana Dashboards** | 2 (15 panels total) |
| **Prometheus Metrics** | 18 custom metrics |
| **Helm Templates** | 8 |

### Architecture

*(See README.md for Mermaid architecture diagrams)*

### End-to-End Flow

```
1. POLL  → Adapter fetches events → Ingest → Cost baseline update
2. FAIL  → Coordinator diagnoses → Pattern match → Propose actions → Policy gate
3. DQ    → Iceberg checks → Schema/freshness/volume → Propose actions if failed
4. COST  → Z-score anomaly detection → Flag spikes → Propose actions
5. APPROVE → Human approves action via API
6. EXECUTE → Exponential backoff retry → Audit log → Prometheus metrics
7. OBSERVE → Grafana dashboards → Pipeline health + Agent performance
```

---

## Gap Analysis: LLM Integration — 2026-04-28

### Current State: Rule-Based, Not AI-Powered

Despite the project's goal of being an "AI control plane," **no LLM models are currently integrated**. All agent intelligence is rule-based:

| Agent | Current Intelligence | How It Works |
|-------|---------------------|-------------|
| **Coordinator** | Keyword pattern matching | 13 hardcoded `(keywords, category, confidence, summary)` tuples checked against `error_message` and logs |
| **DQ Agent** | Deterministic metadata checks | Iceberg REST Catalog API → compare schemas, snapshot timestamps, record counts |
| **Cost Optimizer** | Statistical z-score detection | Welford's online mean/stddev → flag if `(duration - avg) / stddev > 2.0` |
| **Action Executor** | Hardcoded action→adapter routing | `match action_type: case RETRY → adapter.retry_pipeline()` |

The autonomous workflow (detect → diagnose → propose → execute) is real and functional, but the **reasoning is if/else logic, not AI**.

### Limitations of Rule-Based Approach

1. **Novel failures**: Pattern matcher only recognizes the 13 known patterns. Any error message that doesn't contain keywords like "timeout", "schema mismatch", "permission denied", etc. falls through to `UNKNOWN` with 0.40 confidence.

2. **No contextual reasoning**: Rules can't consider *why* a timeout happened — was it a data volume spike? A slow source? Resource contention? The remediation is always the same generic set of actions regardless of context.

3. **No learning**: The system never improves from past incidents. The same failure mode gets the same diagnosis every time, even if previous remediation attempts failed.

4. **Brittle matching**: `"column not found"` matches `SCHEMA_MISMATCH`, but `"field 'revenue' does not exist in source table"` falls through to `UNKNOWN` because the keywords don't match.

5. **No natural language**: Summaries and descriptions are template strings, not intelligent explanations tailored to the incident context.

### Where LLMs Would Transform the Platform

#### 1. Intelligent Failure Diagnosis
**Currently:** `if "timeout" in error_message → TIMEOUT (0.85 confidence)`
**With LLM:** Parse unstructured error messages, stack traces, multi-line logs, and platform-specific error formats to identify root causes — including failure modes never seen before.

Example: An error message like `"SparkException: Job aborted due to stage failure: Task 14 in stage 23.0 failed 4 times, most recent failure: Lost task 14.3 in stage 23.0"` would be correctly classified as RESOURCE_EXHAUSTION even though it contains none of our current keywords.

#### 2. Contextual Remediation Reasoning
**Currently:** `TIMEOUT → [RETRY, SCALE_COMPUTE]` (hardcoded map)
**With LLM:** Reason about the specific timeout context:
- "Source database was slow → RETRY after 30min, don't scale compute"
- "Data volume 10x normal → SCALE_COMPUTE first, then retry"
- "Same timeout 3 times this week → NOTIFY_OWNER, don't auto-retry"

#### 3. Incident Summarization
Generate human-readable incident reports from the chain of events, diagnoses, and actions — useful for:
- Audit trail entries that operators actually read
- Slack/email notifications with context
- Post-mortem analysis

#### 4. Conversational Operations Interface
Operators ask natural language questions:
- *"Why did the sales pipeline fail 3 times this week?"*
- *"Which pipelines are costing the most this month?"*
- *"What should I check before approving this retry action?"*

The LLM queries events, diagnoses, baselines, and audit logs to compose answers.

#### 5. Adaptive Policy Learning
Analyze the history of approved/rejected actions to suggest policy adjustments:
- "RETRY_PIPELINE has been approved 47/50 times for TIMEOUT failures — consider auto-approving"
- "SCALE_COMPUTE was rejected 8/10 times last month — operators prefer manual scaling"

### Proposed Architecture: Hybrid Intelligence

*(See README.md for Mermaid diagram of this flow)*

**Design principle:** Rules handle known patterns fast (< 10ms). The LLM handles novel/complex cases that rules can't classify (1-3s). This avoids unnecessary API calls and cost while ensuring no failure goes undiagnosed.

### Implementation: COMPLETE — 2026-04-29

---

## LLM Integration Build Log — 2026-04-29

### What Was Built

Full LLM integration with dual provider support (remote + local), hybrid diagnosis, and incident summarization.

#### 1. LLM Provider Abstraction (`llm/` package)

Protocol-based design supporting any model backend:

| File | Provider | Connects to |
|------|----------|-------------|
| `llm/base.py` | `LLMProvider` protocol + `LLMResponse` dataclass | (interface) |
| `llm/claude_provider.py` | `ClaudeProvider` | Anthropic API (Claude Opus, Sonnet, Haiku) |
| `llm/local_provider.py` | `LocalProvider` | Any OpenAI-compatible server (Ollama, vLLM, llama.cpp, LM Studio) |
| `llm/factory.py` | `get_llm_provider()` | Returns configured provider or None |

**Provider protocol:**
```python
class LLMProvider(Protocol):
    provider_name: str
    model_name: str
    async def complete(prompt, system, max_tokens, temperature) -> LLMResponse
    async def is_available() -> bool
```

**Local provider compatibility:**
- Ollama: `http://localhost:11434/v1`
- vLLM: `http://localhost:8000/v1`
- llama.cpp: `http://localhost:8080/v1`
- LM Studio: `http://localhost:1234/v1`

All use the same OpenAI-compatible `/chat/completions` endpoint — no openai SDK dependency.

#### 2. LLM Diagnosis Agent (`agents/llm_diagnosis.py`)

Sends structured prompts to the LLM for intelligent failure classification:

- **System prompt**: defines the diagnosis role, valid categories, JSON output schema
- **User prompt**: pipeline name, platform, error message, error code, logs (truncated to 3000 chars), metadata
- **Output**: parsed into `AgentFinding` (same type as rule-based diagnosis)
- **Fallback**: returns UNKNOWN with `fallback=True` if LLM unreachable or response unparseable
- **Markdown fence stripping**: handles LLMs that wrap JSON in \`\`\`json blocks
- **Confidence clamping**: ensures 0.0–1.0 range even if model outputs out-of-range values

#### 3. Incident Summarizer (`agents/summarizer.py`)

Generates natural language summaries for two scenarios:

| Method | Input | Output |
|--------|-------|--------|
| `summarize_incident()` | Pipeline failure + diagnosis + actions | Human-readable incident report |
| `summarize_cost_anomaly()` | Cost spike details + recommendations | Cost alert summary |

Both methods:
- Use LLM when provider is available (with dedicated prompts)
- Fall back to template-based summaries when LLM is unavailable or fails
- Keep summaries under 200 words

#### 4. Hybrid Coordinator (`_hybrid_diagnose()`)

The coordinator now runs a two-stage diagnosis:

```
Stage 1: Rule Engine (always runs, < 10ms)
    ├── High confidence (>= 0.7) AND known category → USE RULES (fast path)
    └── Low confidence OR UNKNOWN category
            │
Stage 2: LLM Escalation (only when needed, ~1-3s)
    ├── LLM confidence > rule confidence → USE LLM RESULT
    ├── LLM confidence <= rule confidence → KEEP RULE RESULT
    └── LLM fails → FALL BACK TO RULE RESULT
```

**Key behaviors:**
- LLM is NEVER called for known patterns with high confidence (saves cost and latency)
- LLM is ONLY called when `llm_provider` is set AND rules produce low confidence
- If LLM fails (connection error, timeout, bad response), rules always provide a fallback
- Threshold configurable via `LLM_CONFIDENCE_THRESHOLD` (default 0.7)

### Files Created / Modified

```
# NEW FILES
src/lakehouse/llm/__init__.py               # Package exports
src/lakehouse/llm/base.py                   # LLMProvider protocol + LLMResponse
src/lakehouse/llm/claude_provider.py        # Anthropic Claude API provider
src/lakehouse/llm/local_provider.py         # OpenAI-compatible local provider
src/lakehouse/llm/factory.py                # Provider factory (claude/local/none)
src/lakehouse/agents/llm_diagnosis.py       # LLM-powered failure diagnosis
src/lakehouse/agents/summarizer.py          # Incident + cost summarizer
tests/test_llm.py                           # 23 tests — providers, diagnosis, summarizer, hybrid, factory

# MODIFIED FILES
src/lakehouse/config.py                     # Added LLM config fields (11 new settings)
src/lakehouse/agents/coordinator.py         # Added llm_provider param, _hybrid_diagnose()
pyproject.toml                              # Added anthropic SDK dependency
.env.example                                # Added LLM environment variables
```

### Configuration

```bash
# Enable LLM (disabled by default — rule-based only)
LLM_ENABLED=true

# Provider: "claude" (remote) or "local" (Ollama/vLLM)
LLM_PROVIDER=claude
LLM_MODEL=claude-sonnet-4-20250514
LLM_CONFIDENCE_THRESHOLD=0.7
ANTHROPIC_API_KEY=sk-ant-...

# OR use a local model:
LLM_PROVIDER=local
LLM_LOCAL_URL=http://localhost:11434/v1    # Ollama
LLM_LOCAL_MODEL=llama3:8b
```

### Bash Commands Used

```bash
# Install anthropic SDK
uv pip install -e ".[dev]" aiosqlite
# Adds: anthropic, distro, jiter, sniffio

# Run full test suite (now 157 tests)
PYTHONPATH=src .venv/bin/python -m pytest tests/ -v
# All 157 tests pass in ~3.9 seconds
```

### Test Results

```
157 passed in 3.87s

tests/test_action_executor.py       8 passed
tests/test_actions.py               3 passed
tests/test_adf_adapter.py           6 passed
tests/test_auth.py                  6 passed
tests/test_coordination_api.py      3 passed
tests/test_coordinator.py          20 passed
tests/test_cost_api.py              7 passed
tests/test_cost_optimizer.py       13 passed
tests/test_events.py                5 passed
tests/test_execute_endpoint.py      4 passed
tests/test_fabric_adapter.py        6 passed
tests/test_health.py                2 passed
tests/test_iceberg_quality.py      10 passed
tests/test_kubeflow_adapter.py      7 passed
tests/test_llm.py                  23 passed    (NEW)
tests/test_policies.py             14 passed
tests/test_polling.py               3 passed
tests/test_quality_api.py           6 passed
tests/test_quality_coordinator.py   7 passed
tests/test_telemetry.py             6 passed
```

### LLM Test Coverage

| Category | Tests | What's covered |
|----------|-------|----------------|
| Provider | 4 | Mock response, call tracking, failure mode, availability |
| Diagnosis | 7 | Valid JSON, schema mismatch, invalid JSON, markdown fences, connection failure, unknown category, confidence clamping |
| Summarizer | 5 | LLM summary, template fallback, LLM failure fallback, cost summary (LLM + template) |
| Hybrid coordinator | 4 | Rules preferred when confident, LLM escalation for UNKNOWN, LLM failure fallback, disabled mode |
| Factory | 3 | Disabled returns None, Claude provider, Local provider |

### Updated Platform Statistics

| Metric | Count |
|--------|-------|
| **API Endpoints** | 24 |
| **Tests** | 157 (all passing) |
| **Source Files** | 50+ |
| **Specialist Agents** | 6 (Coordinator, DQ, Cost, Executor, LLM Diagnosis, Summarizer) |
| **Platform Adapters** | 5 (Mock, ADF, Fabric, Kubeflow, Databricks) |
| **LLM Providers** | 2 (Claude API, Local/Ollama/vLLM) |
| **Grafana Dashboards** | 2 (15 panels) |
| **Prometheus Metrics** | 18 |
| **Helm Templates** | 8 |

---

## README.md Written — 2026-04-29

Professional README.md created covering:
- Project description and capabilities table
- Architecture diagram (ASCII)
- Quick start guide (6 steps: clone, install, configure, infra, run, test)
- Full API reference (24 endpoints across 7 domains)
- Platform adapters table with status
- LLM integration guide (Claude remote + Ollama/vLLM local)
- Observability section (Prometheus, Grafana, OpenTelemetry)
- Background worker usage
- Kubernetes/Helm deployment instructions
- KF4X integration matrix
- Project structure overview
- Development guide (linting, type checking, pre-commit, coverage)
- Configuration reference table
- License

---

## Medium Article Series Written — 2026-04-29

4-part series in `articles/` directory:

| Part | File | Title | Topics |
|------|------|-------|--------|
| 1 | `part-1-architecture.md` | Why Your Data Pipelines Need a Brain | The 3 AM problem, architecture, adapter pattern, domain model, tech stack, design principles |
| 2 | `part-2-agents.md` | The Agents | Coordinator pattern, two-pass diagnosis, category→action mapping, policy engine, exponential backoff executor, full flow walkthrough |
| 3 | `part-3-dq-and-cost.md` | Data Quality & Cost Optimization | Iceberg schema drift/freshness/volume, DQ→remediation pipeline, Welford's algorithm, z-score anomaly detection, unified model |
| 4 | `part-4-llm-and-deployment.md` | LLM Integration & Production Deployment | Hybrid diagnosis, provider abstraction, structured prompting, incident summarization, Docker multi-stage, Helm chart, Prometheus/Grafana, KF4X integration |

Each article is ~1500-2000 words, includes code snippets, architecture diagrams, and links to the next part.

---

## Demo Walkthrough Written — 2026-04-29

Created `demo.md` with two sections:

**Section A: Local Demo (Mock Adapter)** — 11 steps, no cloud credentials needed:
1. Start the platform (Docker + uvicorn)
2. Ingest a failed event (timeout)
3. Trigger coordinator (diagnose + propose)
4. Review proposed actions
5. Approve an action
6. Execute with retry
7. Automated polling loop
8. Schema mismatch scenario
9. Cost baseline + anomaly spike
10. Prometheus metrics check
11. Full end-to-end loop

**Section B: Cloud Demos** — 4 platform-specific walkthroughs:
- **B1: Azure Data Factory** — credential setup (subscription, resource group, factory, service principal), ADF-specific errors (SqlFailedToConnect, ContainerNotFound), retry via ADF CreateRun API
- **B2: Microsoft Fabric** — credential setup (workspace ID, tenant, client), Fabric errors (Lakehouse schema evolution, Spark session timeout), Iceberg DQ checks on Fabric tables
- **B3: Databricks** — credential setup (workspace URL, PAT/service principal), Databricks errors (Spark OOM, Unity Catalog permission denied, Delta constraint violation), cost analysis for expensive jobs
- **B4: Multi-Platform** — ingest events from all 4 platforms simultaneously, coordinate across platforms, unified view

All credential placeholders use `<your-...>` format with "Where to find" instructions pointing to the correct portal pages.

---

## Mermaid Diagrams Added — 2026-04-29

Replaced all ASCII art architecture diagrams with Mermaid code blocks across the codebase. These render natively on GitHub and can be exported to PNG via [Mermaid CLI](https://github.com/mermaid-js/mermaid-cli) or [Mermaid Live Editor](https://mermaid.live).

| File | Diagram | Type |
|------|---------|------|
| `README.md` | 4 diagrams: system overview, coordinator workflow, hybrid LLM, KF4X integration, sequence diagram |
| `articles/part-1-architecture.md` | System architecture — adapters → coordinator → agents → policy → audit |
| `articles/part-2-agents.md` | Coordinator 8-step workflow |
| `articles/part-4-llm-and-deployment.md` | Hybrid LLM diagnosis — fast path / deep path / fallback |
| `agentic_orchestrator.md` | Original blueprint flow — platform → control plane → agents → actions |

All diagrams use consistent color coding:
- Blue (`#e3f2fd`): Core logic (rules, coordinator)
- Orange (`#fff3e0`): External integrations (platforms, LLM)
- Red (`#fce4ec`): Policy / safety gates
- Green (`#e8f5e9`): Success paths, SSO
- Purple (`#f3e5f5`): Observability

To generate PNGs:
```bash
# Install Mermaid CLI
npm install -g @mermaid-js/mermaid-cli

# Extract and render a diagram from a markdown file
mmdc -i README.md -o architecture.png
```

---

## Diagram Images Added to Articles — 2026-04-29

User generated PNG images from Mermaid code and placed them in `articles/`. Each article now includes both the Mermaid source (for GitHub rendering) and the PNG image (for Medium/offline viewing).

| Image File | Article | Diagram |
|---|---|---|
| `articles/part-1-architecture-img&readme.png` | Part 1 | System architecture — platforms → coordinator → agents → policy → audit |
| `articles/part-2-agents-img.png` | Part 2 | Coordinator 8-step workflow |
| `articles/part-4-llm-and-deployment-img.png` | Part 4 | Hybrid LLM diagnosis — rules fast path / LLM deep path / fallback |

Each image is placed directly below its Mermaid code block with a caption.

---

## CI/CD Pipeline Created — 2026-04-29

### What Was Built

Three GitHub Actions workflow files for CI, release, and integration testing.

### Files Created

```
.github/workflows/ci.yml              # Comprehensive CI: lint, typecheck, test, docker, security
.github/workflows/release.yml         # Release pipeline: build + push to GHCR on tag
.github/workflows/integration.yml     # Manual integration tests per platform
```

#### CI Pipeline (`ci.yml`)

Triggers on push to `main`/`develop` and PRs to `main`. Five parallel jobs:

| Job | What it does |
|-----|-------------|
| `lint` | ruff check + ruff format --check |
| `typecheck` | mypy strict mode on src/lakehouse/ |
| `test` | pytest with Postgres + Redis services, coverage report to Codecov |
| `docker` | Build Docker image (runs after lint, typecheck, test pass) |
| `security` | pip-audit for known vulnerabilities |

Test job uses service containers matching docker-compose.yml (postgres:16-alpine, redis:7-alpine).

#### Release Pipeline (`release.yml`)

Triggers on version tags (`v*`). Builds and pushes Docker image to GitHub Container Registry (ghcr.io) with both version tag and `latest`.

#### Integration Tests (`integration.yml`)

Manual trigger (`workflow_dispatch`) with platform selector (adf, fabric, databricks, kubeflow, all). Injects platform-specific secrets from GitHub environment secrets.

### Bash Commands Used

```bash
mkdir -p .github/workflows
```

---

## Databricks Adapter Implementation — 2026-04-29

### What Was Built

Full Databricks platform adapter with real HTTP calls via httpx, following the Kubeflow adapter pattern.

### Files Created / Modified

```
# NEW FILES
src/lakehouse/adapters/databricks.py          # Databricks Jobs API v2.1 adapter (real HTTP)
tests/test_databricks_adapter.py              # 7 tests with mock httpx transport

# MODIFIED FILES
src/lakehouse/adapters/__init__.py            # Added PlatformType.DATABRICKS case to factory
src/lakehouse/config.py                       # Added databricks_workspace_url, databricks_token settings
```

### Databricks Adapter (`adapters/databricks.py`)

Implements the `PlatformAdapter` protocol using the **Databricks Jobs API v2.1**:

| Method | API Endpoint | What it does |
|--------|-------------|--------------|
| `fetch_recent_events()` | GET /api/2.1/jobs/runs/list | Fetches recent runs, maps Databricks states to PipelineStatus |
| `retry_pipeline()` | GET /api/2.1/jobs/runs/get + POST /api/2.1/jobs/run-now | Gets original job_id, triggers new run |
| `cancel_pipeline()` | POST /api/2.1/jobs/runs/cancel | Cancels a running job |
| `get_pipeline_logs()` | GET /api/2.1/jobs/runs/get-output | Retrieves notebook output, log, error info |
| `check_source_availability()` | GET /api/2.0/clusters/list | Verifies workspace is reachable |
| `get_pipeline_metadata()` | GET /api/2.1/jobs/list | Fetches job config filtered by name |

Databricks state mapping:
```
TERMINATED + SUCCESS → succeeded
TERMINATED + FAILED/TIMEDOUT → failed
RUNNING/PENDING → running
CANCELED/TERMINATING/SKIPPED → cancelled
INTERNAL_ERROR → failed
```

### Test Results

```
7 passed in 0.07s

tests/test_databricks_adapter.py::test_fetch_recent_events PASSED
tests/test_databricks_adapter.py::test_retry_pipeline PASSED
tests/test_databricks_adapter.py::test_cancel_pipeline PASSED
tests/test_databricks_adapter.py::test_get_pipeline_logs PASSED
tests/test_databricks_adapter.py::test_check_source_availability PASSED
tests/test_databricks_adapter.py::test_check_source_availability_unreachable PASSED
tests/test_databricks_adapter.py::test_get_pipeline_metadata PASSED
```

Tests use `httpx.MockTransport` to simulate Databricks API responses without network calls.

---

## Conversational Operations Interface — 2026-04-29

### What Was Built

Natural language chat interface for querying the lakehouse via REST API.

### Files Created / Modified

```
# NEW FILES
src/lakehouse/agents/chat_ops.py              # ChatOpsAgent — NL query → DB lookup → LLM answer
src/lakehouse/schemas/chat.py                 # ChatRequest + ChatResponseSchema Pydantic models
src/lakehouse/api/routes/chat.py              # POST /api/v1/chat endpoint
tests/test_chat_ops.py                        # 11 tests — intent detection, DB queries, LLM, API

# MODIFIED FILES
src/lakehouse/api/app.py                      # Registered chat router
```

### ChatOpsAgent (`agents/chat_ops.py`)

Keyword-based intent detection routes questions to the right DB tables:

| Intent | Keywords | Queries |
|--------|----------|---------|
| `failure_analysis` | fail, error, broke | Failed events + diagnoses |
| `cost_inquiry` | cost, expensive, slow, duration | Cost baselines |
| `action_status` | action, approve, reject, execute | Actions by status |
| `pipeline_specific` | pipeline + name | Events for that pipeline |
| `general` | (default) | Recent events + summary stats |

Two response modes:
- **With LLM**: Sends DB context + question to LLM for natural language answer
- **Without LLM**: Returns structured template-based answer (data-only fallback)

### API Endpoint

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/chat` | Accept NL question, return answer + intent + data + llm_used flag |

### Test Results

```
11 passed in 0.40s

tests/test_chat_ops.py::test_intent_failure_keywords PASSED
tests/test_chat_ops.py::test_intent_cost_keywords PASSED
tests/test_chat_ops.py::test_intent_action_keywords PASSED
tests/test_chat_ops.py::test_intent_pipeline_specific PASSED
tests/test_chat_ops.py::test_intent_general_fallback PASSED
tests/test_chat_ops.py::test_query_with_mock_llm PASSED
tests/test_chat_ops.py::test_query_without_llm_fallback PASSED
tests/test_chat_ops.py::test_query_empty_results PASSED
tests/test_chat_ops.py::test_query_cost_inquiry PASSED
tests/test_chat_ops.py::test_query_general_overview PASSED
tests/test_chat_ops.py::test_chat_api_endpoint PASSED
```

---

## Integration Tests Created — 2026-04-29

### What Was Built

Integration test suite for real cloud adapter testing across all 4 platforms (ADF, Fabric, Databricks, Kubeflow). 16 tests total, 4 per platform. All skip cleanly when credentials are absent.

### Files Created / Modified

```
tests/integration/__init__.py                      # Empty init
tests/integration/conftest.py                      # Skip decorators, adapter fixtures, --platform CLI option
tests/integration/test_adf_integration.py          # 4 ADF tests
tests/integration/test_fabric_integration.py       # 4 Fabric tests
tests/integration/test_databricks_integration.py   # 4 Databricks tests
tests/integration/test_kubeflow_integration.py     # 4 Kubeflow tests
pyproject.toml                                     # Registered integration + timeout markers
```

### Usage

```bash
pytest tests/integration/ -v                       # All platforms (skips missing creds)
pytest tests/integration/test_adf_integration.py   # Single platform
pytest tests/integration/ --platform adf           # CLI filter
```

---

## Adaptive Policy Learning — 2026-04-29

### What Was Built

Adaptive policy learning system that analyzes action approval/rejection history to suggest policy tier changes. Optionally uses LLM for richer reasoning.

### Files Created / Modified

```
# NEW FILES
src/lakehouse/agents/policy_learner.py       # PolicyLearner — stats + rule-based/LLM suggestions
src/lakehouse/schemas/policy.py              # Pydantic schemas for policy API
src/lakehouse/api/routes/policy.py           # REST endpoints: stats, suggestions, current config
tests/test_policy_learner.py                 # 12 tests — stats, thresholds, LLM, API

# MODIFIED FILES
src/lakehouse/api/app.py                     # Registered policy router
```

### PolicyLearner (`agents/policy_learner.py`)

Analyzes (action_type, failure_category) groups from the actions + diagnoses tables:

| Method | What it does |
|--------|-------------|
| `get_action_stats()` | Groups actions by (action_type, diagnosis.category), counts approved/rejected/succeeded/failed |
| `analyze_approval_history()` | Generates policy change suggestions based on thresholds or LLM reasoning |

Suggestion thresholds:
- approval_rate > 0.9 AND total >= 10 -> suggest auto-approve
- rejection_rate > 0.8 AND total >= 5 -> suggest forbidden
- Mixed rates -> keep requires_approval (no change suggested if already at that tier)

Two modes:
- **Rule-based only** (no LLM): threshold-based suggestions with confidence scores
- **LLM-enhanced**: sends stats to LLM for richer reasoning about policy changes, falls back to rules on failure

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/policy/stats` | Action approval/rejection stats per group |
| GET | `/api/v1/policy/suggestions` | Policy change suggestions with analysis summary |
| GET | `/api/v1/policy/current` | Current static policy configuration |

### Test Results

```
12 passed in 0.71s

tests/test_policy_learner.py::test_get_action_stats_empty PASSED
tests/test_policy_learner.py::test_get_action_stats_counts PASSED
tests/test_policy_learner.py::test_suggest_auto_approve_high_approval_rate PASSED
tests/test_policy_learner.py::test_suggest_forbid_high_rejection_rate PASSED
tests/test_policy_learner.py::test_suggest_keep_requires_approval_mixed PASSED
tests/test_policy_learner.py::test_with_llm_provider PASSED
tests/test_policy_learner.py::test_without_llm_rule_based_only PASSED
tests/test_policy_learner.py::test_llm_fallback_on_error PASSED
tests/test_policy_learner.py::test_empty_history_no_suggestions PASSED
tests/test_policy_learner.py::test_api_policy_stats PASSED
tests/test_policy_learner.py::test_api_policy_suggestions PASSED
tests/test_policy_learner.py::test_api_policy_current PASSED
```

---

## Alembic Migration Generated — 2026-04-29

### What Was Built

Initial Alembic migration (`001_initial_schema`) creating all 5 core tables with proper columns, indexes, foreign keys, and server defaults.

### Files Created / Modified

```
# NEW FILES
alembic/versions/001_initial_schema.py   # Initial migration — 5 tables, all indexes, FKs, defaults

# MODIFIED FILES
alembic/env.py                           # Allow URL override for testing (skip settings when URL is explicit)
```

### Tables Created

| Table | Columns | Indexes |
|-------|---------|---------|
| `pipeline_events` | 13 | external_run_id, pipeline_name, (platform, status), (pipeline_name, started_at) |
| `diagnoses` | 9 | event_id (FK → pipeline_events) |
| `actions` | 12 | diagnosis_id (FK → diagnoses) |
| `audit_log` | 10 | event_type, resource_id, correlation_id |
| `cost_baselines` | 15 | pipeline_name (unique), last_run_at |

### Usage

```bash
# Run against Postgres
PYTHONPATH=src .venv/bin/python -m alembic upgrade head

# Generate new migration after model changes
PYTHONPATH=src .venv/bin/python -m alembic revision --autogenerate -m "description"

# Rollback
PYTHONPATH=src .venv/bin/python -m alembic downgrade -1
```

Tested successfully against SQLite — creates all tables with correct schema.

---

## Git Repository Initialized — 2026-04-29

### What Was Done

Initialized git repository and created initial commit with all 133 project files (17,355 lines of code).

### Commands Used

```bash
git init
git add <all project files>
git commit -m "Initial commit: Autonomous Lakehouse Operations Platform"
```

### Repository Stats

| Metric | Count |
|--------|-------|
| **Files committed** | 133 |
| **Lines of code** | 17,355 |
| **Unit tests** | 187 (all passing) |
| **Integration tests** | 16 (env-gated) |
| **API endpoints** | 30 |

---

## Final Platform Statistics — 2026-04-29

| Metric | Count |
|--------|-------|
| **API Endpoints** | 30 |
| **Unit Tests** | 187 (all passing) |
| **Integration Tests** | 16 (env-gated) |
| **Source Files** | 60+ |
| **Specialist Agents** | 8 (Coordinator, DQ, Cost, Executor, LLM Diagnosis, Summarizer, ChatOps, PolicyLearner) |
| **Platform Adapters** | 5 (Mock, ADF, Fabric, Databricks, Kubeflow) |
| **LLM Providers** | 2 (Claude API, Local/Ollama/vLLM) |
| **Grafana Dashboards** | 2 (15 panels) |
| **Prometheus Metrics** | 18 |
| **Helm Templates** | 8 |
| **GitHub Actions Workflows** | 3 (CI, Release, Integration) |
| **Alembic Migrations** | 1 (initial schema — 5 tables) |

---

## Session — 2026-05-01

### Status Check

Ran full test suite — **187 passed, 16 skipped** (integration tests skip without cloud credentials). All phases complete, project is ship-ready.

### Changes Made

#### 1. Added "Future Enhancements" section to `README.md`

8 potential next steps: real cloud testing, K8s deployment, LLM enablement, WebSocket streaming, Slack/Teams notifications, historical analytics, multi-tenant support, scheduled DQ scans.

#### 2. Tech Stack Upgrades

Applied 4 upgrades after a full tech stack review:

| Change | Before | After |
|--------|--------|-------|
| **Python version** | 3.11 | **3.12** (faster startup, better error messages) |
| **Prometheus image** | `:latest` | **`v3.2.1`** (pinned for reproducibility) |
| **Grafana image** | `:latest` | **`11.4.0`** (pinned for reproducibility) |
| **`anthropic` SDK** | Hard dependency | **Optional `[llm]` extra** (lighter base install) |
| **Rate limiting** | None | **`slowapi` at 120 req/min** (production hardening) |
| **pytest-asyncio** | `>=0.24.0` | **`>=0.25.0`** (better fixture scoping) |

Files modified:
- `pyproject.toml` — Python 3.12, ruff/mypy targets, `[llm]` optional extra, slowapi dep, pytest-asyncio bump
- `Dockerfile` — Python 3.12 base images, site-packages path
- `docker-compose.yml` — pinned Prometheus v3.2.1, Grafana 11.4.0
- `src/lakehouse/api/app.py` — slowapi rate limiter (120/min default, 429 handler)
- `README.md` — updated prerequisites and install commands for 3.12 + `[llm]` extra

All **187 tests pass** on Python 3.12.

#### 3. Mypy Type Errors Fixed

Fixed 25 mypy type errors across 15 source files (iceberg_quality, kubeflow, databricks, cost routes, claude_provider, quality routes, coordinator, auth middleware, factory, worker tasks, app). Codebase now passes `mypy --strict` cleanly.

#### 4. Documentation Accuracy Sweep

Fixed stale references across 8 files:
- `demo.md` — Python 3.11 → 3.12, `.[dev]` → `.[dev,llm]`
- `articles/part-4-llm-and-deployment.md` — Dockerfile snippets 3.11 → 3.12
- `.github/workflows/ci.yml` — Python 3.11 → 3.12, `.[dev]` → `.[dev,llm]` (all 4 jobs)
- `.github/workflows/integration.yml` — Python 3.11 → 3.12, `.[dev]` → `.[dev,llm]`
- `README.md` — corrected endpoint count to 25, test count to 203

---

## Session — 2026-05-02

### Changes Made

#### License: AGPL-3.0

Added GNU Affero General Public License v3.0:
- `LICENSE` — full AGPL-3.0 text from gnu.org (661 lines)
- `pyproject.toml` — added `license = "AGPL-3.0-only"` and `license-files = ["LICENSE"]`
- `README.md` — updated license section from Apache 2.0 to AGPL-3.0

All runtime dependencies (FastAPI, SQLAlchemy, Pydantic, httpx, structlog, arq, PyJWT, prometheus-client, OpenTelemetry, slowapi, anthropic, etc.) use permissive licenses (MIT, BSD, Apache-2.0) — all compatible with AGPL-3.0, no violations.

---

*This file captures the full build history of the Autonomous Lakehouse Operations Platform.*
