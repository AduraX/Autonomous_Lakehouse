# Autonomous Lakehouse Operations Platform

A production-grade AI control plane for self-healing data pipelines. Monitors pipeline runs across **Azure Data Factory**, **Microsoft Fabric**, **Databricks**, and **Kubeflow**, diagnoses failures, proposes remediation actions, validates data quality, and optimizes costs — with optional LLM-powered intelligence.

---

## What It Does

| Capability | How |
|---|---|
| **Self-healing pipelines** | Detects failures, diagnoses root cause (13 rule-based patterns + LLM), proposes and executes remediation |
| **Data quality monitoring** | Schema drift, freshness, and volume anomaly checks on Iceberg tables via REST Catalog |
| **Cost optimization** | Rolling baselines per pipeline, z-score anomaly detection, optimization recommendations |
| **Policy-gated actions** | 3-tier policy engine (auto-approve / require approval / forbidden) — safe by default |
| **Full observability** | 18 Prometheus metrics, 2 Grafana dashboards, OpenTelemetry distributed tracing |
| **Multi-platform** | Pluggable adapters for ADF, Fabric, Databricks, Kubeflow, and mock (local dev) |
| **LLM intelligence** | Optional hybrid diagnosis — rules for known patterns, Claude or local LLM for novel failures |

---

## Architecture

### System Overview

```mermaid
graph TD
    subgraph Platforms["Lakehouse Platforms"]
        ADF["Azure Data Factory"]
        Fabric["Microsoft Fabric"]
        DBX["Databricks"]
        KF["Kubeflow / KF4X"]
    end

    subgraph Core["Autonomous Lakehouse Ops"]
        Adapter["Adapter Layer"]
        Coord["Coordinator Agent"]
        Rules["Rule Engine — 13 patterns"]
        DQ["DQ Agent — Iceberg"]
        Cost["Cost Agent — Z-score"]
        LLM["LLM Agent — Claude / Ollama"]
        Policy["Policy Engine — 3-tier"]
        Exec["Action Executor — exp. backoff"]
        Audit["Audit Trail — PostgreSQL"]
    end

    subgraph Observability
        Prom["Prometheus — 18 metrics"]
        Graf["Grafana — 2 dashboards"]
    end

    Platforms --> Adapter --> Coord
    Coord --> Rules & DQ & Cost
    Rules -. "low confidence?" .-> LLM
    Rules & LLM & DQ & Cost --> Policy --> Exec --> Audit
    Coord --> Prom --> Graf

    style Rules fill:#e3f2fd,stroke:#1565c0
    style LLM fill:#fff3e0,stroke:#e65100
    style Policy fill:#fce4ec,stroke:#c62828
    style DQ fill:#e8f5e9,stroke:#2e7d32
    style Cost fill:#f3e5f5,stroke:#6a1b9a
```

### Coordinator Workflow

Every failed pipeline event flows through this 8-step process:

```mermaid
graph LR
    A["1. Load event"] --> B{"2. Failed?"}
    B -->|No| Skip["Skip / DQ check"]
    B -->|Yes| C["3. Fetch logs +<br/>metadata"]
    C --> D["4. Diagnose<br/>(rules + LLM)"]
    D --> E["5. Map to<br/>actions"]
    E --> F["6. Apply<br/>policy"]
    F --> G["7. Persist<br/>(DB + audit)"]
    G --> H["8. Return<br/>result"]

    style B fill:#fff3e0,stroke:#e65100
    style D fill:#e3f2fd,stroke:#1565c0
    style F fill:#fce4ec,stroke:#c62828
    style G fill:#e8f5e9,stroke:#2e7d32
```

### Hybrid LLM Diagnosis

Rules handle known patterns fast. The LLM handles novel failures that rules can't classify:

```mermaid
graph TD
    Input["Error Message + Logs"] --> Rules

    Rules["Rule Engine<br/>13 patterns — < 10ms"]
    Rules -->|"high confidence<br/>known pattern"| FastPath["Use rule result"]
    Rules -->|"confidence < 0.7<br/>OR category = UNKNOWN"| LLM

    LLM["LLM Agent<br/>Claude / Ollama — ~1-3s"]
    LLM --> DeepPath["Novel failure classified"]

    LLM -. "LLM fails?" .-> Fallback["Fallback to rule result"]

    style Rules fill:#e3f2fd,stroke:#1565c0
    style LLM fill:#fff3e0,stroke:#e65100
    style FastPath fill:#e8f5e9,stroke:#2e7d32
    style DeepPath fill:#e8f5e9,stroke:#2e7d32
    style Fallback fill:#fce4ec,stroke:#c62828
```

### KF4X Integration

When deployed alongside [Kubeflow4X](https://github.com/AduraX/Kubeflow4X), the platform acts as the operations brain for the entire ML stack:

```mermaid
graph TD
    KC["Keycloak SSO"]

    subgraph KF4X["KF4X Platform"]
        Kubeflow
        MLflow
        Feast
        Iceberg
        Superset
    end

    subgraph Lakehouse["Autonomous Lakehouse Ops"]
        Coordinator
        DQAgent["DQ Agent"]
        CostOpt["Cost Optimizer"]
        Executor
    end

    subgraph Infra["Shared Infrastructure"]
        PG["PostgreSQL"]
        Prom["Prometheus + Grafana"]
        Argo["ArgoCD"]
    end

    KC --> KF4X & Lakehouse
    Lakehouse <--> KF4X
    Lakehouse --> Infra
    Superset --> PG
    Prom --> Lakehouse

    style KC fill:#e8f5e9,stroke:#2e7d32
    style Lakehouse fill:#e3f2fd,stroke:#1565c0
    style KF4X fill:#fff3e0,stroke:#e65100
```

### End-to-End Self-Healing Flow

```mermaid
sequenceDiagram
    participant P as Platform (ADF/Fabric/DBX)
    participant A as Adapter
    participant C as Coordinator
    participant R as Rule Engine
    participant L as LLM (optional)
    participant PO as Policy Engine
    participant E as Executor
    participant DB as PostgreSQL

    P->>A: Pipeline run completed
    A->>C: PipelineEvent (failed)
    C->>A: Fetch logs + metadata
    A-->>C: Logs, pipeline config

    C->>R: Diagnose (error + logs)
    alt Known pattern (high confidence)
        R-->>C: Diagnosis (e.g. TIMEOUT, 0.88)
    else Unknown / low confidence
        R-->>C: UNKNOWN (0.40)
        C->>L: Escalate to LLM
        L-->>C: Diagnosis (e.g. RESOURCE_EXHAUSTION, 0.91)
    end

    C->>PO: Propose actions
    PO-->>C: NOTIFY_OWNER=approved, RETRY=proposed

    C->>DB: Persist diagnosis + actions + audit

    Note over E: Human approves RETRY action

    E->>A: Execute retry (with backoff)
    A->>P: Trigger pipeline rerun
    E->>DB: Action SUCCEEDED + audit log
```

---

## Quick Start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (package manager)
- Docker and Docker Compose (for Postgres, Redis, Prometheus, Grafana)

### 1. Clone and install

```bash
git clone <repository-url>
cd Autonomous_Lakehouse

# Create virtual environment and install dependencies
uv venv --python 3.12
uv pip install -e ".[dev,llm]" aiosqlite
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env — defaults work for local development with mock adapter
```

### 3. Start infrastructure

```bash
docker compose up -d postgres redis prometheus grafana
```

### 4. Run the API server

```bash
PYTHONPATH=src .venv/bin/python -m uvicorn lakehouse.api.app:create_app \
  --factory --reload
```

The API is now running at **http://localhost:8000**. Open **http://localhost:8000/docs** for the interactive Swagger UI.

### 5. Try it out

```bash
# Ingest a failed pipeline event
curl -X POST http://localhost:8000/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "external_run_id": "run-001",
    "pipeline_name": "ingest_sales",
    "platform": "mock",
    "status": "failed",
    "error_message": "Connection timeout to source database after 300s",
    "duration_seconds": 300
  }'

# Coordinate diagnosis and remediation
curl -X POST http://localhost:8000/api/v1/coordinate \
  -H "Content-Type: application/json" \
  -d '{"event_id": 1}'

# Or poll the adapter for events and auto-coordinate
curl -X POST "http://localhost:8000/api/v1/poll?limit=5"
```

### 6. Run tests

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/ -v
# 203 tests collected (187 unit + 16 integration)
```

For a full hands-on walkthrough including cloud platform demos (ADF, Fabric, Databricks), see **[demo.md](demo.md)**.

---

## API Reference

25 REST endpoints across 9 domains:

### Health

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Liveness probe |
| GET | `/health/ready` | Readiness probe (checks DB connectivity) |
| GET | `/metrics` | Prometheus metrics |

### Pipeline Events

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/v1/events` | Ingest a pipeline event |
| GET | `/api/v1/events` | List events (paginated, filterable by status/name) |
| GET | `/api/v1/events/{id}` | Get a single event |

### Diagnoses

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/v1/diagnoses` | Create a diagnosis |
| GET | `/api/v1/diagnoses/{id}` | Get a diagnosis |
| GET | `/api/v1/events/{id}/diagnoses` | List diagnoses for an event |

### Actions

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/v1/actions` | Propose an action (policy-gated) |
| GET | `/api/v1/actions/{id}` | Get an action |
| PATCH | `/api/v1/actions/{id}` | Approve or reject an action |
| POST | `/api/v1/actions/{id}/execute` | Execute an approved action (with retry) |
| GET | `/api/v1/diagnoses/{id}/actions` | List actions for a diagnosis |

### Coordination

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/v1/coordinate` | Trigger coordinator for a pipeline event |
| POST | `/api/v1/poll` | Poll adapter, ingest events, coordinate failures |

### Data Quality

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/v1/quality/namespaces` | List Iceberg catalog namespaces |
| GET | `/api/v1/quality/namespaces/{ns}/tables` | List tables in a namespace |
| POST | `/api/v1/quality/check` | Run DQ checks on a single table |
| POST | `/api/v1/quality/scan` | Scan all tables in a namespace |

### Cost Optimization

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/v1/cost/baselines` | List all pipeline cost baselines |
| GET | `/api/v1/cost/baselines/{name}` | Get baseline for a specific pipeline |
| POST | `/api/v1/cost/analyze` | Analyze an event for cost anomalies |
| GET | `/api/v1/cost/summary` | Aggregated cost dashboard data |

### Adaptive Policy

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/v1/policy/current` | Current policy configuration |
| GET | `/api/v1/policy/stats` | Action approval/rejection stats by group |
| GET | `/api/v1/policy/suggestions` | LLM-powered policy change suggestions |

### Chat Operations

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/v1/chat` | Natural language query about pipeline operations |

---

## Platform Adapters

The system supports multiple data platforms through a pluggable adapter interface. Set `PLATFORM_ADAPTER` in your `.env` file:

| Adapter | Value | Status | Connects to |
|---|---|---|---|
| Mock | `mock` | Ready | Generates fake events for local development |
| Azure Data Factory | `adf` | Stub | ADF REST API |
| Microsoft Fabric | `fabric` | Stub | Fabric REST API |
| Kubeflow | `kubeflow` | Ready | Kubeflow Pipelines v2 API |
| Databricks | `databricks` | Ready | Databricks Jobs API v2.1 |

ADF and Fabric adapters include the API integration structure with mock data — swap to real Azure calls by providing credentials. See **[demo.md](demo.md)** Section B for cloud platform setup guides with credential placeholders.

---

## LLM Integration

The platform supports optional LLM-powered diagnosis. When enabled, the coordinator uses a **hybrid approach**:

1. **Rule engine** runs first (< 10ms, 13 patterns)
2. If rules produce low confidence or UNKNOWN, **LLM is called** (~1-3s)
3. The higher-confidence result is used
4. If the LLM fails, rules provide a safe fallback

### Enable with Claude (remote)

```bash
LLM_ENABLED=true
LLM_PROVIDER=claude
LLM_MODEL=claude-sonnet-4-20250514
ANTHROPIC_API_KEY=sk-ant-...
```

### Enable with Ollama (local)

```bash
LLM_ENABLED=true
LLM_PROVIDER=local
LLM_LOCAL_URL=http://localhost:11434/v1
LLM_LOCAL_MODEL=llama3:8b
```

### Supported local servers

Any server exposing an OpenAI-compatible `/v1/chat/completions` endpoint:

| Server | Default URL |
|---|---|
| [Ollama](https://ollama.com) | `http://localhost:11434/v1` |
| [vLLM](https://docs.vllm.ai) | `http://localhost:8000/v1` |
| [llama.cpp server](https://github.com/ggerganov/llama.cpp) | `http://localhost:8080/v1` |
| [LM Studio](https://lmstudio.ai) | `http://localhost:1234/v1` |

---

## Observability

### Prometheus Metrics

18 custom metrics exposed at `/metrics`, covering events, coordination, diagnoses, actions, data quality, cost anomalies, and adapter calls. All use the `lakehouse_` prefix.

### Grafana Dashboards

Two dashboards auto-provisioned on startup:

- **Pipeline Health** — event rates, success/failure split, duration distribution, coordination latency, diagnoses by category
- **Agent Performance** — actions proposed/executed, execution duration, retry attempts, adapter latency, DQ checks, cost anomalies

Access Grafana at **http://localhost:3000** (admin/admin).

### OpenTelemetry Tracing

Distributed tracing across API requests, coordinator, adapters, and executor. Console export in development, OTLP gRPC in production.

---

## Background Worker

For production workloads, use the arq (Redis-backed) background worker instead of synchronous API calls:

```bash
# Start the worker
PYTHONPATH=src .venv/bin/python -m arq lakehouse.worker.tasks.WorkerSettings
```

Three task types:
- `coordinate_event` — diagnose and propose actions for a failed event
- `execute_action` — execute an approved action with retry
- `poll_events` — poll adapter, ingest events, coordinate failures

---

## Kubernetes Deployment

A Helm chart is included for production deployment:

```bash
helm install lakehouse-ops ./infra/helm/lakehouse-ops \
  --set database.url="postgresql+asyncpg://user:pass@postgres:5432/lakehouse" \
  --set redis.url="redis://redis:6379/0" \
  --set ingress.host="lakehouse-ops.example.com"
```

The chart creates:
- API deployment (2 replicas, liveness/readiness probes, Prometheus annotations)
- Worker deployment (1 replica)
- ClusterIP service
- Ingress (nginx, optional)
- HPA (2-8 replicas, 70% CPU target)
- ConfigMap + Secret for configuration
- ServiceAccount

---

## KF4X Integration

This platform integrates with [Kubeflow4X (KF4X)](https://github.com/AduraX/Kubeflow4X) for enterprise ML platform operations:

| KF4X Component | Integration |
|---|---|
| Kubeflow Pipelines | `kubeflow` adapter monitors pipeline runs |
| Iceberg REST Catalog | DQ agent validates table quality |
| Keycloak | JWT auth middleware for SSO |
| PostgreSQL | Shared database instance |
| ArgoCD | GitOps deployment of the operations platform |
| Superset | BI dashboards query operations data |
| Prometheus/Grafana | Fills KF4X's observability gap |

---

## Project Structure

```
src/lakehouse/
  adapters/          Platform adapters (ADF, Fabric, Databricks, Kubeflow, Mock)
  agents/            Specialist agents (Coordinator, DQ, Cost, LLM, Summarizer, Executor, ChatOps, PolicyLearner)
  api/               FastAPI application, routes, middleware
  llm/               LLM provider abstraction (Claude, Local/Ollama)
  models/            SQLAlchemy ORM models
  schemas/           Pydantic request/response schemas
  worker/            arq background worker tasks
  config.py          Pydantic settings (all config from env vars)
  database.py        Async SQLAlchemy engine + session
  logging.py         Structured JSON logging (structlog)
  policies.py        Static policy classification engine
  telemetry.py       OpenTelemetry tracing + Prometheus metrics

infra/
  grafana/           Dashboard JSON + provisioning config
  helm/              Kubernetes Helm chart
  prometheus/        Scrape configuration

tests/               203 tests (187 unit + 16 integration, pytest, async, in-memory SQLite)
```

---

## Development

### Run linting and type checks

```bash
# Lint
.venv/bin/ruff check src/ tests/

# Format
.venv/bin/ruff format src/ tests/

# Type check
.venv/bin/mypy src/
```

### Install pre-commit hooks

```bash
.venv/bin/pre-commit install
```

Hooks run on every commit: ruff (lint + format), mypy (types), yaml/json validation, large file check, branch protection.

### Run tests with coverage

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/ -v --cov=lakehouse --cov-report=term-missing
```

---

## Configuration Reference

All configuration is via environment variables (or `.env` file). See [`.env.example`](.env.example) for the full list.

| Variable | Default | Description |
|---|---|---|
| `APP_ENV` | `development` | Environment (development/staging/production) |
| `DATABASE_URL` | `postgresql+asyncpg://...` | Async PostgreSQL connection string |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis for background worker |
| `PLATFORM_ADAPTER` | `mock` | Platform adapter (mock/adf/fabric/databricks/kubeflow) |
| `LLM_ENABLED` | `false` | Enable LLM-powered diagnosis |
| `LLM_PROVIDER` | `claude` | LLM provider (claude/local) |
| `ANTHROPIC_API_KEY` | | Claude API key (when provider=claude, requires `[llm]` extra) |
| `LLM_LOCAL_URL` | `http://localhost:11434/v1` | Local model server URL |
| `LLM_LOCAL_MODEL` | `llama3:8b` | Local model name |
| `ICEBERG_REST_CATALOG_URL` | | Iceberg catalog for DQ checks |
| `KEYCLOAK_URL` | | Keycloak server for JWT auth |

---

## Documentation

| Document | Description |
|---|---|
| **[demo.md](demo.md)** | Hands-on walkthrough — local mock demo + cloud platform demos (ADF, Fabric, Databricks) |
| **[.env.example](.env.example)** | Complete environment variable reference |

---

## Future Enhancements

| Area | Description |
|---|---|
| **Real cloud testing** | Wire up ADF/Fabric/Databricks credentials and run the 16 integration tests against live platforms |
| **Deploy to Kubernetes** | Use the included Helm chart with real Postgres, Redis, and ingress infrastructure |
| **Enable LLM diagnosis** | Set `LLM_ENABLED=true` with an Anthropic API key or local Ollama server for hybrid AI diagnosis |
| **WebSocket/SSE streaming** | Add real-time event streaming to replace polling for live dashboards |
| **Slack/Teams notifications** | Wire `NOTIFY_OWNER` actions to actual messaging services (Slack webhooks, Teams connectors) |
| **Historical analytics** | Trend dashboards for failure rate over time, MTTR tracking, and incident heatmaps |
| **Multi-tenant support** | Per-team namespacing of events, policies, and RBAC scoping via Keycloak roles |
| **Scheduled DQ scans** | Cron-based Iceberg table quality scans via the background worker |

---

## License

This project is licensed under the [GNU Affero General Public License v3.0 (AGPL-3.0)](LICENSE).
