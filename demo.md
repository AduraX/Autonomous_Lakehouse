# Demo Walkthrough

A hands-on guide to the Autonomous Lakehouse Operations Platform. Follow along step by step to see the full self-healing lifecycle — from pipeline failure to automated remediation.

This walkthrough has two sections:

- **Section A**: Local demo with the mock adapter (no cloud credentials needed)
- **Section B**: Cloud demos for Azure Data Factory, Microsoft Fabric, and Databricks

---

# Section A: Local Demo (Mock Adapter)

Everything runs locally. No cloud accounts, no credentials, no infrastructure beyond Docker.

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- Docker and Docker Compose
- `curl` and `jq` (for pretty-printing JSON)

## Step 0: Start the Platform

```bash
# Clone and install
git clone <repository-url>
cd Autonomous_Lakehouse

uv venv --python 3.12
uv pip install -e ".[dev,llm]" aiosqlite

# Copy environment config (defaults work for local demo)
cp .env.example .env

# Start Postgres, Redis, Prometheus, Grafana
docker compose up -d postgres redis prometheus grafana

# Start the API server
PYTHONPATH=src .venv/bin/python -m uvicorn lakehouse.api.app:create_app \
  --factory --host 0.0.0.0 --port 8000 --reload
```

Verify it's running:

```bash
curl -s http://localhost:8000/health | jq
```

Expected:
```json
{
  "status": "healthy"
}
```

---

## Step 1: Ingest a Failed Pipeline Event

Simulate a pipeline that failed due to a connection timeout:

```bash
curl -s -X POST http://localhost:8000/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "external_run_id": "adf-run-demo-001",
    "pipeline_name": "ingest_sales_data",
    "platform": "adf",
    "status": "failed",
    "error_message": "Connection timeout to source database after 300s",
    "error_code": "TIMEOUT_5001",
    "duration_seconds": 300.5,
    "started_at": "2026-04-29T08:00:00Z",
    "finished_at": "2026-04-29T08:05:00Z"
  }' | jq
```

Expected:
```json
{
  "id": 1,
  "external_run_id": "adf-run-demo-001",
  "pipeline_name": "ingest_sales_data",
  "platform": "adf",
  "status": "failed",
  "error_message": "Connection timeout to source database after 300s",
  "error_code": "TIMEOUT_5001",
  "duration_seconds": 300.5,
  ...
}
```

**What happened**: The event was ingested into PostgreSQL. No diagnosis yet — that's the next step.

---

## Step 2: Trigger the Coordinator

Tell the coordinator to diagnose and propose remediation for event #1:

```bash
curl -s -X POST http://localhost:8000/api/v1/coordinate \
  -H "Content-Type: application/json" \
  -d '{"event_id": 1}' | jq
```

Expected:
```json
{
  "event_id": 1,
  "findings": [
    {
      "agent_name": "failure_diagnosis",
      "category": "timeout",
      "confidence": 0.88,
      "summary": "Connection timeout to data source",
      "details": {
        "matched_keywords": ["connection timeout"],
        "error_message": "Connection timeout to source database after 300s",
        "error_code": "TIMEOUT_5001"
      }
    }
  ],
  "proposed_actions": [
    {
      "action_type": "retry_pipeline",
      "description": "Retry failed pipeline — diagnosed as timeout",
      "policy": "requires_approval"
    },
    {
      "action_type": "scale_compute",
      "description": "Scale compute resources to prevent resource exhaustion",
      "policy": "requires_approval"
    }
  ],
  "summary": "Diagnosed 'timeout' with 88% confidence. Proposed 2 remediation action(s).",
  "coordinated_at": "2026-04-29T08:10:00Z"
}
```

**What happened**:
1. The rule engine matched `"connection timeout"` → category `TIMEOUT` (0.88 confidence)
2. TIMEOUT maps to `[RETRY_PIPELINE, SCALE_COMPUTE]`
3. Both actions require human approval (policy engine)
4. A `Diagnosis` and two `Action` records were persisted to the database

---

## Step 3: Review the Proposed Actions

List actions for the diagnosis:

```bash
# First, get the diagnosis ID
curl -s http://localhost:8000/api/v1/events/1/diagnoses | jq '.[0].id'
# Returns: 1

# List actions for diagnosis #1
curl -s http://localhost:8000/api/v1/diagnoses/1/actions | jq
```

Expected:
```json
[
  {
    "id": 1,
    "diagnosis_id": 1,
    "action_type": "retry_pipeline",
    "description": "Retry failed pipeline — diagnosed as timeout",
    "status": "proposed",
    "approved_by": null,
    "executed_by": "coordinator_agent",
    ...
  },
  {
    "id": 2,
    "diagnosis_id": 1,
    "action_type": "scale_compute",
    "description": "Scale compute resources to prevent resource exhaustion",
    "status": "proposed",
    ...
  }
]
```

Both actions are `"proposed"` — waiting for approval.

---

## Step 4: Approve an Action

Approve the retry action:

```bash
curl -s -X PATCH http://localhost:8000/api/v1/actions/1 \
  -H "Content-Type: application/json" \
  -d '{
    "status": "approved",
    "approved_by": "engineer@company.com"
  }' | jq '.status, .approved_by'
```

Expected:
```
"approved"
"engineer@company.com"
```

---

## Step 5: Execute the Approved Action

Execute with exponential backoff retry:

```bash
curl -s -X POST http://localhost:8000/api/v1/actions/1/execute | jq
```

Expected:
```json
{
  "id": 1,
  "action_type": "retry_pipeline",
  "status": "succeeded",
  "result_json": "{\"status\": \"triggered\", \"new_run_id\": \"mock-retry-adf-run-demo-001\"}",
  "error_message": null,
  ...
}
```

**What happened**: The action executor called `adapter.retry_pipeline()` via the mock adapter. In production with a real ADF adapter, this would trigger an actual pipeline rerun.

Try executing the non-approved action — it should fail:

```bash
curl -s -X POST http://localhost:8000/api/v1/actions/2/execute | jq
```

Expected:
```json
{
  "detail": "Action 2 is 'proposed', must be 'approved' to execute"
}
```

The policy engine prevents executing unapproved actions.

---

## Step 6: Automated Polling Loop

Instead of manually ingesting events and coordinating, use the poll endpoint:

```bash
curl -s -X POST "http://localhost:8000/api/v1/poll?limit=5" | jq
```

Expected:
```json
{
  "ingested": 5,
  "coordinated": 2,
  "cost_anomalies": 0,
  "polled_at": "2026-04-29T08:15:00Z",
  "coordination_results": [
    {
      "event_id": 3,
      "pipeline": "validate_inventory_feed",
      "category": "schema_mismatch",
      "actions": 2
    },
    {
      "event_id": 5,
      "pipeline": "sync_crm_contacts",
      "category": "timeout",
      "actions": 2
    }
  ]
}
```

**What happened**: The mock adapter generated 5 fake pipeline events (~60% succeed, ~30% fail, ~10% timeout). Failed events were automatically diagnosed and actions were proposed.

---

## Step 7: Ingest a Schema Mismatch

```bash
curl -s -X POST http://localhost:8000/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "external_run_id": "fabric-run-demo-002",
    "pipeline_name": "load_dimension_products",
    "platform": "fabric",
    "status": "failed",
    "error_message": "Column '\''category_id'\'' not found in source table '\''products_raw'\''",
    "error_code": "SCHEMA_ERROR_4012",
    "duration_seconds": 45.2
  }' | jq '.id'
# Returns the event ID (e.g., 8)

# Coordinate it
curl -s -X POST http://localhost:8000/api/v1/coordinate \
  -H "Content-Type: application/json" \
  -d '{"event_id": 8}' | jq '.findings[0].category, .proposed_actions[].action_type'
```

Expected:
```
"schema_mismatch"
"block_downstream"
"notify_owner"
```

**What happened**: The rule engine matched `["column", "not found"]` → `SCHEMA_MISMATCH`. The system proposes blocking downstream tables (prevent bad data propagation) and notifying the owner.

---

## Step 8: Cost Baseline and Anomaly Detection

Build a baseline by ingesting several normal events:

```bash
# Ingest 6 normal events (~100-120s duration)
for i in $(seq 1 6); do
  curl -s -X POST http://localhost:8000/api/v1/events \
    -H "Content-Type: application/json" \
    -d "{
      \"external_run_id\": \"cost-demo-$i\",
      \"pipeline_name\": \"etl_daily_revenue\",
      \"platform\": \"databricks\",
      \"status\": \"succeeded\",
      \"duration_seconds\": $((100 + RANDOM % 20))
    }" > /dev/null
done

# Analyze each to build the baseline
for i in $(seq 9 14); do
  curl -s -X POST http://localhost:8000/api/v1/cost/analyze \
    -H "Content-Type: application/json" \
    -d "{\"event_id\": $i}" > /dev/null
done

# Check the baseline
curl -s http://localhost:8000/api/v1/cost/baselines/etl_daily_revenue | jq '{
  pipeline_name, avg_duration, stddev_duration, total_runs, failure_rate
}'
```

Expected (values will vary):
```json
{
  "pipeline_name": "etl_daily_revenue",
  "avg_duration": 109.5,
  "stddev_duration": 6.2,
  "total_runs": 6,
  "failure_rate": 0.0
}
```

Now inject a cost spike:

```bash
# Ingest a 600-second run (5x normal)
curl -s -X POST http://localhost:8000/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "external_run_id": "cost-demo-spike",
    "pipeline_name": "etl_daily_revenue",
    "platform": "databricks",
    "status": "succeeded",
    "duration_seconds": 600
  }' | jq '.id'
# Returns event ID (e.g., 15)

# Analyze the spike
curl -s -X POST http://localhost:8000/api/v1/cost/analyze \
  -H "Content-Type: application/json" \
  -d '{"event_id": 15}' | jq
```

Expected:
```json
{
  "event_id": 15,
  "pipeline_name": "etl_daily_revenue",
  "duration_seconds": 600.0,
  "findings": [
    {
      "anomaly_type": "duration_spike",
      "severity": "high",
      "message": "Duration spike: 600s is +448% over baseline (avg 110s, z-score 79.1)",
      "recommendations": [
        "Review query execution plan for regressions",
        "Check for data volume spikes in source tables",
        "Consider scaling compute resources",
        "Review partitioning strategy for large tables"
      ]
    }
  ],
  "actions_proposed": 2,
  ...
}
```

Check the cost summary:

```bash
curl -s http://localhost:8000/api/v1/cost/summary | jq '{
  total_pipelines, total_runs, total_duration_hours, 
  anomaly_pipelines: [.anomaly_pipelines[].pipeline_name]
}'
```

---

## Step 9: View the Audit Trail

All events have been ingested — check the full list:

```bash
curl -s "http://localhost:8000/api/v1/events?page_size=5" | jq '{
  total, items: [.items[] | {id, pipeline_name, status, platform}]
}'
```

List all diagnoses:

```bash
curl -s http://localhost:8000/api/v1/events/1/diagnoses | jq '.[].{category, confidence, summary, agent_name}'
```

---

## Step 10: Check Prometheus Metrics

```bash
curl -s http://localhost:8000/metrics | grep lakehouse_
```

Expected (sample):
```
lakehouse_events_ingested_total{platform="adf",status="failed"} 1.0
lakehouse_events_ingested_total{platform="mock",status="succeeded"} 3.0
lakehouse_coordinations_total{category="timeout"} 1.0
lakehouse_coordinations_total{category="schema_mismatch"} 1.0
lakehouse_actions_proposed_total{action_type="retry_pipeline",policy="requires_approval"} 1.0
lakehouse_actions_executed_total{action_type="retry_pipeline",status="succeeded"} 1.0
lakehouse_cost_anomalies_total{anomaly_type="duration_spike",severity="high"} 1.0
```

Open Grafana at **http://localhost:3000** (admin/admin) to see the dashboards.

---

## Step 11: Test the Full End-to-End Loop

One command that does everything — poll, diagnose, propose:

```bash
curl -s -X POST "http://localhost:8000/api/v1/poll?limit=10" | jq '{
  ingested, coordinated, cost_anomalies,
  failures: [.coordination_results[] | {pipeline, category, actions}]
}'
```

This single call:
1. Fetches 10 events from the mock adapter
2. Ingests each into the database
3. Updates cost baselines for completed events
4. Diagnoses failures and proposes remediation actions
5. Returns a summary

**Congratulations** — you've completed the local demo. The platform is detecting failures, diagnosing root causes, proposing actions, enforcing policies, executing with retry, tracking costs, and exposing metrics.

---

---

# Section B: Cloud Demos

These demos use real cloud platform adapters. Each requires platform-specific credentials.

---

## B1: Azure Data Factory (ADF)

### Prerequisites

- An Azure subscription with an ADF instance
- A [Service Principal](https://learn.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal) with **Data Factory Contributor** role
- At least one pipeline configured in ADF (can be a simple copy activity)

### Configure Credentials

Edit your `.env` file:

```bash
# --- Platform ---
PLATFORM_ADAPTER=adf

# --- Azure Data Factory ---
ADF_SUBSCRIPTION_ID=<your-azure-subscription-id>        # e.g., 12345678-abcd-efgh-ijkl-123456789012
ADF_RESOURCE_GROUP=<your-resource-group>                 # e.g., rg-data-engineering-prod
ADF_FACTORY_NAME=<your-adf-factory-name>                 # e.g., adf-lakehouse-prod
ADF_TENANT_ID=<your-azure-ad-tenant-id>                  # e.g., 87654321-dcba-hgfe-lkji-210987654321
ADF_CLIENT_ID=<your-service-principal-client-id>         # e.g., abcdef12-3456-7890-abcd-ef1234567890
ADF_CLIENT_SECRET=<your-service-principal-client-secret> # e.g., Xyz~1234567890abcdefghijklmnopqrst

# --- LLM (optional but recommended for ADF error messages) ---
LLM_ENABLED=true
LLM_PROVIDER=claude
ANTHROPIC_API_KEY=<your-anthropic-api-key>               # e.g., sk-ant-api03-...
```

> **Where to find these values:**
> - **Subscription ID**: Azure Portal → Subscriptions
> - **Resource Group**: Azure Portal → Resource Groups
> - **Factory Name**: Azure Portal → Data Factory → Overview
> - **Tenant ID / Client ID / Client Secret**: Azure Portal → App Registrations → your service principal

### Restart the API

```bash
# Stop the running server (Ctrl+C) and restart
PYTHONPATH=src .venv/bin/python -m uvicorn lakehouse.api.app:create_app \
  --factory --host 0.0.0.0 --port 8000 --reload
```

### Poll ADF for Recent Pipeline Runs

```bash
curl -s -X POST "http://localhost:8000/api/v1/poll?limit=10" | jq
```

This calls the ADF REST API:
```
GET https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroup}/providers/Microsoft.DataFactory/factories/{factoryName}/pipelineruns?api-version=2018-06-01
```

Expected response:
```json
{
  "ingested": 10,
  "coordinated": 3,
  "cost_anomalies": 0,
  "coordination_results": [
    {
      "event_id": 1,
      "pipeline": "CopyFromBlobToSQL",
      "category": "timeout",
      "actions": 2
    }
  ]
}
```

### Trigger a Retry via ADF API

```bash
# Approve the retry action
curl -s -X PATCH http://localhost:8000/api/v1/actions/1 \
  -H "Content-Type: application/json" \
  -d '{"status": "approved", "approved_by": "data-engineer@company.com"}'

# Execute — this calls ADF's CreateRun API
curl -s -X POST http://localhost:8000/api/v1/actions/1/execute | jq
```

In production, this calls:
```
POST https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroup}/providers/Microsoft.DataFactory/factories/{factoryName}/pipelines/{pipelineName}/createRun?api-version=2018-06-01
```

### ADF-Specific Error Examples

Try ingesting these common ADF error patterns:

```bash
# ErrorCode: 2011 — Source dataset connection failure
curl -s -X POST http://localhost:8000/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "external_run_id": "adf-prod-run-8821",
    "pipeline_name": "CopyFromBlobToSQL",
    "platform": "adf",
    "status": "failed",
    "error_message": "ErrorCode=SqlFailedToConnect, The TCP/IP connection to the host sql-prod.database.windows.net, port 1433 has failed. Connection timeout.",
    "error_code": "2011"
  }'

# ErrorCode: 2200 — Azure Blob not found
curl -s -X POST http://localhost:8000/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "external_run_id": "adf-prod-run-8822",
    "pipeline_name": "IngestCustomerData",
    "platform": "adf",
    "status": "failed",
    "error_message": "The specified container does not exist. RequestId: abc-123. StatusCode: 404. ErrorCode: ContainerNotFound",
    "error_code": "2200"
  }'

# Coordinate both
curl -s -X POST http://localhost:8000/api/v1/coordinate -d '{"event_id": 1}' | jq '.findings[0].category'
# → "timeout"

curl -s -X POST http://localhost:8000/api/v1/coordinate -d '{"event_id": 2}' | jq '.findings[0].category'
# → "source_unavailable"
```

---

## B2: Microsoft Fabric

### Prerequisites

- A [Microsoft Fabric](https://www.microsoft.com/en-us/microsoft-fabric) workspace
- A Service Principal with **Fabric Workspace Contributor** permissions
- At least one pipeline or notebook configured in Fabric

### Configure Credentials

```bash
# --- Platform ---
PLATFORM_ADAPTER=fabric

# --- Microsoft Fabric ---
FABRIC_WORKSPACE_ID=<your-fabric-workspace-id>           # e.g., a1b2c3d4-e5f6-7890-abcd-ef1234567890
FABRIC_TENANT_ID=<your-azure-ad-tenant-id>               # e.g., 87654321-dcba-hgfe-lkji-210987654321
FABRIC_CLIENT_ID=<your-service-principal-client-id>      # e.g., abcdef12-3456-7890-abcd-ef1234567890
FABRIC_CLIENT_SECRET=<your-service-principal-secret>     # e.g., Xyz~1234567890abcdefghijklmnopqrst

# --- Iceberg (if using Fabric Lakehouse with Iceberg) ---
ICEBERG_REST_CATALOG_URL=<your-iceberg-catalog-url>      # e.g., https://onelake.dfs.fabric.microsoft.com
ICEBERG_WAREHOUSE=<your-warehouse-path>                  # e.g., abfss://workspace@onelake.dfs.fabric.microsoft.com
```

> **Where to find these values:**
> - **Workspace ID**: Fabric Portal → Workspace → Settings → About this workspace
> - **Tenant / Client / Secret**: Azure Portal → App Registrations (same service principal as ADF, or create a new one)

### Poll Fabric for Recent Runs

```bash
curl -s -X POST "http://localhost:8000/api/v1/poll?limit=10" | jq
```

This calls the Fabric REST API:
```
GET https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances
```

### Fabric-Specific Error Examples

```bash
# Lakehouse write failure — schema evolution conflict
curl -s -X POST http://localhost:8000/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "external_run_id": "fabric-run-7721",
    "pipeline_name": "IngestToLakehouse_Sales",
    "platform": "fabric",
    "status": "failed",
    "error_message": "Schema mismatch: column '\''revenue_usd'\'' (type: decimal(18,2)) cannot be written as string. Target table schema has evolved since the pipeline was last updated.",
    "error_code": "LAKEHOUSE_SCHEMA_4050"
  }'

# Spark session timeout
curl -s -X POST http://localhost:8000/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "external_run_id": "fabric-run-7722",
    "pipeline_name": "TransformNotebook_CustomerSegmentation",
    "platform": "fabric",
    "status": "timed_out",
    "error_message": "Spark session timed out after 3600 seconds. The notebook execution exceeded the maximum allowed runtime.",
    "error_code": "SPARK_TIMEOUT_5010",
    "duration_seconds": 3600
  }'

# Coordinate
curl -s -X POST http://localhost:8000/api/v1/coordinate -d '{"event_id": 1}' | jq '.findings[0].category'
# → "schema_mismatch"

curl -s -X POST http://localhost:8000/api/v1/coordinate -d '{"event_id": 2}' | jq '.findings[0].category'
# → "timeout"
```

### Run Data Quality Checks on Fabric Iceberg Tables

If your Fabric Lakehouse exposes an Iceberg REST Catalog:

```bash
# List namespaces
curl -s http://localhost:8000/api/v1/quality/namespaces | jq

# Check a specific table
curl -s -X POST http://localhost:8000/api/v1/quality/check \
  -H "Content-Type: application/json" \
  -d '{
    "namespace": "lakehouse",
    "table": "sales_fact",
    "expected_columns": ["order_id", "customer_id", "amount", "order_date", "region"],
    "max_age_hours": 12
  }' | jq
```

---

## B3: Databricks

### Prerequisites

- A [Databricks](https://www.databricks.com/) workspace (AWS, Azure, or GCP)
- A [Personal Access Token](https://docs.databricks.com/en/dev-tools/auth/pat.html) or Service Principal token
- At least one job configured in Databricks

### Configure Credentials

```bash
# --- Platform ---
PLATFORM_ADAPTER=databricks

# --- Databricks ---
DATABRICKS_WORKSPACE_URL=<your-databricks-workspace-url> # e.g., https://adb-1234567890123456.7.azuredatabricks.net
DATABRICKS_TOKEN=<your-personal-access-token>            # e.g., dapi1234567890abcdef1234567890abcdef

# --- Iceberg (if using Unity Catalog with Iceberg) ---
ICEBERG_REST_CATALOG_URL=<your-unity-catalog-url>        # e.g., https://adb-123.7.azuredatabricks.net/api/2.1/unity-catalog/iceberg
ICEBERG_WAREHOUSE=<your-catalog-name>                    # e.g., main
```

> **Where to find these values:**
> - **Workspace URL**: Databricks workspace → browser URL bar (e.g., `https://adb-1234567890.7.azuredatabricks.net`)
> - **Personal Access Token**: Databricks → User Settings → Developer → Access Tokens → Generate New Token
> - **Service Principal Token**: Databricks → Admin Console → Service Principals → OAuth Secret

### Poll Databricks for Recent Job Runs

```bash
curl -s -X POST "http://localhost:8000/api/v1/poll?limit=10" | jq
```

This calls the Databricks Jobs API:
```
GET https://{host}/api/2.1/jobs/runs/list?limit=10&expand_tasks=true
```

### Databricks-Specific Error Examples

```bash
# Cluster OOM — Spark executor killed
curl -s -X POST http://localhost:8000/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "external_run_id": "dbx-run-44210",
    "pipeline_name": "ETL_FeatureEngineering",
    "platform": "databricks",
    "status": "failed",
    "error_message": "SparkException: Job aborted due to stage failure: Task 14 in stage 23.0 failed 4 times. Container killed by YARN for exceeding memory limits. 15.2 GB of 15 GB physical memory used.",
    "error_code": "SPARK_OOM",
    "duration_seconds": 1847,
    "metadata_json": "{\"cluster_id\": \"0429-123456-abcdefgh\", \"cluster_type\": \"Standard_DS3_v2\", \"num_workers\": 4}"
  }'

# Unity Catalog permission denied
curl -s -X POST http://localhost:8000/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "external_run_id": "dbx-run-44211",
    "pipeline_name": "WriteToDeltaLake_Customers",
    "platform": "databricks",
    "status": "failed",
    "error_message": "PERMISSION_DENIED: User does not have USE SCHEMA permission on schema main.production. Required permissions: USE SCHEMA on main.production.",
    "error_code": "PERMISSION_DENIED"
  }'

# Delta table constraint violation
curl -s -X POST http://localhost:8000/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "external_run_id": "dbx-run-44212",
    "pipeline_name": "MergeInto_OrdersFact",
    "platform": "databricks",
    "status": "failed",
    "error_message": "CHECK constraint customer_id_not_null (customer_id IS NOT NULL) violated by row with null values in: customer_id",
    "error_code": "DELTA_CONSTRAINT_VIOLATION"
  }'

# Coordinate all three
for id in 1 2 3; do
  echo "--- Event $id ---"
  curl -s -X POST http://localhost:8000/api/v1/coordinate \
    -H "Content-Type: application/json" \
    -d "{\"event_id\": $id}" | jq '{category: .findings[0].category, confidence: .findings[0].confidence, actions: [.proposed_actions[].action_type]}'
done
```

Expected:
```
--- Event 1 ---
{
  "category": "resource_exhaustion",
  "confidence": 0.9,
  "actions": ["scale_compute", "retry_pipeline"]
}
--- Event 2 ---
{
  "category": "permission_denied",
  "confidence": 0.92,
  "actions": ["refresh_credentials", "notify_owner"]
}
--- Event 3 ---
{
  "category": "data_quality",
  "confidence": 0.84,
  "actions": ["block_downstream", "notify_owner"]
}
```

### Cost Analysis for Databricks Jobs

Databricks jobs are often the most expensive pipelines. After ingesting several runs:

```bash
# Build a baseline from normal runs
for i in $(seq 1 8); do
  curl -s -X POST http://localhost:8000/api/v1/events \
    -H "Content-Type: application/json" \
    -d "{
      \"external_run_id\": \"dbx-cost-$i\",
      \"pipeline_name\": \"ETL_FeatureEngineering\",
      \"platform\": \"databricks\",
      \"status\": \"succeeded\",
      \"duration_seconds\": $((1800 + RANDOM % 200))
    }" > /dev/null
  # Analyze each
  curl -s -X POST http://localhost:8000/api/v1/cost/analyze \
    -d "{\"event_id\": $((i))}" > /dev/null
done

# Check the baseline
curl -s http://localhost:8000/api/v1/cost/baselines/ETL_FeatureEngineering | jq '{
  avg_duration, stddev_duration, total_runs, total_duration_seconds,
  avg_cost_per_run
}'

# Get the full cost summary
curl -s http://localhost:8000/api/v1/cost/summary | jq
```

---

## B4: Multi-Platform Demo

The real power is monitoring **all platforms simultaneously**. Configure multiple adapters by switching `PLATFORM_ADAPTER` between polls, or ingest events from different platforms directly:

```bash
# ADF event
curl -s -X POST http://localhost:8000/api/v1/events -H "Content-Type: application/json" \
  -d '{"external_run_id": "adf-001", "pipeline_name": "CopyBlob", "platform": "adf", "status": "failed", "error_message": "Connection timeout", "duration_seconds": 120}'

# Fabric event
curl -s -X POST http://localhost:8000/api/v1/events -H "Content-Type: application/json" \
  -d '{"external_run_id": "fab-001", "pipeline_name": "IngestLakehouse", "platform": "fabric", "status": "failed", "error_message": "Schema mismatch on revenue column", "duration_seconds": 45}'

# Databricks event
curl -s -X POST http://localhost:8000/api/v1/events -H "Content-Type: application/json" \
  -d '{"external_run_id": "dbx-001", "pipeline_name": "TrainMLModel", "platform": "databricks", "status": "failed", "error_message": "Out of memory: 16GB exceeded", "duration_seconds": 2400}'

# Kubeflow event
curl -s -X POST http://localhost:8000/api/v1/events -H "Content-Type: application/json" \
  -d '{"external_run_id": "kf-001", "pipeline_name": "feature_pipeline_v3", "platform": "kubeflow", "status": "failed", "error_message": "Upstream pipeline not completed within SLA", "duration_seconds": 900}'

# Coordinate all four
for id in 1 2 3 4; do
  curl -s -X POST http://localhost:8000/api/v1/coordinate -d "{\"event_id\": $id}" | jq '{platform: .findings[0].details.error_message, category: .findings[0].category}'
done

# See the full picture
curl -s "http://localhost:8000/api/v1/events?page_size=10" | jq '{
  total,
  platforms: [.items[] | .platform] | unique,
  statuses: [.items[] | .status] | unique
}'
```

---

## Next Steps After the Demo

| What | How |
|---|---|
| **Start the background worker** | `PYTHONPATH=src .venv/bin/python -m arq lakehouse.worker.tasks.WorkerSettings` |
| **View Grafana dashboards** | http://localhost:3000 (admin/admin) |
| **Enable LLM diagnosis** | Set `LLM_ENABLED=true` + `ANTHROPIC_API_KEY` in `.env` |
| **Run on Kubernetes** | `helm install lakehouse-ops ./infra/helm/lakehouse-ops --set ...` |
| **Run tests** | `PYTHONPATH=src .venv/bin/python -m pytest tests/ -v` |
| **View API docs** | http://localhost:8000/docs (Swagger UI) |
