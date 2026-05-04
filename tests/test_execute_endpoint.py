"""Tests for the action execution endpoint (POST /api/v1/actions/{id}/execute)."""

from httpx import AsyncClient


async def _create_approved_action(client: AsyncClient) -> int:
    """Create an event → diagnosis → approved action, return the action ID."""
    # Create event
    resp = await client.post(
        "/api/v1/events",
        json={
            "external_run_id": "exec-api-test",
            "pipeline_name": "ingest_sales",
            "platform": "mock",
            "status": "failed",
            "error_message": "Schema mismatch",
        },
    )
    event_id = resp.json()["id"]

    # Create diagnosis
    resp = await client.post(
        "/api/v1/diagnoses",
        json={
            "event_id": event_id,
            "category": "schema_mismatch",
            "confidence": 0.9,
            "summary": "Schema mismatch detected",
            "agent_name": "test",
        },
    )
    diagnosis_id = resp.json()["id"]

    # Create action (NOTIFY_OWNER is auto-approved by policy)
    resp = await client.post(
        "/api/v1/actions",
        json={
            "diagnosis_id": diagnosis_id,
            "action_type": "notify_owner",
            "description": "Notify owner about schema change",
        },
    )
    assert resp.json()["status"] == "approved"
    return resp.json()["id"]


async def test_execute_approved_action(client: AsyncClient) -> None:
    """Should execute an approved action and return succeeded status."""
    action_id = await _create_approved_action(client)

    resp = await client.post(f"/api/v1/actions/{action_id}/execute")
    assert resp.status_code == 200

    data = resp.json()
    assert data["status"] == "succeeded"


async def test_execute_proposed_action_rejected(client: AsyncClient) -> None:
    """Should reject execution of a non-approved action."""
    # Create event + diagnosis
    resp = await client.post(
        "/api/v1/events",
        json={
            "external_run_id": "exec-reject-test",
            "pipeline_name": "test",
            "platform": "mock",
            "status": "failed",
            "error_message": "timeout",
        },
    )
    event_id = resp.json()["id"]

    resp = await client.post(
        "/api/v1/diagnoses",
        json={
            "event_id": event_id,
            "category": "timeout",
            "confidence": 0.85,
            "summary": "Timeout",
            "agent_name": "test",
        },
    )
    diagnosis_id = resp.json()["id"]

    # Create RETRY_PIPELINE action (requires approval → PROPOSED status)
    resp = await client.post(
        "/api/v1/actions",
        json={
            "diagnosis_id": diagnosis_id,
            "action_type": "retry_pipeline",
            "description": "Retry the pipeline",
        },
    )
    action_id = resp.json()["id"]
    assert resp.json()["status"] == "proposed"

    # Try to execute — should fail
    resp = await client.post(f"/api/v1/actions/{action_id}/execute")
    assert resp.status_code == 400


async def test_execute_nonexistent_action(client: AsyncClient) -> None:
    """Should return 400 for non-existent action."""
    resp = await client.post("/api/v1/actions/99999/execute")
    assert resp.status_code == 400


async def test_approve_then_execute(client: AsyncClient) -> None:
    """Full flow: create proposed action → approve → execute."""
    # Create event + diagnosis
    resp = await client.post(
        "/api/v1/events",
        json={
            "external_run_id": "approve-exec-test",
            "pipeline_name": "test",
            "platform": "mock",
            "status": "failed",
            "error_message": "timeout",
        },
    )
    event_id = resp.json()["id"]

    resp = await client.post(
        "/api/v1/diagnoses",
        json={
            "event_id": event_id,
            "category": "timeout",
            "confidence": 0.85,
            "summary": "Timeout detected",
            "agent_name": "test",
        },
    )
    diagnosis_id = resp.json()["id"]

    # Create action (PROPOSED)
    resp = await client.post(
        "/api/v1/actions",
        json={
            "diagnosis_id": diagnosis_id,
            "action_type": "retry_pipeline",
            "description": "Retry the pipeline",
        },
    )
    action_id = resp.json()["id"]
    assert resp.json()["status"] == "proposed"

    # Approve it
    resp = await client.patch(
        f"/api/v1/actions/{action_id}",
        json={
            "status": "approved",
            "approved_by": "engineer@company.com",
        },
    )
    assert resp.json()["status"] == "approved"

    # Execute it
    resp = await client.post(f"/api/v1/actions/{action_id}/execute")
    assert resp.status_code == 200
    assert resp.json()["status"] == "succeeded"
