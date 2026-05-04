"""Tests for Diagnosis and Action API endpoints."""

from httpx import AsyncClient


async def _create_event(client: AsyncClient) -> int:
    """Helper: create a pipeline event and return its ID."""
    resp = await client.post(
        "/api/v1/events",
        json={
            "external_run_id": "run-diag-test",
            "pipeline_name": "ingest_sales_data",
            "platform": "mock",
            "status": "failed",
            "error_message": "Schema mismatch",
        },
    )
    return resp.json()["id"]


async def _create_diagnosis(client: AsyncClient, event_id: int) -> int:
    """Helper: create a diagnosis and return its ID."""
    resp = await client.post(
        "/api/v1/diagnoses",
        json={
            "event_id": event_id,
            "category": "schema_mismatch",
            "confidence": 0.92,
            "summary": "Column 'amount' type changed from DECIMAL to STRING",
            "agent_name": "pipeline_recovery_agent",
        },
    )
    return resp.json()["id"]


async def test_full_workflow(client: AsyncClient) -> None:
    """End-to-end: event -> diagnosis -> action -> approve."""
    # 1. Ingest event
    event_id = await _create_event(client)

    # 2. Create diagnosis
    diagnosis_id = await _create_diagnosis(client, event_id)

    # Verify diagnosis
    resp = await client.get(f"/api/v1/diagnoses/{diagnosis_id}")
    assert resp.status_code == 200
    assert resp.json()["category"] == "schema_mismatch"

    # 3. Propose action
    resp = await client.post(
        "/api/v1/actions",
        json={
            "diagnosis_id": diagnosis_id,
            "action_type": "retry_pipeline",
            "description": "Retry with updated schema mapping",
        },
    )
    assert resp.status_code == 201
    action_id = resp.json()["id"]
    assert resp.json()["status"] == "proposed"

    # 4. Approve action
    resp = await client.patch(
        f"/api/v1/actions/{action_id}",
        json={
            "status": "approved",
            "approved_by": "engineer@company.com",
        },
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "approved"
    assert resp.json()["approved_by"] == "engineer@company.com"


async def test_list_event_diagnoses(client: AsyncClient) -> None:
    """Should list all diagnoses for an event."""
    event_id = await _create_event(client)
    await _create_diagnosis(client, event_id)

    resp = await client.get(f"/api/v1/events/{event_id}/diagnoses")
    assert resp.status_code == 200
    assert len(resp.json()) == 1


async def test_list_diagnosis_actions(client: AsyncClient) -> None:
    """Should list all actions for a diagnosis."""
    event_id = await _create_event(client)
    diagnosis_id = await _create_diagnosis(client, event_id)

    await client.post(
        "/api/v1/actions",
        json={
            "diagnosis_id": diagnosis_id,
            "action_type": "notify_owner",
            "description": "Alert pipeline owner about schema change",
        },
    )

    resp = await client.get(f"/api/v1/diagnoses/{diagnosis_id}/actions")
    assert resp.status_code == 200
    assert len(resp.json()) == 1
