"""Tests for Pipeline Event API endpoints."""

from httpx import AsyncClient


async def test_ingest_event(client: AsyncClient) -> None:
    """Should create a new pipeline event and return 201."""
    payload = {
        "external_run_id": "adf-run-test-001",
        "pipeline_name": "ingest_sales_data",
        "platform": "adf",
        "status": "failed",
        "error_message": "Schema mismatch on column 'amount'",
        "error_code": "SCHEMA_ERROR_4012",
        "duration_seconds": 142.5,
    }
    response = await client.post("/api/v1/events", json=payload)
    assert response.status_code == 201

    data = response.json()
    assert data["external_run_id"] == "adf-run-test-001"
    assert data["pipeline_name"] == "ingest_sales_data"
    assert data["status"] == "failed"
    assert data["id"] is not None


async def test_get_event(client: AsyncClient) -> None:
    """Should retrieve a previously ingested event."""
    # Create
    payload = {
        "external_run_id": "adf-run-test-002",
        "pipeline_name": "transform_daily_revenue",
        "platform": "mock",
        "status": "succeeded",
    }
    create_resp = await client.post("/api/v1/events", json=payload)
    event_id = create_resp.json()["id"]

    # Retrieve
    response = await client.get(f"/api/v1/events/{event_id}")
    assert response.status_code == 200
    assert response.json()["pipeline_name"] == "transform_daily_revenue"


async def test_get_event_not_found(client: AsyncClient) -> None:
    """Should return 404 for non-existent event."""
    response = await client.get("/api/v1/events/99999")
    assert response.status_code == 404


async def test_list_events_pagination(client: AsyncClient) -> None:
    """Should return paginated event list."""
    # Ingest 3 events
    for i in range(3):
        await client.post(
            "/api/v1/events",
            json={
                "external_run_id": f"run-{i}",
                "pipeline_name": f"pipeline_{i}",
                "platform": "mock",
                "status": "succeeded",
            },
        )

    response = await client.get("/api/v1/events", params={"page": 1, "page_size": 2})
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 3
    assert len(data["items"]) == 2
    assert data["page"] == 1
    assert data["page_size"] == 2


async def test_list_events_filter_by_status(client: AsyncClient) -> None:
    """Should filter events by status."""
    await client.post(
        "/api/v1/events",
        json={
            "external_run_id": "run-ok",
            "pipeline_name": "p1",
            "platform": "mock",
            "status": "succeeded",
        },
    )
    await client.post(
        "/api/v1/events",
        json={
            "external_run_id": "run-fail",
            "pipeline_name": "p2",
            "platform": "mock",
            "status": "failed",
        },
    )

    response = await client.get("/api/v1/events", params={"status": "failed"})
    data = response.json()
    assert data["total"] == 1
    assert data["items"][0]["status"] == "failed"
