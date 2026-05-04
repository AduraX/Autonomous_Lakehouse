"""Tests for the Cost Optimization API endpoints."""

from httpx import AsyncClient


async def _ingest_events(client: AsyncClient, count: int = 8) -> list[int]:
    """Ingest multiple events to build baselines."""
    ids = []
    for i in range(count):
        resp = await client.post(
            "/api/v1/events",
            json={
                "external_run_id": f"cost-api-{i}",
                "pipeline_name": "etl_daily",
                "platform": "mock",
                "status": "succeeded",
                "duration_seconds": 100.0 + i * 5,
            },
        )
        assert resp.status_code == 201
        ids.append(resp.json()["id"])
    return ids


async def test_analyze_builds_baseline(client: AsyncClient) -> None:
    """POST /api/v1/cost/analyze should update baseline and return analysis."""
    ids = await _ingest_events(client, count=6)

    # Analyze the last event
    resp = await client.post("/api/v1/cost/analyze", json={"event_id": ids[-1]})
    assert resp.status_code == 200

    data = resp.json()
    assert data["event_id"] == ids[-1]
    assert data["pipeline_name"] == "etl_daily"
    assert "findings" in data
    assert "analyzed_at" in data


async def test_get_baseline(client: AsyncClient) -> None:
    """GET /api/v1/cost/baselines/{name} should return baseline stats."""
    ids = await _ingest_events(client)

    # Analyze all to build baseline
    for eid in ids:
        await client.post("/api/v1/cost/analyze", json={"event_id": eid})

    resp = await client.get("/api/v1/cost/baselines/etl_daily")
    assert resp.status_code == 200

    data = resp.json()
    assert data["pipeline_name"] == "etl_daily"
    assert data["total_runs"] == 8
    assert data["avg_duration"] > 0
    assert "failure_rate" in data
    assert "avg_cost_per_run" in data


async def test_list_baselines(client: AsyncClient) -> None:
    """GET /api/v1/cost/baselines should return all baselines."""
    ids = await _ingest_events(client, count=3)
    for eid in ids:
        await client.post("/api/v1/cost/analyze", json={"event_id": eid})

    resp = await client.get("/api/v1/cost/baselines")
    assert resp.status_code == 200
    assert len(resp.json()) >= 1


async def test_cost_summary(client: AsyncClient) -> None:
    """GET /api/v1/cost/summary should return aggregated cost stats."""
    ids = await _ingest_events(client, count=3)
    for eid in ids:
        await client.post("/api/v1/cost/analyze", json={"event_id": eid})

    resp = await client.get("/api/v1/cost/summary")
    assert resp.status_code == 200

    data = resp.json()
    assert data["total_pipelines"] >= 1
    assert data["total_runs"] >= 3
    assert "total_duration_hours" in data
    assert "top_cost_pipelines" in data
    assert "high_failure_pipelines" in data
    assert "anomaly_pipelines" in data


async def test_get_baseline_not_found(client: AsyncClient) -> None:
    """Should return 404 for unknown pipeline."""
    resp = await client.get("/api/v1/cost/baselines/nonexistent_pipeline")
    assert resp.status_code == 404


async def test_analyze_nonexistent_event(client: AsyncClient) -> None:
    """Should return 404 for unknown event."""
    resp = await client.post("/api/v1/cost/analyze", json={"event_id": 99999})
    assert resp.status_code == 404
