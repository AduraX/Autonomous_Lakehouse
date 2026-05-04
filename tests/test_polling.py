"""Tests for the event polling endpoint (POST /api/v1/poll)."""

from httpx import AsyncClient


async def test_poll_ingests_events(client: AsyncClient) -> None:
    """Should ingest mock events and return counts."""
    resp = await client.post("/api/v1/poll", params={"limit": 5})
    assert resp.status_code == 200

    data = resp.json()
    assert data["ingested"] > 0
    assert "polled_at" in data


async def test_poll_coordinates_failures(client: AsyncClient) -> None:
    """Should coordinate failed events and return results."""
    resp = await client.post("/api/v1/poll", params={"limit": 10})
    assert resp.status_code == 200

    data = resp.json()
    # Mock adapter generates ~30% failures, so with 10 events we should get some
    if data["coordinated"] > 0:
        assert len(data["coordination_results"]) == data["coordinated"]
        for cr in data["coordination_results"]:
            assert "event_id" in cr
            assert "pipeline" in cr
            assert "category" in cr
            assert "actions" in cr


async def test_poll_events_appear_in_list(client: AsyncClient) -> None:
    """Polled events should be retrievable via the events API."""
    # Poll
    await client.post("/api/v1/poll", params={"limit": 3})

    # Verify events exist
    resp = await client.get("/api/v1/events")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] >= 3
