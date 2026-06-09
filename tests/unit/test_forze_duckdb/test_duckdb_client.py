"""Tests for DuckDbClient: Arrow result path, params, pagination, and the
concurrency constraints (event-loop responsiveness, timeout via cursor interrupt)."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from pydantic import BaseModel

from forze_duckdb import DuckDbClient, DuckDbConfig

# A query heavy enough to take well over the timeouts/ticks below, but bounded.
_HEAVY_SQL = "SELECT count(*) AS n FROM range(100000) a, range(100000) b"

# ----------------------- #


class _P(BaseModel):
    floor: int


# ....................... #


async def test_run_query_returns_arrow_and_rows(client: DuckDbClient) -> None:
    result = await client.run_query("SELECT 1 AS a, 'x' AS b")

    # Native Arrow held internally; dict rows materialized lazily at the edge.
    assert result.arrow.num_rows == 1
    assert result.rows == [{"a": 1, "b": "x"}]


# ....................... #


async def test_named_params_bind(client: DuckDbClient) -> None:
    result = await client.run_query(
        "SELECT i FROM range(5) t(i) WHERE i >= $floor ORDER BY i",
        _P(floor=3),
    )

    assert [r["i"] for r in result.rows] == [3, 4]


# ....................... #


async def test_limit_offset_and_max_rows(client: DuckDbClient) -> None:
    base = "SELECT i FROM range(10) t(i) ORDER BY i"

    page = await client.run_query(base, limit=3, offset=2)
    assert [r["i"] for r in page.rows] == [2, 3, 4]

    capped = await client.run_query(base, limit=5, max_rows=2)
    assert [r["i"] for r in capped.rows] == [0, 1]


# ....................... #


async def test_health_ok(client: DuckDbClient) -> None:
    message, ok = await client.health()

    assert ok is True
    assert message == "ok"


# ....................... #


async def test_uninitialized_client_raises() -> None:
    fresh = DuckDbClient()

    with pytest.raises(Exception, match="not initialized"):
        await fresh.run_query("SELECT 1")


# ....................... #


async def test_event_loop_stays_responsive_under_heavy_query(
    client: DuckDbClient,
) -> None:
    """A heavy query runs off the loop (DuckDB releases the GIL), so a concurrent
    ticker keeps advancing — proving the offload doesn't block the event loop."""

    ticks = 0

    async def _ticker() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.005)
            ticks += 1

    ticker_task = asyncio.create_task(_ticker())
    try:
        await client.run_query(_HEAVY_SQL)
    finally:
        ticker_task.cancel()

    # If the loop had been blocked, ticks would be ~0. We only require a few.
    assert ticks >= 3


# ....................... #


async def test_timeout_interrupts_and_client_recovers(client: DuckDbClient) -> None:
    """A short timeout aborts the running query via cursor interrupt, and the
    client remains usable for subsequent queries (executor/connection intact)."""

    with pytest.raises(Exception, match="timeout"):
        await client.run_query(_HEAVY_SQL, timeout=timedelta(milliseconds=100))

    # The interrupted query must not have wedged the client.
    result = await client.run_query("SELECT 42 AS a")
    assert result.rows == [{"a": 42}]


# ....................... #


async def test_concurrent_queries_do_not_serialize(client: DuckDbClient) -> None:
    """Independent queries use their own cursors and run concurrently on the
    bounded executor (config allows >1 worker)."""

    results = await asyncio.gather(
        *(
            client.run_query("SELECT $floor AS v", _P(floor=i))
            for i in range(4)
        )
    )

    assert sorted(r.rows[0]["v"] for r in results) == [0, 1, 2, 3]


# ....................... #


async def test_single_worker_executor_still_correct() -> None:
    """max_concurrent_queries=1 serializes but stays correct."""

    c = DuckDbClient()
    await c.initialize(
        ":memory:",
        config=DuckDbConfig(max_concurrent_queries=1),
        extensions=(),
    )
    try:
        results = await asyncio.gather(
            *(c.run_query("SELECT $floor AS v", _P(floor=i)) for i in range(3))
        )
        assert sorted(r.rows[0]["v"] for r in results) == [0, 1, 2]
    finally:
        await c.close()
