"""Connection pool gauges against a real psycopg pool (issue #843).

One connection (``max_size=1``), so the pool's state is fully determined by what the test
holds: one caller in a transaction owns the connection, and a second caller has to queue.
The numbers are read through ``instrument_connection_pools`` and an in-memory OTel reader,
the same path a scrape takes.
"""

import asyncio
from collections.abc import Callable
from datetime import timedelta
from typing import Any

import pytest

pytest.importorskip("psycopg")

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from forze.application.contracts.pools import ConnectionPoolStats
from forze.application.execution import instrument_connection_pools
from forze.application.execution.observability import (
    CONNECTION_POOL_IDLE_GAUGE,
    CONNECTION_POOL_IN_USE_GAUGE,
    CONNECTION_POOL_MAX_GAUGE,
    CONNECTION_POOL_WAITING_GAUGE,
)
from forze_postgres.kernel.client.client import PostgresClient, PostgresConfig

# ----------------------- #


def _gauges(reader: InMemoryMetricReader) -> dict[str, Any]:
    """The ``postgres`` client's value per pool gauge, from one collection."""

    data = reader.get_metrics_data()
    out: dict[str, Any] = {}

    if data is None:  # no gauge observed anything
        return out

    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                for dp in metric.data.data_points:
                    assert dict(dp.attributes) == {"forze.client": "postgres"}
                    out[metric.name] = dp.value

    return out


async def _until(client: PostgresClient, done: Callable[[ConnectionPoolStats], bool]) -> None:
    async with asyncio.timeout(10):
        while True:
            stats = client.connection_pool_stats()

            if stats is not None and done(stats):
                return

            await asyncio.sleep(0.02)


# ----------------------- #


@pytest.mark.asyncio
async def test_gauges_follow_a_held_connection_and_a_queued_caller(postgres_container) -> None:
    url = postgres_container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
    client = PostgresClient()
    await client.initialize(
        dsn=url,
        config=PostgresConfig(min_size=1, max_size=1),
        acquire_timeout=timedelta(seconds=10),
    )

    reader = InMemoryMetricReader()
    instrument_connection_pools(
        {"postgres": client},
        meter=MeterProvider(metric_readers=[reader]).get_meter("test"),
    )

    holding = asyncio.Event()
    release = asyncio.Event()
    tasks: list[asyncio.Future[Any]] = []

    async def hold() -> None:
        async with client.transaction():
            await client.fetch_value("SELECT 1")
            holding.set()
            await release.wait()

    try:
        await _until(client, lambda s: s.idle == 1)

        assert _gauges(reader) == {
            CONNECTION_POOL_IN_USE_GAUGE: 0,
            CONNECTION_POOL_IDLE_GAUGE: 1,
            CONNECTION_POOL_MAX_GAUGE: 1,
            CONNECTION_POOL_WAITING_GAUGE: 0,
        }

        holder = asyncio.ensure_future(hold())
        signalled = asyncio.ensure_future(holding.wait())
        tasks += [holder, signalled]
        # A hold() that fails before it holds never sets the event: surface its error.
        await asyncio.wait([holder, signalled], return_when=asyncio.FIRST_COMPLETED)

        if holder.done():
            holder.result()

        held = _gauges(reader)
        assert held[CONNECTION_POOL_IN_USE_GAUGE] == 1
        assert held[CONNECTION_POOL_IDLE_GAUGE] == 0
        assert held[CONNECTION_POOL_WAITING_GAUGE] == 0

        waiter = asyncio.ensure_future(client.fetch_value("SELECT 2"))
        tasks.append(waiter)
        await _until(client, lambda s: s.waiting == 1)

        contended = _gauges(reader)
        assert contended[CONNECTION_POOL_IN_USE_GAUGE] == 1
        assert contended[CONNECTION_POOL_WAITING_GAUGE] == 1

        release.set()
        await holder
        assert await waiter == 2

        await _until(client, lambda s: s.in_use == 0)

        drained = _gauges(reader)
        assert drained[CONNECTION_POOL_IN_USE_GAUGE] == 0
        assert drained[CONNECTION_POOL_IDLE_GAUGE] == 1
        assert drained[CONNECTION_POOL_WAITING_GAUGE] == 0

    finally:
        release.set()

        # Retrieve both outcomes so a failed assertion above is not buried under
        # "exception was never retrieved" warnings from a task left running.
        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        await client.close()

    # A closed client keeps its label registered and reports nothing.
    assert _gauges(reader) == {}
