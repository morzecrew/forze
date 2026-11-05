"""Connection pool gauges against a real redis-py pool (issue #843).

A blocking ``XREAD`` holds one connection for as long as it waits, so the pool's state is
fixed by what the test holds; the ``XADD`` that wakes it takes a second connection. The
numbers are read through ``instrument_connection_pools`` and an in-memory OTel reader, the
same path a scrape takes.
"""

import asyncio
from collections.abc import Callable
from typing import Any
from uuid import uuid4

import pytest

pytest.importorskip("redis")

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from testcontainers.redis import RedisContainer

from forze.application.contracts.pools import ConnectionPoolStats
from forze.application.execution import instrument_connection_pools
from forze.application.execution.observability import (
    CONNECTION_POOL_IDLE_GAUGE,
    CONNECTION_POOL_IN_USE_GAUGE,
    CONNECTION_POOL_MAX_GAUGE,
)
from forze_redis.kernel.client import RedisClient, RedisConfig

# ----------------------- #


def _gauges(reader: InMemoryMetricReader) -> dict[str, Any]:
    """The ``redis`` client's value per pool gauge, from one collection."""

    data = reader.get_metrics_data()
    out: dict[str, Any] = {}

    if data is None:  # no gauge observed anything
        return out

    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                for dp in metric.data.data_points:
                    assert dict(dp.attributes) == {"forze.client": "redis"}
                    out[metric.name] = dp.value

    return out


async def _until(client: RedisClient, done: Callable[[ConnectionPoolStats], bool]) -> None:
    async with asyncio.timeout(10):
        while True:
            stats = client.connection_pool_stats()

            if stats is not None and done(stats):
                return

            await asyncio.sleep(0.02)


# ----------------------- #


@pytest.mark.asyncio
async def test_gauges_follow_a_held_connection(redis_container: RedisContainer) -> None:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    client = RedisClient()
    await client.initialize(dsn=f"redis://{host}:{port}/0", config=RedisConfig(max_size=2))

    reader = InMemoryMetricReader()
    instrument_connection_pools(
        {"redis": client},
        meter=MeterProvider(metric_readers=[reader]).get_meter("test"),
    )

    stream = f"it:pool:{uuid4().hex[:12]}"
    reading: asyncio.Future[Any] | None = None

    try:
        # The ping in initialize opened one connection and gave it back.
        assert _gauges(reader) == {
            CONNECTION_POOL_IN_USE_GAUGE: 0,
            CONNECTION_POOL_IDLE_GAUGE: 1,
            CONNECTION_POOL_MAX_GAUGE: 2,
        }

        reading = asyncio.ensure_future(client.xread({stream: "$"}, block_ms=10_000))
        await _until(client, lambda s: s.in_use == 1)

        assert _gauges(reader) == {
            CONNECTION_POOL_IN_USE_GAUGE: 1,
            CONNECTION_POOL_IDLE_GAUGE: 0,
            CONNECTION_POOL_MAX_GAUGE: 2,
        }

        await client.xadd(stream, {"k": "v"})
        assert await reading

        await _until(client, lambda s: s.in_use == 0)

        assert _gauges(reader) == {
            CONNECTION_POOL_IN_USE_GAUGE: 0,
            CONNECTION_POOL_IDLE_GAUGE: 2,
            CONNECTION_POOL_MAX_GAUGE: 2,
        }

    finally:
        if reading is not None:
            reading.cancel()
            await asyncio.gather(reading, return_exceptions=True)

        await client.close()

    # A closed client keeps its label registered and reports nothing.
    assert _gauges(reader) == {}
