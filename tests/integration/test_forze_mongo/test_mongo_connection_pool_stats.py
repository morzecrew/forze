"""Connection pool gauges against a real pymongo pool (issue #843).

No minimum pool, so every connection is one an operation opened. A ``$where`` that sleeps
on the server holds its connection for as long as it runs, so the pool's state is fixed
by what the test runs. The numbers are read through ``instrument_connection_pools`` and an
in-memory OTel reader, the same path a scrape takes.
"""

import asyncio
from collections.abc import Callable
from typing import Any
from uuid import uuid4

import pytest

pytest.importorskip("pymongo")

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from testcontainers.mongodb import MongoDbContainer

from forze.application.contracts.pools import ConnectionPoolStats
from forze.application.execution import instrument_connection_pools
from forze.application.execution.observability import (
    CONNECTION_POOL_IDLE_GAUGE,
    CONNECTION_POOL_IN_USE_GAUGE,
    CONNECTION_POOL_MAX_GAUGE,
)
from forze_mongo.kernel.client import MongoClient, MongoConfig

# ----------------------- #


def _gauges(reader: InMemoryMetricReader) -> dict[str, Any]:
    """The ``mongo`` client's value per pool gauge, from one collection."""

    data = reader.get_metrics_data()
    out: dict[str, Any] = {}

    if data is None:  # no gauge observed anything
        return out

    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                for dp in metric.data.data_points:
                    assert dict(dp.attributes) == {"forze.client": "mongo"}
                    out[metric.name] = dp.value

    return out


async def _until(client: MongoClient, done: Callable[[ConnectionPoolStats], bool]) -> None:
    async with asyncio.timeout(10):
        while True:
            stats = client.connection_pool_stats()

            if stats is not None and done(stats):
                return

            await asyncio.sleep(0.02)


# ----------------------- #


@pytest.mark.asyncio
async def test_gauges_follow_a_held_connection(mongo_container: MongoDbContainer) -> None:
    client = MongoClient()
    await client.initialize(
        mongo_container.get_connection_url(),
        db_name=f"forze_test_{uuid4().hex[:8]}",
        config=MongoConfig(min_pool_size=0, max_pool_size=2),
    )

    reader = InMemoryMetricReader()
    instrument_connection_pools(
        {"mongo": client},
        meter=MeterProvider(metric_readers=[reader]).get_meter("test"),
    )

    slow: asyncio.Future[Any] | None = None

    try:
        _ok, healthy = await client.health()
        assert healthy

        # The ping opened one connection and gave it back.
        assert _gauges(reader) == {
            CONNECTION_POOL_IN_USE_GAUGE: 0,
            CONNECTION_POOL_IDLE_GAUGE: 1,
            CONNECTION_POOL_MAX_GAUGE: 2,
        }

        coll = await client.collection("pool_stats")
        await coll.insert_one({"_id": 1})

        slow = asyncio.ensure_future(coll.find_one({"$where": "sleep(1500) || true"}))
        await _until(client, lambda s: s.in_use == 1)

        assert _gauges(reader) == {
            CONNECTION_POOL_IN_USE_GAUGE: 1,
            CONNECTION_POOL_IDLE_GAUGE: 0,
            CONNECTION_POOL_MAX_GAUGE: 2,
        }

        assert await slow == {"_id": 1}

        await _until(client, lambda s: s.in_use == 0)

        assert _gauges(reader) == {
            CONNECTION_POOL_IN_USE_GAUGE: 0,
            CONNECTION_POOL_IDLE_GAUGE: 1,
            CONNECTION_POOL_MAX_GAUGE: 2,
        }

    finally:
        if slow is not None:
            slow.cancel()
            await asyncio.gather(slow, return_exceptions=True)

        await client.close()

    # A closed client keeps its label registered and reports nothing.
    assert _gauges(reader) == {}
