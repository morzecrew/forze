"""Redis counter against a live server — the shared conformance battery.

# covers: CounterPort.incr
# covers: CounterPort.incr_batch
# covers: CounterPort.decr
# covers: CounterPort.reset
"""

from __future__ import annotations

from uuid import uuid4

import pytest
import pytest_asyncio

from forze_redis.adapters.counter import RedisCounterAdapter
from tests.support.counter_conformance import COUNTER_BATTERY, Check, CounterHarness

# ----------------------- #


@pytest_asyncio.fixture
async def harness(redis_client) -> CounterHarness:
    run = uuid4().hex[:8]

    return CounterHarness(
        counter=RedisCounterAdapter(client=redis_client, namespace=f"conf{run}"),
        suffix=lambda name: f"{name}-{run}",
    )


@pytest.mark.parametrize("check", COUNTER_BATTERY, ids=lambda check: check.__name__)
async def test_counter_battery(check: Check, harness: CounterHarness) -> None:
    await check(harness)
