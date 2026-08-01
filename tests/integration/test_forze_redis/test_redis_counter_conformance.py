"""Redis counter against a live server — the shared conformance battery.

# covers: CounterPort.incr
# covers: CounterPort.incr_batch
# covers: CounterPort.decr
# covers: CounterPort.reset
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
import pytest_asyncio

from forze.application.contracts.tenancy import TenantIdentity
from forze_redis.adapters.counter import RedisCounterAdapter
from tests.support.counter_conformance import COUNTER_BATTERY, Check, CounterHarness

# ----------------------- #


@pytest_asyncio.fixture
async def harness(redis_client) -> CounterHarness:
    run = uuid4().hex[:8]
    namespace = f"conf{run}"

    def _for_tenant(tenant: UUID) -> RedisCounterAdapter:
        # One key namespace, two tenants — the tenant must reach the key itself.
        return RedisCounterAdapter(
            client=redis_client,
            namespace=namespace,
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=tenant),
        )

    return CounterHarness(
        counter=RedisCounterAdapter(client=redis_client, namespace=namespace),
        suffix=lambda name: f"{name}-{run}",
        for_tenant=_for_tenant,
    )


@pytest.mark.conformance(plane="counter", engine="redis")
@pytest.mark.parametrize("check", COUNTER_BATTERY, ids=lambda check: check.__name__)
async def test_counter_battery(check: Check, harness: CounterHarness) -> None:
    await check(harness)
