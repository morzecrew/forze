"""Mongo counter against a live server — the shared conformance battery.

# covers: CounterPort.incr
# covers: CounterPort.incr_batch
# covers: CounterPort.decr
# covers: CounterPort.reset
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
import pytest_asyncio

pytest.importorskip("pymongo")

from forze.application.contracts.tenancy import TenantIdentity
from forze_mongo.adapters.counter import MongoCounterAdapter
from forze_mongo.execution.deps.configs import MongoCounterConfig
from tests.support.counter_conformance import COUNTER_BATTERY, Check, CounterHarness

# ----------------------- #


@pytest_asyncio.fixture
async def harness(mongo_client) -> CounterHarness:
    db_name = (await mongo_client.db()).name
    run = uuid4().hex[:8]
    config = MongoCounterConfig(collection=(db_name, f"counter_conf_{run}"))

    def _for_tenant(tenant: UUID) -> MongoCounterAdapter:
        # One collection, two tenants — the tenant must reach the document id.
        return MongoCounterAdapter(
            client=mongo_client,
            config=config,
            route="conformance",
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=tenant),
        )

    return CounterHarness(
        counter=MongoCounterAdapter(client=mongo_client, config=config, route="conformance"),
        suffix=lambda name: f"{name}-{run}",
        for_tenant=_for_tenant,
    )


@pytest.mark.conformance(plane="counter", engine="mongo")
@pytest.mark.parametrize("check", COUNTER_BATTERY, ids=lambda check: check.__name__)
async def test_counter_battery(check: Check, harness: CounterHarness) -> None:
    await check(harness)
