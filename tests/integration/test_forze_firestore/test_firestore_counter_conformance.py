"""Firestore counter against the emulator — the shared conformance battery.

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
from forze_firestore.adapters import FirestoreCounterAdapter, FirestoreCounterAdminAdapter
from forze_firestore.execution.deps.configs import FirestoreCounterConfig
from forze_firestore.kernel.client import FirestoreClient
from tests.support.counter_conformance import COUNTER_BATTERY, Check, CounterHarness

# ----------------------- #


@pytest_asyncio.fixture(scope="function")
async def harness(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> CounterHarness:
    run = uuid4().hex[:8]
    config = FirestoreCounterConfig(collection=("(default)", unique_collection))

    def _for_tenant(tenant: UUID) -> FirestoreCounterAdapter:
        # One collection, two tenants — the tenant must reach the document id.
        return FirestoreCounterAdapter(
            client=firestore_client,
            config=config,
            route="conformance",
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=tenant),
        )

    async def _legacy(suffix: str, value: int) -> None:
        # The pre-route document id: no route tag, ``s:`` + the suffix, no tenant.
        coll = await firestore_client.collection(unique_collection, database="(default)")
        await firestore_client.set_document(coll, f"s:{suffix}", {"value": value})

    return CounterHarness(
        counter=FirestoreCounterAdapter(
            client=firestore_client,
            config=config,
            route="conformance",
        ),
        suffix=lambda name: f"{name}-{run}",
        admin=FirestoreCounterAdminAdapter(
            client=firestore_client,
            config=config,
            route="conformance",
        ),
        legacy=_legacy,
        for_tenant=_for_tenant,
    )


@pytest.mark.conformance(plane="counter", engine="firestore")
@pytest.mark.parametrize("check", COUNTER_BATTERY, ids=lambda check: check.__name__)
async def test_counter_battery(check: Check, harness: CounterHarness) -> None:
    await check(harness)
