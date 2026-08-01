"""Mock counter — the shared conformance battery.

The mock runs the same battery the real backends do, which is the whole point: it used to
count with unbounded Python integers and accept allocations no store can hold, so anything
proven against it about a counter's domain was proven against a fiction.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from forze.application.contracts.tenancy import TenantIdentity
from forze_mock.adapters.counter import MockCounterAdapter
from forze_mock.state import MockState
from tests.support.counter_conformance import COUNTER_BATTERY, Check, CounterHarness

# ----------------------- #


@pytest.fixture
def harness() -> CounterHarness:
    run = uuid4().hex[:8]
    # One shared MockState across both tenants, so the partition check is asked the same
    # question the real backends are: two tenants over ONE store.
    state = MockState()

    def _for_tenant(tenant: UUID) -> MockCounterAdapter:
        return MockCounterAdapter(
            state=state,
            namespace="conformance",
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=tenant),
        )

    return CounterHarness(
        counter=MockCounterAdapter(state=state, namespace="conformance"),
        suffix=lambda name: f"{name}-{run}",
        for_tenant=_for_tenant,
    )


@pytest.mark.conformance(plane="counter", engine="mock")
@pytest.mark.parametrize("check", COUNTER_BATTERY, ids=lambda check: check.__name__)
async def test_counter_battery(check: Check, harness: CounterHarness) -> None:
    await check(harness)
