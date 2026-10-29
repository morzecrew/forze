"""Mock counter — the shared conformance battery.

The mock runs the same battery the real backends do, which is the whole point: it used to
count with unbounded Python integers and accept allocations no store can hold, so anything
proven against it about a counter's domain was proven against a fiction.

The leg binds a frozen clock and advances it for the lifetime checks, which proves the mock's
expiry runs on the bound time source — the seam a simulation drives it through.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.tenancy import TenantIdentity
from forze.base.primitives import FrozenTimeSource, bind_time_source
from forze_mock.adapters.counter import MockCounterAdapter, MockCounterAdminAdapter
from forze_mock.state import MockState
from tests.support.counter_conformance import COUNTER_BATTERY, Check, CounterHarness

# ----------------------- #


@pytest.fixture
def clock() -> FrozenTimeSource:
    return FrozenTimeSource(datetime(2026, 1, 1, tzinfo=UTC))


@pytest.fixture
def harness(clock: FrozenTimeSource) -> CounterHarness:
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

    async def _advance(delta: timedelta) -> None:
        clock.instant += delta

    return CounterHarness(
        counter=MockCounterAdapter(state=state, namespace="conformance"),
        suffix=lambda name: f"{name}-{run}",
        admin=MockCounterAdminAdapter(state=state, namespace="conformance"),
        wait=_advance,
        for_tenant=_for_tenant,
    )


@pytest.mark.conformance(plane="counter", engine="mock")
@pytest.mark.parametrize("check", COUNTER_BATTERY, ids=lambda check: check.__name__)
async def test_counter_battery(
    check: Check,
    harness: CounterHarness,
    clock: FrozenTimeSource,
) -> None:
    with bind_time_source(clock):
        await check(harness)
