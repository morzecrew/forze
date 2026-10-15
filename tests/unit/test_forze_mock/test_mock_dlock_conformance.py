"""Mock distributed lock — the shared conformance battery.

The mock is the lock unit tests and simulation resolve, so it has to keep the same fencing
counter the Redis lock does: an advance it accepted and Redis refused, or the reverse, would
make every test written against it prove the wrong thing.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID

import pytest

from forze.application.contracts.dlock import DistributedLockSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze_mock.adapters.dlock import MockDistributedLockAdapter
from forze_mock.state import MockState
from tests.support.dlock_conformance import DLOCK_BATTERY, Check, DlockHarness

# ----------------------- #

_SPEC = DistributedLockSpec(name="conformance", ttl=timedelta(seconds=60))


@pytest.fixture
def harness() -> DlockHarness:
    # One shared MockState across tenants, as the real backend is one Redis.
    state = MockState()

    def _for_tenant(tenant: UUID) -> MockDistributedLockAdapter:
        return MockDistributedLockAdapter(
            spec=_SPEC,
            state=state,
            namespace="conformance",
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=tenant),
        )

    return DlockHarness(
        lock=MockDistributedLockAdapter(spec=_SPEC, state=state, namespace="conformance"),
        for_tenant=_for_tenant,
    )


@pytest.mark.conformance(plane="dlock", engine="mock")
@pytest.mark.parametrize("check", DLOCK_BATTERY, ids=lambda check: check.__name__)
async def test_dlock_battery(check: Check, harness: DlockHarness) -> None:
    await check(harness)
