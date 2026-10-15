"""Redis distributed lock against the shared conformance battery.

The leg where the numbers are real: the advance runs as a Lua script that compares in
doubles and formats large numbers with an exponent, so exactness and the bound on ``past``
are properties of the wire here, not of a Python dict.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.dlock import DistributedLockSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze_redis.adapters import RedisDistributedLockAdapter
from forze_redis.kernel.client import RedisClient
from tests.support.dlock_conformance import DLOCK_BATTERY, Check, DlockHarness

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #

_SPEC = DistributedLockSpec(name="conformance", ttl=timedelta(seconds=60))


@pytest.fixture
def harness(redis_client: RedisClient) -> DlockHarness:
    namespace = f"it:dlock-conf:{uuid4().hex[:12]}"

    def _for_tenant(tenant: UUID) -> RedisDistributedLockAdapter:
        return RedisDistributedLockAdapter(
            client=redis_client,
            namespace=namespace,
            spec=_SPEC,
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=tenant),
        )

    return DlockHarness(
        lock=RedisDistributedLockAdapter(client=redis_client, namespace=namespace, spec=_SPEC),
        for_tenant=_for_tenant,
    )


@pytest.mark.conformance(plane="dlock", engine="redis")
@pytest.mark.parametrize("check", DLOCK_BATTERY, ids=lambda check: check.__name__)
async def test_dlock_battery(check: Check, harness: DlockHarness) -> None:
    await check(harness)
