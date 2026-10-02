"""A principal's tenants are read in one batch, and listed exactly as the row-by-row reads did.

The listing used to read each tenant on its own, one read per membership. The row-by-row listing
is the oracle (``tests.support.tenant_memberships``): on random memberships — inactive tenants,
a tenant joined twice, a membership naming a tenant that does not exist — both must return the
same tenants in the same order, or fail with the same error.
"""

from __future__ import annotations

import random
from typing import Any
from uuid import uuid4

import pytest

from forze.base.exceptions import CoreException
from forze.testing import context_from_modules
from forze_mock import MockDepsModule
from tests.support.counting_ports import CountingQuery
from tests.support.tenant_memberships import (
    create_tenant,
    join,
    list_both_ways,
    management,
    oracle_principal_tenants,
    wide_memberships,
)

pytestmark = pytest.mark.unit


async def _outcome(call: Any) -> Any:
    try:
        return await call
    except CoreException as error:
        return ("raised", error.kind, error.code)


class TestTheBatchedListingAnswersAsTheRowByRowOne:
    async def test_on_random_memberships(self) -> None:
        listed = raised = repeated = inactive = 0

        for seed in range(300):
            rnd = random.Random(seed)
            ctx = context_from_modules(MockDepsModule())
            principal_id = uuid4()
            tenants = [
                await create_tenant(ctx, f"tenant-{i}", active=rnd.random() < 0.7)
                for i in range(rnd.randint(0, 40))
            ]

            joined: list[Any] = []

            for who in (principal_id, uuid4()):
                for tenant in rnd.sample(tenants, rnd.randint(0, len(tenants))):
                    await join(ctx, who, tenant.id)
                    joined += [tenant] if who == principal_id else []

                    # Now and then joined twice: the listing repeats it.
                    if rnd.random() < 0.05:
                        await join(ctx, who, tenant.id)

                if rnd.random() < 0.1:
                    await join(ctx, who, uuid4())

            adapter = management(ctx)
            got = await _outcome(adapter.list_principal_tenants(principal_id))
            want = await _outcome(oracle_principal_tenants(adapter, principal_id))

            assert got == want, f"seed {seed}"

            if isinstance(got, tuple):
                raised += 1
            else:
                listed += 1
                repeated += len(got) != len(set(got))
                inactive += any(not t.is_active for t in joined)

        # Every leg ran: listings that succeed, repeat a tenant, leave inactive ones out, and fail.
        assert listed > 150 and raised > 30 and repeated > 20 and inactive > 100, (
            listed,
            raised,
            repeated,
            inactive,
        )

    async def test_past_one_batch_with_inactive_and_repeated_tenants(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        principal_id, active = await wide_memberships(ctx)

        listed = await list_both_ways(ctx, principal_id)

        assert sorted(t.tenant_key for t in listed) == sorted([*active, "tenant-34"])


class TestTheReadsDoNotGrowWithTheMemberships:
    @pytest.mark.parametrize("memberships", [0, 1, 10, 29])
    async def test_one_scan_and_one_batch(self, memberships: int) -> None:
        ctx = context_from_modules(MockDepsModule())
        principal_id = uuid4()

        for i in range(memberships):
            await join(ctx, principal_id, (await create_tenant(ctx, f"tenant-{i}")).id)

        reads: list[str] = []
        adapter = management(ctx, wrap=lambda port, name: CountingQuery(port, reads, name))

        assert len(await adapter.list_principal_tenants(principal_id)) == memberships
        assert reads == ["binding.scan"] + (["tenant.get_many"] if memberships else [])

    async def test_the_row_by_row_listing_read_once_per_membership(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        principal_id = uuid4()

        for i in range(10):
            await join(ctx, principal_id, (await create_tenant(ctx, f"tenant-{i}")).id)

        reads: list[str] = []
        adapter = management(ctx, wrap=lambda port, name: CountingQuery(port, reads, name))
        await oracle_principal_tenants(adapter, principal_id)

        assert len(reads) == 11
