"""Durable tenancy on Mongo, at the tier where placement is observable.

The shared battery (`tests/support/durable_conformance`) drives the tenant *predicate* over
one static collection, which is the tagged tier. What it cannot reach is the namespace tier:
with a per-tenant collection resolver the collection is itself the isolation mechanism, so a
document written into the wrong one is unreachable rather than merely unfiltered — and both
readings of "which tenant?" put it in the same place when the collection is static. That is
the gap this file covers, and the Postgres mirror of it lives in
`test_pg_durable_tenancy_integration.py`.

# covers: DurableRunStorePort.enqueue
# covers: DurableScheduleStorePort.put
"""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.durable.function import DurableScheduleRecord
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow
from forze_mongo.adapters.durable import MongoDurableRunStore, MongoDurableScheduleStore
from forze_mongo.execution.deps.configs import (
    MongoDurableRunConfig,
    MongoDurableScheduleConfig,
)
from forze_mongo.kernel.client import MongoClient

# ----------------------- #

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
def tenants() -> tuple[UUID, UUID]:
    return uuid4(), uuid4()


def _run_store(
    client: MongoClient, db_name: str, suffix: str, tenant: UUID | None
) -> MongoDurableRunStore:
    """A run store over a *per-tenant* collection — the tier where placement is provable."""

    def collection(tenant_id: UUID | None) -> tuple[str, str]:
        assert tenant_id is not None
        return db_name, f"durable_run_{tenant_id.hex[:8]}_{suffix}"

    return MongoDurableRunStore(
        client=client,
        config=MongoDurableRunConfig(collection=collection),
        tenant_provider=(None if tenant is None else (lambda: TenantIdentity(tenant_id=tenant))),
    )


def _schedule_store(
    client: MongoClient, db_name: str, suffix: str, tenant: UUID | None
) -> MongoDurableScheduleStore:
    def collection(tenant_id: UUID | None) -> tuple[str, str]:
        assert tenant_id is not None
        return db_name, f"durable_schedule_{tenant_id.hex[:8]}_{suffix}"

    return MongoDurableScheduleStore(
        client=client,
        config=MongoDurableScheduleConfig(collection=collection),
        tenant_provider=(None if tenant is None else (lambda: TenantIdentity(tenant_id=tenant))),
    )


# ....................... #


class TestNamespacePlacement:
    async def test_a_run_enqueued_for_a_tenant_lands_in_that_tenants_collection(
        self, mongo_client: MongoClient, tenants: tuple[UUID, UUID]
    ) -> None:
        db_name = (await mongo_client.db()).name
        suffix = uuid4().hex[:8]
        tenant_a, _ = tenants

        run = await _run_store(mongo_client, db_name, suffix, None).enqueue(
            "for-a", input_json={"t": "a"}, tenant_id=tenant_a
        )

        reached = await _run_store(mongo_client, db_name, suffix, tenant_a).load(run.run_id)

        assert reached is not None
        assert reached.name == "for-a"
        assert reached.tenant_id == tenant_a

    async def test_a_schedule_put_for_a_tenant_lands_where_that_tenant_looks(
        self, mongo_client: MongoClient, tenants: tuple[UUID, UUID]
    ) -> None:
        db_name = (await mongo_client.db()).name
        suffix = uuid4().hex[:8]
        tenant_a, tenant_b = tenants

        await _schedule_store(mongo_client, db_name, suffix, None).put(
            DurableScheduleRecord(
                schedule_id="nightly",
                name="fn",
                cron="0 3 * * *",
                next_fire_at=utcnow() - timedelta(minutes=1),
                tenant_id=tenant_a,
            )
        )

        owner = _schedule_store(mongo_client, db_name, suffix, tenant_a)
        loaded = await owner.load("nightly")

        assert loaded is not None
        # Due for the tenant it was registered for — a schedule in the wrong collection
        # never fires, and nothing reports that it did not.
        due = await owner.claim_due(now=utcnow(), limit=10)

        assert [record.schedule_id for record in due] == ["nightly"]
        assert (
            await _schedule_store(mongo_client, db_name, suffix, tenant_b).load("nightly") is None
        )

    async def test_a_contradicted_tenant_is_refused_rather_than_half_applied(
        self, mongo_client: MongoClient, tenants: tuple[UUID, UUID]
    ) -> None:
        db_name = (await mongo_client.db()).name
        suffix = uuid4().hex[:8]
        tenant_a, tenant_b = tenants

        with pytest.raises(CoreException) as raised:
            await _run_store(mongo_client, db_name, suffix, tenant_a).enqueue(
                "cross", input_json=None, tenant_id=tenant_b
            )

        assert raised.value.code == "tenant_mismatch"
