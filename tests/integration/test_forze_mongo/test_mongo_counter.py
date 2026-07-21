"""Integration tests for MongoCounterAdapter and MongoCounterAdminAdapter."""

import asyncio
from uuid import uuid4

import pytest
import pytest_asyncio

from forze.base.exceptions import CoreException
from forze_mongo.adapters.counter import MongoCounterAdapter, MongoCounterAdminAdapter
from forze_mongo.execution.deps.configs import MongoCounterConfig
from forze_mongo.kernel.client import MongoClient

# ----------------------- #


@pytest_asyncio.fixture(scope="function")
async def counter_config(mongo_client: MongoClient) -> MongoCounterConfig:
    db_name = (await mongo_client.db()).name
    return MongoCounterConfig(collection=(db_name, f"counters_{uuid4().hex[:8]}"))


@pytest_asyncio.fixture(scope="function")
async def mongo_counter(
    mongo_client: MongoClient,
    counter_config: MongoCounterConfig,
) -> MongoCounterAdapter:
    return MongoCounterAdapter(client=mongo_client, config=counter_config, route="orders")


@pytest_asyncio.fixture(scope="function")
async def mongo_counter_admin(
    mongo_client: MongoClient,
    counter_config: MongoCounterConfig,
) -> MongoCounterAdminAdapter:
    return MongoCounterAdminAdapter(client=mongo_client, config=counter_config, route="orders")


# ....................... #


@pytest.mark.asyncio
async def test_counter_incr(mongo_counter: MongoCounterAdapter) -> None:
    """incr increments and returns new value."""
    assert await mongo_counter.incr() == 1
    assert await mongo_counter.incr(by=4) == 5


@pytest.mark.asyncio
async def test_counter_decr(mongo_counter: MongoCounterAdapter) -> None:
    """decr decrements and returns new value."""
    await mongo_counter.incr(by=10)
    assert await mongo_counter.decr(by=3) == 7


@pytest.mark.asyncio
async def test_counter_reset(mongo_counter: MongoCounterAdapter) -> None:
    """reset sets value and returns the new value; next incr continues from it."""
    await mongo_counter.incr(by=5)
    assert await mongo_counter.reset(value=100) == 100
    assert await mongo_counter.incr() == 101


@pytest.mark.asyncio
async def test_counter_reset_creates_missing(mongo_counter: MongoCounterAdapter) -> None:
    """reset on a counter that never allocated creates it (the import idiom)."""
    assert await mongo_counter.reset(value=42, suffix="fresh") == 42
    assert await mongo_counter.incr(suffix="fresh") == 43


@pytest.mark.asyncio
async def test_counter_incr_batch(mongo_counter: MongoCounterAdapter) -> None:
    """incr_batch allocates contiguous ascending values."""
    assert await mongo_counter.incr_batch(size=5) == [1, 2, 3, 4, 5]
    assert await mongo_counter.incr_batch(size=3) == [6, 7, 8]


@pytest.mark.asyncio
async def test_counter_incr_batch_size_one(mongo_counter: MongoCounterAdapter) -> None:
    """incr_batch with size=1 returns a single allocated value."""
    assert await mongo_counter.incr_batch(size=1) == [1]
    assert await mongo_counter.incr_batch(size=1) == [2]


@pytest.mark.asyncio
async def test_counter_incr_batch_size_zero_rejected(
    mongo_counter: MongoCounterAdapter,
) -> None:
    """incr_batch with size < 1 is a caller error."""
    with pytest.raises(CoreException, match="at least 1"):
        await mongo_counter.incr_batch(size=0)


@pytest.mark.asyncio
async def test_counter_suffix_partitions(mongo_counter: MongoCounterAdapter) -> None:
    """Different suffixes (including None) yield independent counters."""
    assert await mongo_counter.incr(suffix="a") == 1
    assert await mongo_counter.incr(suffix="b") == 1
    assert await mongo_counter.incr() == 1
    assert await mongo_counter.incr(suffix="a") == 2


@pytest.mark.asyncio
async def test_counter_empty_suffix_distinct_from_none(
    mongo_counter: MongoCounterAdapter,
    mongo_counter_admin: MongoCounterAdminAdapter,
) -> None:
    """suffix="" is a real partition, not an alias of the unsuffixed counter."""
    assert await mongo_counter.incr(by=2) == 2
    assert await mongo_counter.incr(suffix="") == 1

    entries = {e.suffix: e.value for e in await mongo_counter_admin.list_counters()}
    assert entries == {None: 2, "": 1}


@pytest.mark.asyncio
async def test_counter_concurrent_incr_distinct(
    mongo_counter: MongoCounterAdapter,
) -> None:
    """Concurrent incr() calls each allocate a distinct value."""
    values = await asyncio.gather(*(mongo_counter.incr() for _ in range(20)))
    assert sorted(values) == list(range(1, 21))


@pytest.mark.asyncio
async def test_counter_allocation_survives_caller_rollback(
    mongo_client_replica: MongoClient,
) -> None:
    """An allocation inside a rolled-back transaction is burned, not reused."""
    db_name = (await mongo_client_replica.db()).name
    counter = MongoCounterAdapter(
        client=mongo_client_replica,
        config=MongoCounterConfig(collection=(db_name, f"counters_{uuid4().hex[:8]}")),
        route="orders",
    )

    with pytest.raises(RuntimeError, match="rollback"):
        async with mongo_client_replica.transaction():
            assert await counter.incr() == 1
            raise RuntimeError("rollback")

    assert await counter.incr() == 2


@pytest.mark.asyncio
async def test_counter_admin_enumerates(
    mongo_counter: MongoCounterAdapter,
    mongo_counter_admin: MongoCounterAdminAdapter,
) -> None:
    """Enumeration reports every partition, decodes the unsuffixed counter, and does
    not move any counter."""
    await mongo_counter.incr(by=2)
    await mongo_counter.incr(by=1, suffix="2026")
    await mongo_counter.incr(by=5, suffix="2027")

    entries = {e.suffix: e.value for e in await mongo_counter_admin.list_counters()}
    assert entries == {None: 2, "2026": 1, "2027": 5}

    # Enumeration is read-only: the next allocation continues, not skips.
    assert await mongo_counter.incr() == 3


@pytest.mark.asyncio
async def test_counter_export_import_continuity(
    mongo_counter: MongoCounterAdapter,
    mongo_counter_admin: MongoCounterAdminAdapter,
) -> None:
    """The portability idiom: reset(entry.value) elsewhere continues the sequence."""
    await mongo_counter.incr_batch(9)

    [entry] = await mongo_counter_admin.list_counters()
    assert await mongo_counter.reset(entry.value) == 9
    assert await mongo_counter.incr() == 10


# ....................... #
# Tenancy + route isolation (the differential leg — the mock cannot show these)


from forze.application.contracts.tenancy import TenantIdentity  # noqa: E402


@pytest.mark.asyncio
async def test_tagged_tenants_do_not_share_a_sequence(
    mongo_client: MongoClient, counter_config: MongoCounterConfig
) -> None:
    """Two tenants on one shared collection keep independent sequences."""

    a, b = uuid4(), uuid4()

    def _counter(tenant: object) -> MongoCounterAdapter:
        cfg = MongoCounterConfig(collection=counter_config.collection, tenant_aware=True)
        return MongoCounterAdapter(
            client=mongo_client,
            config=cfg,
            route="orders",
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=tenant),
        )

    assert await _counter(a).incr() == 1
    assert await _counter(a).incr() == 2
    assert await _counter(b).incr() == 1  # b starts fresh

    admin_a = MongoCounterAdminAdapter(
        client=mongo_client,
        config=MongoCounterConfig(collection=counter_config.collection, tenant_aware=True),
        route="orders",
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=a),
    )
    assert {e.suffix: e.value for e in await admin_a.list_counters()} == {None: 2}


@pytest.mark.asyncio
async def test_two_specs_sharing_a_collection_do_not_merge(
    mongo_client: MongoClient, counter_config: MongoCounterConfig
) -> None:
    """Two counter specs (routes) on one shared collection keep independent sequences."""

    orders = MongoCounterAdapter(client=mongo_client, config=counter_config, route="orders")
    invoices = MongoCounterAdapter(client=mongo_client, config=counter_config, route="invoices")

    assert await orders.incr() == 1
    assert await orders.incr() == 2
    assert await invoices.incr() == 1  # distinct sequence, not 3

    orders_admin = MongoCounterAdminAdapter(
        client=mongo_client, config=counter_config, route="orders"
    )
    assert {e.suffix: e.value for e in await orders_admin.list_counters()} == {None: 2}


@pytest.mark.asyncio
async def test_legacy_document_continues_its_sequence(
    mongo_client: MongoClient, counter_config: MongoCounterConfig
) -> None:
    """A counter document written before the route fold (legacy ``_id``, no ``route`` field)
    is migrated onto the new id so its sequence continues instead of restarting at zero."""

    db_name, coll_name = counter_config.collection
    coll = await mongo_client.collection(coll_name, db_name=db_name)

    # Seed a pre-route document: legacy unsuffixed _id "", no route/tenant fields, value=41.
    await coll.insert_one({"_id": "", "suffix": None, "value": 41})

    counter = MongoCounterAdapter(client=mongo_client, config=counter_config, route="orders")
    assert await counter.incr() == 42  # continues from 41
    assert await counter.incr() == 43

    admin = MongoCounterAdminAdapter(client=mongo_client, config=counter_config, route="orders")
    assert {e.suffix: e.value for e in await admin.list_counters()} == {None: 43}

    # The legacy document was retired (migrated onto the route-prefixed id).
    assert await coll.find_one({"_id": ""}) is None


@pytest.mark.asyncio
async def test_legacy_migration_keeps_new_document_when_both_exist(
    mongo_client: MongoClient, counter_config: MongoCounterConfig
) -> None:
    """If a route-prefixed document already exists when a legacy row is found (a concurrent
    writer or a prior migration), migration keeps the new document and only retires the
    legacy one — the new sequence is never overwritten by the legacy value."""

    db_name, coll_name = counter_config.collection
    coll = await mongo_client.collection(coll_name, db_name=db_name)

    counter = MongoCounterAdapter(client=mongo_client, config=counter_config, route="orders")

    # Establish the route-prefixed document (value=1), then re-introduce a legacy row that
    # claims a far larger value; the next allocation must not adopt it.
    assert await counter.incr() == 1
    await coll.insert_one({"_id": "", "suffix": None, "value": 500})

    assert await counter.incr() == 2  # continues the new sequence, ignores the legacy 500

    # The legacy row is retired even though its value was discarded.
    assert await coll.find_one({"_id": ""}) is None
