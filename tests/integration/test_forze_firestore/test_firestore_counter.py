"""Integration tests for FirestoreCounterAdapter and FirestoreCounterAdminAdapter."""

import asyncio

import pytest
import pytest_asyncio

from forze.base.exceptions import CoreException
from forze_firestore.adapters import FirestoreCounterAdapter, FirestoreCounterAdminAdapter
from forze_firestore.execution.deps.configs import FirestoreCounterConfig
from forze_firestore.kernel.client import FirestoreClient

# ----------------------- #


@pytest.fixture
def counter_config(unique_collection: str) -> FirestoreCounterConfig:
    return FirestoreCounterConfig(collection=("(default)", unique_collection))


@pytest_asyncio.fixture(scope="function")
async def fs_counter(
    firestore_client: FirestoreClient,
    counter_config: FirestoreCounterConfig,
) -> FirestoreCounterAdapter:
    return FirestoreCounterAdapter(client=firestore_client, config=counter_config, route="orders")


@pytest_asyncio.fixture(scope="function")
async def fs_counter_admin(
    firestore_client: FirestoreClient,
    counter_config: FirestoreCounterConfig,
) -> FirestoreCounterAdminAdapter:
    return FirestoreCounterAdminAdapter(client=firestore_client, config=counter_config, route="orders")


# ....................... #


@pytest.mark.asyncio
async def test_counter_incr(fs_counter: FirestoreCounterAdapter) -> None:
    """incr increments and returns new value."""
    assert await fs_counter.incr() == 1
    assert await fs_counter.incr(by=4) == 5


@pytest.mark.asyncio
async def test_counter_decr(fs_counter: FirestoreCounterAdapter) -> None:
    """decr decrements and returns new value."""
    await fs_counter.incr(by=10)
    assert await fs_counter.decr(by=3) == 7


@pytest.mark.asyncio
async def test_counter_reset(fs_counter: FirestoreCounterAdapter) -> None:
    """reset sets value and returns the new value; next incr continues from it."""
    await fs_counter.incr(by=5)
    assert await fs_counter.reset(value=100) == 100
    assert await fs_counter.incr() == 101


@pytest.mark.asyncio
async def test_counter_reset_creates_missing(fs_counter: FirestoreCounterAdapter) -> None:
    """reset on a counter that never allocated creates it (the import idiom)."""
    assert await fs_counter.reset(value=42, suffix="fresh") == 42
    assert await fs_counter.incr(suffix="fresh") == 43


@pytest.mark.asyncio
async def test_counter_incr_batch(fs_counter: FirestoreCounterAdapter) -> None:
    """incr_batch allocates contiguous ascending values."""
    assert await fs_counter.incr_batch(size=5) == [1, 2, 3, 4, 5]
    assert await fs_counter.incr_batch(size=3) == [6, 7, 8]


@pytest.mark.asyncio
async def test_counter_incr_batch_size_one(fs_counter: FirestoreCounterAdapter) -> None:
    """incr_batch with size=1 returns a single allocated value."""
    assert await fs_counter.incr_batch(size=1) == [1]
    assert await fs_counter.incr_batch(size=1) == [2]


@pytest.mark.asyncio
async def test_counter_incr_batch_size_zero_rejected(
    fs_counter: FirestoreCounterAdapter,
) -> None:
    """incr_batch with size < 1 is a caller error."""
    with pytest.raises(CoreException, match="at least 1"):
        await fs_counter.incr_batch(size=0)


@pytest.mark.asyncio
async def test_counter_suffix_partitions(fs_counter: FirestoreCounterAdapter) -> None:
    """Different suffixes (including None) yield independent counters."""
    assert await fs_counter.incr(suffix="a") == 1
    assert await fs_counter.incr(suffix="b") == 1
    assert await fs_counter.incr() == 1
    assert await fs_counter.incr(suffix="a") == 2


@pytest.mark.asyncio
async def test_counter_suffix_cannot_collide_with_unsuffixed(
    fs_counter: FirestoreCounterAdapter,
) -> None:
    """A suffix named like the unsuffixed sentinel stays a distinct partition."""
    assert await fs_counter.incr() == 1
    assert await fs_counter.incr(suffix="_") == 1
    assert await fs_counter.incr() == 2


@pytest.mark.asyncio
async def test_counter_empty_suffix_distinct_from_none(
    fs_counter: FirestoreCounterAdapter,
    fs_counter_admin: FirestoreCounterAdminAdapter,
) -> None:
    """suffix="" is a real partition, not an alias of the unsuffixed counter."""
    assert await fs_counter.incr(by=2) == 2
    assert await fs_counter.incr(suffix="") == 1

    entries = {e.suffix: e.value for e in await fs_counter_admin.list_counters()}
    assert entries == {None: 2, "": 1}


@pytest.mark.asyncio
async def test_counter_concurrent_incr_distinct(
    fs_counter: FirestoreCounterAdapter,
) -> None:
    """Concurrent incr() calls each allocate a distinct value (exercises the
    transaction-abort retry path; small N — one write/s per document sustained)."""
    values = await asyncio.gather(*(fs_counter.incr() for _ in range(5)))
    assert sorted(values) == list(range(1, 6))


@pytest.mark.asyncio
async def test_counter_allocation_survives_caller_rollback(
    firestore_client: FirestoreClient,
    fs_counter: FirestoreCounterAdapter,
) -> None:
    """An allocation inside a rolled-back transaction is burned, not reused."""
    with pytest.raises(RuntimeError, match="rollback"):
        async with firestore_client.transaction():
            assert await fs_counter.incr() == 1
            raise RuntimeError("rollback")

    assert await fs_counter.incr() == 2


@pytest.mark.asyncio
async def test_counter_admin_enumerates(
    fs_counter: FirestoreCounterAdapter,
    fs_counter_admin: FirestoreCounterAdminAdapter,
) -> None:
    """Enumeration reports every partition, decodes the unsuffixed counter, and does
    not move any counter."""
    await fs_counter.incr(by=2)
    await fs_counter.incr(by=1, suffix="2026")
    await fs_counter.incr(by=5, suffix="2027")

    entries = {e.suffix: e.value for e in await fs_counter_admin.list_counters()}
    assert entries == {None: 2, "2026": 1, "2027": 5}

    # Enumeration is read-only: the next allocation continues, not skips.
    assert await fs_counter.incr() == 3


@pytest.mark.asyncio
async def test_counter_export_import_continuity(
    fs_counter: FirestoreCounterAdapter,
    fs_counter_admin: FirestoreCounterAdminAdapter,
) -> None:
    """The portability idiom: reset(entry.value) elsewhere continues the sequence."""
    await fs_counter.incr_batch(9)

    [entry] = await fs_counter_admin.list_counters()
    assert await fs_counter.reset(entry.value) == 9
    assert await fs_counter.incr() == 10


# ....................... #
# Tenancy + route isolation (the differential leg — the mock cannot show these)


from uuid import uuid4  # noqa: E402

from forze.application.contracts.tenancy import TenantIdentity  # noqa: E402


@pytest.mark.asyncio
async def test_tagged_tenants_do_not_share_a_sequence(
    firestore_client, counter_config
) -> None:
    """Two tenants on one shared collection keep independent sequences."""

    a, b = uuid4(), uuid4()

    def _counter(tenant: object) -> FirestoreCounterAdapter:
        return FirestoreCounterAdapter(
            client=firestore_client,
            config=counter_config,
            route="orders",
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=tenant),
        )

    assert await _counter(a).incr() == 1
    assert await _counter(a).incr() == 2
    assert await _counter(b).incr() == 1  # b starts fresh

    admin_a = FirestoreCounterAdminAdapter(
        client=firestore_client,
        config=counter_config,
        route="orders",
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=a),
    )
    assert {e.suffix: e.value for e in await admin_a.list_counters()} == {None: 2}


@pytest.mark.asyncio
async def test_two_specs_sharing_a_collection_do_not_merge(
    firestore_client, counter_config
) -> None:
    """Two counter specs (routes) on one shared collection keep independent sequences."""

    orders = FirestoreCounterAdapter(client=firestore_client, config=counter_config, route="orders")
    invoices = FirestoreCounterAdapter(
        client=firestore_client, config=counter_config, route="invoices"
    )

    assert await orders.incr() == 1
    assert await orders.incr() == 2
    assert await invoices.incr() == 1  # distinct sequence, not 3

    orders_admin = FirestoreCounterAdminAdapter(
        client=firestore_client, config=counter_config, route="orders"
    )
    assert {e.suffix: e.value for e in await orders_admin.list_counters()} == {None: 2}
