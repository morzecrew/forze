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
    return MongoCounterAdapter(client=mongo_client, config=counter_config)


@pytest_asyncio.fixture(scope="function")
async def mongo_counter_admin(
    mongo_client: MongoClient,
    counter_config: MongoCounterConfig,
) -> MongoCounterAdminAdapter:
    return MongoCounterAdminAdapter(client=mongo_client, config=counter_config)


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
async def test_counter_concurrent_incr_distinct(
    mongo_counter: MongoCounterAdapter,
) -> None:
    """Concurrent incr() calls each allocate a distinct value."""
    values = await asyncio.gather(*(mongo_counter.incr() for _ in range(20)))
    assert sorted(values) == list(range(1, 21))


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
