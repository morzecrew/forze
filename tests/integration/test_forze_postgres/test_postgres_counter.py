"""Integration tests for PostgresCounterAdapter and PostgresCounterAdminAdapter."""

import asyncio
from uuid import uuid4

import pytest
import pytest_asyncio
from psycopg import sql

from forze.base.exceptions import CoreException
from forze_postgres.adapters.counter import (
    PostgresCounterAdapter,
    PostgresCounterAdminAdapter,
)
from forze_postgres.execution.deps.configs import PostgresCounterConfig
from forze_postgres.kernel.client import PostgresClient

# ----------------------- #


@pytest_asyncio.fixture(scope="function")
async def counter_table(pg_client: PostgresClient) -> str:
    """Create a dedicated counters table and return its name."""

    table = f"counters_{uuid4().hex[:8]}"
    await pg_client.execute(
        sql.SQL(
            """
            CREATE TABLE {table} (
                tenant_id TEXT   NOT NULL,
                suffix    TEXT   NOT NULL,
                value     BIGINT NOT NULL,
                PRIMARY KEY (tenant_id, suffix)
            )
            """
        ).format(table=sql.Identifier("public", table))
    )
    return table


@pytest_asyncio.fixture(scope="function")
async def pg_counter(
    pg_client: PostgresClient,
    counter_table: str,
) -> PostgresCounterAdapter:
    return PostgresCounterAdapter(
        client=pg_client,
        config=PostgresCounterConfig(relation=("public", counter_table)),
    )


@pytest_asyncio.fixture(scope="function")
async def pg_counter_admin(
    pg_client: PostgresClient,
    counter_table: str,
) -> PostgresCounterAdminAdapter:
    return PostgresCounterAdminAdapter(
        client=pg_client,
        config=PostgresCounterConfig(relation=("public", counter_table)),
    )


# ....................... #


@pytest.mark.asyncio
async def test_counter_incr(pg_counter: PostgresCounterAdapter) -> None:
    """incr increments and returns new value."""
    assert await pg_counter.incr() == 1
    assert await pg_counter.incr(by=4) == 5


@pytest.mark.asyncio
async def test_counter_decr(pg_counter: PostgresCounterAdapter) -> None:
    """decr decrements and returns new value."""
    await pg_counter.incr(by=10)
    assert await pg_counter.decr(by=3) == 7


@pytest.mark.asyncio
async def test_counter_reset(pg_counter: PostgresCounterAdapter) -> None:
    """reset sets value and returns the new value; next incr continues from it."""
    await pg_counter.incr(by=5)
    assert await pg_counter.reset(value=100) == 100
    assert await pg_counter.incr() == 101


@pytest.mark.asyncio
async def test_counter_reset_creates_missing(pg_counter: PostgresCounterAdapter) -> None:
    """reset on a counter that never allocated creates it (the import idiom)."""
    assert await pg_counter.reset(value=42, suffix="fresh") == 42
    assert await pg_counter.incr(suffix="fresh") == 43


@pytest.mark.asyncio
async def test_counter_incr_batch(pg_counter: PostgresCounterAdapter) -> None:
    """incr_batch allocates contiguous ascending values."""
    assert await pg_counter.incr_batch(size=5) == [1, 2, 3, 4, 5]
    assert await pg_counter.incr_batch(size=3) == [6, 7, 8]


@pytest.mark.asyncio
async def test_counter_incr_batch_size_one(pg_counter: PostgresCounterAdapter) -> None:
    """incr_batch with size=1 returns a single allocated value."""
    assert await pg_counter.incr_batch(size=1) == [1]
    assert await pg_counter.incr_batch(size=1) == [2]


@pytest.mark.asyncio
async def test_counter_incr_batch_size_zero_rejected(
    pg_counter: PostgresCounterAdapter,
) -> None:
    """incr_batch with size < 1 is a caller error."""
    with pytest.raises(CoreException, match="at least 1"):
        await pg_counter.incr_batch(size=0)


@pytest.mark.asyncio
async def test_counter_suffix_partitions(pg_counter: PostgresCounterAdapter) -> None:
    """Different suffixes (including None) yield independent counters."""
    assert await pg_counter.incr(suffix="a") == 1
    assert await pg_counter.incr(suffix="b") == 1
    assert await pg_counter.incr() == 1
    assert await pg_counter.incr(suffix="a") == 2


@pytest.mark.asyncio
async def test_counter_concurrent_incr_distinct(
    pg_counter: PostgresCounterAdapter,
) -> None:
    """Concurrent incr() calls each allocate a distinct value."""
    values = await asyncio.gather(*(pg_counter.incr() for _ in range(20)))
    assert sorted(values) == list(range(1, 21))


@pytest.mark.asyncio
async def test_counter_admin_enumerates(
    pg_counter: PostgresCounterAdapter,
    pg_counter_admin: PostgresCounterAdminAdapter,
) -> None:
    """Enumeration reports every partition, decodes the unsuffixed counter, and does
    not move any counter."""
    await pg_counter.incr(by=2)
    await pg_counter.incr(by=1, suffix="2026")
    await pg_counter.incr(by=5, suffix="2027")

    entries = {e.suffix: e.value for e in await pg_counter_admin.list_counters()}
    assert entries == {None: 2, "2026": 1, "2027": 5}

    # Enumeration is read-only: the next allocation continues, not skips.
    assert await pg_counter.incr() == 3


@pytest.mark.asyncio
async def test_counter_export_import_continuity(
    pg_counter: PostgresCounterAdapter,
    pg_counter_admin: PostgresCounterAdminAdapter,
) -> None:
    """The portability idiom: reset(entry.value) elsewhere continues the sequence."""
    await pg_counter.incr_batch(9)

    [entry] = await pg_counter_admin.list_counters()
    assert await pg_counter.reset(entry.value) == 9
    assert await pg_counter.incr() == 10
