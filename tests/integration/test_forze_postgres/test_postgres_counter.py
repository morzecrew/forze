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
        route="orders",
    )


@pytest_asyncio.fixture(scope="function")
async def pg_counter_admin(
    pg_client: PostgresClient,
    counter_table: str,
) -> PostgresCounterAdminAdapter:
    return PostgresCounterAdminAdapter(
        client=pg_client,
        config=PostgresCounterConfig(relation=("public", counter_table)),
        route="orders",
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
async def test_counter_empty_suffix_distinct_from_none(
    pg_counter: PostgresCounterAdapter,
    pg_counter_admin: PostgresCounterAdminAdapter,
) -> None:
    """suffix="" is a real partition, not an alias of the unsuffixed counter."""
    assert await pg_counter.incr(by=2) == 2
    assert await pg_counter.incr(suffix="") == 1

    entries = {e.suffix: e.value for e in await pg_counter_admin.list_counters()}
    assert entries == {None: 2, "": 1}


@pytest.mark.asyncio
async def test_counter_concurrent_incr_distinct(
    pg_counter: PostgresCounterAdapter,
) -> None:
    """Concurrent incr() calls each allocate a distinct value."""
    values = await asyncio.gather(*(pg_counter.incr() for _ in range(20)))
    assert sorted(values) == list(range(1, 21))


@pytest.mark.asyncio
async def test_counter_allocation_survives_caller_rollback(
    pg_client: PostgresClient,
    pg_counter: PostgresCounterAdapter,
) -> None:
    """An allocation inside a rolled-back transaction is burned, not reused."""
    with pytest.raises(RuntimeError, match="rollback"):
        async with pg_client.transaction():
            assert await pg_counter.incr() == 1
            raise RuntimeError("rollback")

    assert await pg_counter.incr() == 2


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


# ....................... #
# Tenancy + route isolation (the differential leg — the mock cannot show these)


from forze.application.contracts.tenancy import TenantIdentity  # noqa: E402


def _tenant_counter(
    pg_client: PostgresClient, table: str, *, route: str, tenant: object
) -> PostgresCounterAdapter:
    """A tagged-tier counter bound to a fixed tenant, sharing *table* with others."""

    return PostgresCounterAdapter(
        client=pg_client,
        config=PostgresCounterConfig(relation=("public", table), tenant_aware=True),
        route=route,
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tenant) if tenant is not None else None,
    )


@pytest.mark.asyncio
async def test_tagged_tenants_do_not_share_a_sequence(
    pg_client: PostgresClient, counter_table: str
) -> None:
    """Two tenants on one shared table keep independent sequences — no silent collision."""

    a, b = uuid4(), uuid4()
    counter_a = _tenant_counter(pg_client, counter_table, route="orders", tenant=a)
    counter_b = _tenant_counter(pg_client, counter_table, route="orders", tenant=b)

    assert await counter_a.incr() == 1
    assert await counter_a.incr() == 2
    assert await counter_b.incr() == 1  # b starts fresh, not at 3

    admin_a = PostgresCounterAdminAdapter(
        client=pg_client,
        config=PostgresCounterConfig(relation=("public", counter_table), tenant_aware=True),
        route="orders",
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=a),
    )
    assert {e.suffix: e.value for e in await admin_a.list_counters()} == {None: 2}


@pytest.mark.asyncio
async def test_two_specs_sharing_a_table_do_not_merge(
    pg_client: PostgresClient, counter_table: str
) -> None:
    """Two counter specs (routes) on one shared table keep independent sequences."""

    orders = PostgresCounterAdapter(
        client=pg_client,
        config=PostgresCounterConfig(relation=("public", counter_table)),
        route="orders",
    )
    invoices = PostgresCounterAdapter(
        client=pg_client,
        config=PostgresCounterConfig(relation=("public", counter_table)),
        route="invoices",
    )

    assert await orders.incr() == 1
    assert await orders.incr() == 2
    assert await invoices.incr() == 1  # invoices is a distinct sequence, not 3

    orders_admin = PostgresCounterAdminAdapter(
        client=pg_client,
        config=PostgresCounterConfig(relation=("public", counter_table)),
        route="orders",
    )
    # enumeration reports only this route's counters, decoded to their real suffixes
    assert {e.suffix: e.value for e in await orders_admin.list_counters()} == {None: 2}


@pytest.mark.asyncio
async def test_namespace_tier_resolves_a_per_tenant_relation(
    pg_client: PostgresClient,
) -> None:
    """Namespace tier (no tagged ``tenant_aware``, a per-tenant relation resolver): the
    bound tenant must reach relation resolution — the getter bug dropped it, folding
    every tenant onto one table."""

    a, b = uuid4(), uuid4()
    tables = {a: f"ns_a_{uuid4().hex[:8]}", b: f"ns_b_{uuid4().hex[:8]}"}

    for name in tables.values():
        await pg_client.execute(
            sql.SQL(
                "CREATE TABLE {t} (tenant_id TEXT NOT NULL, suffix TEXT NOT NULL, "
                "value BIGINT NOT NULL, PRIMARY KEY (tenant_id, suffix))"
            ).format(t=sql.Identifier("public", name))
        )

    def _counter(tenant: object) -> PostgresCounterAdapter:
        # tenant_aware=False (namespace tier), but a resolver keys the table by tenant.
        return PostgresCounterAdapter(
            client=pg_client,
            config=PostgresCounterConfig(
                relation=lambda tid: ("public", tables[tid]),
            ),
            route="orders",
            tenant_provider=lambda: TenantIdentity(tenant_id=tenant),
        )

    assert await _counter(a).incr() == 1
    assert await _counter(a).incr() == 2
    assert await _counter(b).incr() == 1  # b's table is separate — not folded onto a's


@pytest.mark.asyncio
async def test_legacy_unprefixed_row_continues_its_sequence(
    pg_client: PostgresClient, counter_table: str
) -> None:
    """A counter allocated before the route fold (stored at the bare ``suffix`` key) must
    continue from its value on the next increment, not restart at zero and reissue numbers."""

    # Seed a pre-route row: tenant_id='' (no tenant), suffix='' (unsuffixed), value=41.
    await pg_client.execute(
        sql.SQL("INSERT INTO {t} (tenant_id, suffix, value) VALUES ('', '', 41)").format(
            t=sql.Identifier("public", counter_table)
        )
    )
    counter = PostgresCounterAdapter(
        client=pg_client,
        config=PostgresCounterConfig(relation=("public", counter_table)),
        route="orders",
    )

    assert await counter.incr() == 42  # continues from the legacy 41, not from 0
    assert await counter.incr() == 43  # and the legacy value is not re-added

    admin = PostgresCounterAdminAdapter(
        client=pg_client,
        config=PostgresCounterConfig(relation=("public", counter_table)),
        route="orders",
    )
    assert {e.suffix: e.value for e in await admin.list_counters()} == {None: 43}


@pytest.mark.asyncio
async def test_legacy_suffixed_row_continues_its_sequence(
    pg_client: PostgresClient, counter_table: str
) -> None:
    await pg_client.execute(
        sql.SQL("INSERT INTO {t} (tenant_id, suffix, value) VALUES ('', 's:2026', 7)").format(
            t=sql.Identifier("public", counter_table)
        )
    )
    counter = PostgresCounterAdapter(
        client=pg_client,
        config=PostgresCounterConfig(relation=("public", counter_table)),
        route="orders",
    )
    assert await counter.incr(suffix="2026") == 8
