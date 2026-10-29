"""Postgres counter lifetimes: the optional ``expires_at`` column, its migration, and the purge.

The shared battery pins what a ``ttl`` means on a table that has the column. These pin what
is particular to Postgres: an older table without it keeps working until a ``ttl`` is asked
for, the migration can land under a running process, and lapsed rows stay until a purge.

# covers: PostgresCounterAdminAdapter.purge_expired
"""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("psycopg")

from psycopg import sql

from forze.application.contracts.counter import (
    COUNTER_TTL_UNSUPPORTED_CODE,
    CounterSpec,
    counter_capabilities,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze.testing import context_from_deps
from forze_postgres import PostgresDepsModule
from forze_postgres.adapters.counter import PostgresCounterAdapter, PostgresCounterAdminAdapter
from forze_postgres.execution.deps.configs import PostgresCounterConfig
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client import PostgresClient

# ----------------------- #

TTL = timedelta(hours=1)
"""Long enough never to lapse on its own; the tests lapse a row by moving its instant."""


async def _create(pg_client: PostgresClient, *, with_expiry: bool) -> str:
    table = f"counter_exp_{uuid4().hex[:8]}"
    expiry = ", expires_at timestamptz" if with_expiry else ""
    await pg_client.execute(
        sql.SQL(
            "CREATE TABLE {t} (tenant_id text NOT NULL, suffix text NOT NULL, "
            "value bigint NOT NULL" + expiry + ", PRIMARY KEY (tenant_id, suffix))"
        ).format(t=sql.Identifier("public", table))
    )
    return table


async def _lapse_all(pg_client: PostgresClient, table: str) -> None:
    """Move every lifetime in *table* into the past — a lapse without waiting one out."""

    await pg_client.execute(
        sql.SQL(
            "UPDATE {t} SET expires_at = now() - interval '1 second' WHERE expires_at IS NOT NULL"
        ).format(t=sql.Identifier("public", table))
    )


async def _rows(pg_client: PostgresClient, table: str) -> int:
    return int(
        await pg_client.fetch_value(
            sql.SQL("SELECT count(*) FROM {t}").format(t=sql.Identifier("public", table))
        )
    )


@pytest_asyncio.fixture
async def legacy_table(pg_client: PostgresClient) -> str:
    return await _create(pg_client, with_expiry=False)


@pytest_asyncio.fixture
async def table(pg_client: PostgresClient) -> str:
    return await _create(pg_client, with_expiry=True)


def _counter(
    pg_client: PostgresClient, table: str, route: str = "orders"
) -> PostgresCounterAdapter:
    return PostgresCounterAdapter(
        client=pg_client,
        config=PostgresCounterConfig(relation=("public", table)),
        route=route,
        introspector=PostgresIntrospector(client=pg_client),
    )


def _admin(
    pg_client: PostgresClient, table: str, route: str = "orders"
) -> PostgresCounterAdminAdapter:
    return PostgresCounterAdminAdapter(
        client=pg_client,
        config=PostgresCounterConfig(relation=("public", table)),
        route=route,
        introspector=PostgresIntrospector(client=pg_client),
    )


# ....................... #


async def test_a_table_without_the_column_works_until_a_ttl_is_requested(
    pg_client: PostgresClient, legacy_table: str
) -> None:
    counter = _counter(pg_client, legacy_table)

    assert await counter.incr() == 1
    assert await counter.incr(by=0) == 1
    assert await counter.reset(5) == 5
    assert [e.value for e in await _admin(pg_client, legacy_table).list_counters()] == [5]

    for write, args in ((counter.incr, {}), (counter.incr, {"by": 0}), (counter.reset, {})):
        with pytest.raises(CoreException) as error:
            await write(suffix="new", ttl=TTL, **args)

        assert error.value.kind is ExceptionKind.CONFIGURATION
        assert error.value.code == COUNTER_TTL_UNSUPPORTED_CODE
        assert "ADD COLUMN expires_at" in str(error.value)

    assert await _rows(pg_client, legacy_table) == 1, "a refused ttl stored nothing"


async def test_an_adapter_without_catalog_access_refuses_a_ttl(
    pg_client: PostgresClient, table: str
) -> None:
    counter = PostgresCounterAdapter(
        client=pg_client,
        config=PostgresCounterConfig(relation=("public", table)),
        route="orders",
    )

    with pytest.raises(CoreException) as error:
        await counter.incr(ttl=TTL)

    assert error.value.code == COUNTER_TTL_UNSUPPORTED_CODE
    assert await counter.incr() == 1, "and keeps working without one"


async def test_the_migration_can_land_under_a_running_process(
    pg_client: PostgresClient, legacy_table: str
) -> None:
    counter = _counter(pg_client, legacy_table)

    with pytest.raises(CoreException):
        await counter.incr(ttl=TTL)  # caches the table without the column

    await pg_client.execute(
        sql.SQL("ALTER TABLE {t} ADD COLUMN expires_at timestamptz").format(
            t=sql.Identifier("public", legacy_table)
        )
    )

    assert await counter.incr(ttl=TTL) == 1, "the next ttl call finds the column"

    await _lapse_all(pg_client, legacy_table)

    assert await counter.incr(by=0) == 0


async def test_a_read_falls_back_to_a_legacy_row_only_while_the_counter_is_absent(
    pg_client: PostgresClient, table: str
) -> None:
    await pg_client.execute(
        sql.SQL("INSERT INTO {t} (tenant_id, suffix, value) VALUES ('', '', 41)").format(
            t=sql.Identifier("public", table)
        )
    )
    counter = _counter(pg_client, table)

    assert await counter.incr(by=0) == 41, "the pre-route row stands in for an absent counter"
    assert await _rows(pg_client, table) == 1, "and reading it created nothing"

    assert await counter.incr(ttl=TTL) == 42, "the first write seeds from it"

    await _lapse_all(pg_client, table)

    # The route-prefixed row exists and has lapsed: the counter is absent, not the legacy 41.
    assert await counter.incr(by=0) == 0
    assert await counter.incr() == 1


async def test_purge_removes_only_this_routes_lapsed_rows(
    pg_client: PostgresClient, table: str
) -> None:
    orders, invoices = _counter(pg_client, table), _counter(pg_client, table, route="invoices")

    for suffix in ("a", "b", "c"):
        await orders.incr(suffix=suffix, ttl=TTL)

    await invoices.incr(ttl=TTL)
    await _lapse_all(pg_client, table)
    await orders.incr(suffix="live", ttl=TTL)
    await orders.incr(suffix="forever")

    assert await _admin(pg_client, table).purge_expired() == 3
    assert {e.suffix for e in await _admin(pg_client, table).list_counters()} == {
        "live",
        "forever",
    }
    # The other route's lapsed row is its own purge's business.
    assert await _rows(pg_client, table) == 3
    assert await _admin(pg_client, table).purge_expired() == 0


async def test_purge_on_a_table_without_the_column_is_a_no_op(
    pg_client: PostgresClient, legacy_table: str
) -> None:
    await _counter(pg_client, legacy_table).incr()

    assert await _admin(pg_client, legacy_table).purge_expired() == 0
    assert await _rows(pg_client, legacy_table) == 1


async def test_the_wired_route_honours_a_ttl(pg_client: PostgresClient, table: str) -> None:
    ctx = context_from_deps(
        PostgresDepsModule(
            client=pg_client,
            counters={"orders": PostgresCounterConfig(relation=("public", table))},
        )()
    )
    counter = ctx.counter(CounterSpec(name="orders"))

    assert counter_capabilities(counter).expiry
    assert await counter.incr(ttl=TTL) == 1

    await _lapse_all(pg_client, table)

    assert await counter.incr(by=0) == 0
