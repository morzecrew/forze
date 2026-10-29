"""Postgres counter lifetimes: the optional ``expires_at`` column, its migration, and the purge.

The shared battery pins what a ``ttl`` means on a table that has the column. These pin what
is particular to Postgres: an older table without it keeps working until a ``ttl`` is asked
for, the migration can land under a running process, and lapsed rows stay until a purge.

# covers: PostgresCounterAdminAdapter.purge_expired, postgres_counter_purge_lifecycle_step
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("psycopg")

from psycopg import sql

from forze.application.contracts.authn import login_digest
from forze.application.contracts.counter import (
    COUNTER_TTL_UNSUPPORTED_CODE,
    CounterSpec,
    counter_capabilities,
)
from forze.application.integrations.authn import LockoutConfig, LoginLockoutGuard
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import FrozenTimeSource, bind_time_source
from forze.testing import context_from_deps
from forze_postgres import PostgresDepsModule, postgres_counter_purge_lifecycle_step
from forze_postgres.adapters import counter as counter_module
from forze_postgres.adapters.counter import (
    _EXPIRY_RECHECK,  # pyright: ignore[reportPrivateUsage]
    PostgresCounterAdapter,
    PostgresCounterAdminAdapter,
)
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


async def _behind_a_row_lock(
    pg_client: PostgresClient, table: str, write: Callable[[], Awaitable[int]]
) -> tuple[int, datetime]:
    """Run *write* while another transaction holds *table*'s rows, which lapse during the hold.

    Returns what *write* returned and the database instant just before the rows were released,
    so a write that judged its row by the statement's start instant shows against it.
    """

    t = sql.Identifier("public", table)

    async with pg_client.transaction():
        await pg_client.execute(
            sql.SQL("UPDATE {t} SET expires_at = clock_timestamp() + interval '1 second'").format(
                t=t
            )
        )
        task = asyncio.create_task(write())

        async with asyncio.timeout(10):  # until the write is queued behind the row lock
            while not await pg_client.fetch_value(
                "SELECT EXISTS (SELECT 1 FROM pg_locks "
                "WHERE NOT granted AND pg_backend_pid() = ANY(pg_blocking_pids(pid)))"
            ):
                await asyncio.sleep(0.01)

        await pg_client.execute("SELECT pg_sleep(1.2)")  # past the lifetime set above
        released = await pg_client.fetch_value("SELECT clock_timestamp()")

    return await task, released


async def _expires_at(pg_client: PostgresClient, table: str) -> datetime:
    return await pg_client.fetch_value(
        sql.SQL("SELECT expires_at FROM {t}").format(t=sql.Identifier("public", table))
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


async def test_an_adapter_built_by_hand_still_sees_the_column(
    pg_client: PostgresClient, table: str
) -> None:
    """Without a wired introspector the adapter builds its own, rather than answering as if
    the column were absent — which would read a lapsed row as live and add onto it."""

    config = PostgresCounterConfig(relation=("public", table))
    counter = PostgresCounterAdapter(client=pg_client, config=config, route="orders")
    admin = PostgresCounterAdminAdapter(client=pg_client, config=config, route="orders")

    assert await counter.incr(by=5, ttl=TTL) == 5

    await _lapse_all(pg_client, table)

    assert await counter.incr(by=0) == 0
    assert await admin.list_counters() == []
    assert await admin.purge_expired() == 1
    assert await counter.incr() == 1


async def test_capabilities_report_a_table_known_to_lack_the_column(
    pg_client: PostgresClient, table: str, legacy_table: str
) -> None:
    migrated, legacy = _counter(pg_client, table), _counter(pg_client, legacy_table)

    assert counter_capabilities(migrated).expiry is True
    assert counter_capabilities(legacy).expiry is True, "unknown until a ttl call checks"

    with pytest.raises(CoreException):
        await legacy.incr(ttl=TTL)

    assert counter_capabilities(legacy).expiry is False, "the refusal recorded the answer"
    assert counter_capabilities(migrated).expiry is True


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

    # Inside the re-check cooldown the cached answer stands, so a caller that falls back on
    # the refusal on every call does not turn each one into a catalog query.
    with pytest.raises(CoreException):
        await counter.incr(ttl=TTL)

    # Stand in for the cooldown.
    _EXPIRY_RECHECK[counter.introspector].invalidate(("", f"public.{legacy_table}"))

    assert await counter.incr(ttl=TTL) == 1, "the next ttl call finds the column"

    await _lapse_all(pg_client, legacy_table)

    assert await counter.incr(by=0) == 0


async def test_a_bump_held_up_by_a_row_lock_sees_the_lapse_that_happened_meanwhile(
    pg_client: PostgresClient, table: str
) -> None:
    """The lapse is judged, and the new lifetime started, when the row is processed.

    ``now()`` is the statement's start: a bump that waited out the lapse on a row lock would
    add onto the lapsed value and start the new lifetime in the past.
    """

    counter = _counter(pg_client, table)
    await counter.incr(by=5, ttl=TTL)

    value, released = await _behind_a_row_lock(pg_client, table, lambda: counter.incr(ttl=TTL))

    assert value == 1, "the lapsed 5 was overwritten, not added onto"
    assert await _expires_at(pg_client, table) >= released + TTL


async def test_a_reset_held_up_by_a_row_lock_gets_its_whole_ttl(
    pg_client: PostgresClient, table: str
) -> None:
    counter = _counter(pg_client, table)
    await counter.incr(ttl=TTL)

    value, released = await _behind_a_row_lock(
        pg_client, table, lambda: counter.reset(7, ttl=TTL)
    )

    assert value == 7
    assert await _expires_at(pg_client, table) >= released + TTL, "no part spent waiting"


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


async def test_purge_walks_every_batch_once(
    pg_client: PostgresClient, table: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Lapsed and live rows interleaved across several batches: every lapsed one goes."""

    monkeypatch.setattr(counter_module, "_PURGE_BATCH", 2)
    counter = _counter(pg_client, table)

    for index in range(7):
        await counter.incr(suffix=f"dead{index}", ttl=TTL)

    await _lapse_all(pg_client, table)

    for index in range(4):
        await counter.incr(suffix=f"dead{index}x")  # sorts between the lapsed rows

    assert await _admin(pg_client, table).purge_expired() == 7
    assert await _rows(pg_client, table) == 4


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


async def test_the_purge_step_deletes_lapsed_rows_and_keeps_live_ones(
    pg_client: PostgresClient, table: str
) -> None:
    ctx = context_from_deps(
        PostgresDepsModule(
            client=pg_client,
            counters={"orders": PostgresCounterConfig(relation=("public", table))},
        )()
    )
    counter = ctx.counter(CounterSpec(name="orders"))

    for day in ("mon", "tue", "wed"):
        await counter.incr(suffix=day, ttl=TTL)

    await _lapse_all(pg_client, table)
    await counter.incr(suffix="thu", ttl=TTL)
    await counter.incr(suffix="forever")

    step = postgres_counter_purge_lifecycle_step(
        specs=[CounterSpec(name="orders")], interval=timedelta(hours=1)
    )
    await step.startup(ctx)

    try:
        # The loop purges once at startup, then sleeps out the interval.
        async with asyncio.timeout(10):
            while await _rows(pg_client, table) > 2:
                await asyncio.sleep(0.05)
    finally:
        await step.shutdown(ctx)

    entries = await _admin(pg_client, table).list_counters()

    assert sorted(entry.suffix or "" for entry in entries) == ["forever", "thu"]


async def test_the_lockout_keeps_working_on_a_table_without_the_column(
    pg_client: PostgresClient, legacy_table: str
) -> None:
    """An upgrade must not lock everyone out: the lockout writes without a ttl instead.

    Its buckets then stay behind as they did before counters could expire, while a caller
    that asks the same counter for a ttl directly is still refused.
    """

    counter = _counter(pg_client, legacy_table, route="authn_lockout")
    guard = LoginLockoutGuard(
        counter=counter,
        config=LockoutConfig(threshold=2, window=timedelta(minutes=15)),
    )
    digest = login_digest("alice")
    t0 = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)

    with bind_time_source(FrozenTimeSource(t0)):
        assert await guard.locked_until(digest) is None

        await guard.record_failure(digest)
        await guard.record_failure(digest)

        assert await guard.is_locked(digest) is True

        await guard.record_success(digest)

        assert await guard.is_locked(digest) is False

        await guard.record_failure(digest)
        await guard.record_failure(digest)

    with bind_time_source(FrozenTimeSource(t0 + timedelta(minutes=15))):
        assert await guard.is_locked(digest) is False, "the next window unlocks"

    assert await _rows(pg_client, legacy_table) == 1, "one window was written, without expiry"

    with pytest.raises(CoreException) as refused:
        await counter.incr(suffix="direct", ttl=TTL)

    assert refused.value.code == COUNTER_TTL_UNSUPPORTED_CODE
