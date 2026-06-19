"""Integration tests for ``lazy_transaction``: defer pool checkout to first query.

With ``lazy_transaction`` enabled, opening a root transaction scope acquires no
pool connection until the first statement runs. These tests use a **max_size=1**
pool so the single physical connection is the contended resource: if a lazy
scope held it during a pre-query window, a concurrent query would block — and the
eager-contrast test proves exactly that discrimination.

The scope must still behave like a transaction once materialized: statements ride
one connection (same backend, same xid), writes commit on clean exit and roll
back on error or cancellation, and read-only / isolation options compose into the
deferred ``BEGIN`` and never leak past the scope.
"""

import asyncio
from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("psycopg")

from datetime import timedelta

from forze_postgres.kernel.client.client import (
    PostgresClient,
    PostgresConfig,
    PostgresTransactionOptions,
)

# ----------------------- #


def _dsn(container) -> str:
    url = container.get_connection_url()
    if url.startswith("postgresql+psycopg://"):
        url = url.replace("postgresql+psycopg://", "postgresql://")
    return url


@pytest_asyncio.fixture(scope="function")
async def lazy_single_client(postgres_container) -> PostgresClient:
    """Lazy client on a single-connection pool with a short acquire timeout."""

    client = PostgresClient()
    await client.initialize(
        dsn=_dsn(postgres_container),
        config=PostgresConfig(min_size=1, max_size=1, lazy_transaction=True),
        acquire_timeout=timedelta(seconds=2),
    )
    yield client
    await client.close()


@pytest_asyncio.fixture(scope="function")
async def eager_single_client(postgres_container) -> PostgresClient:
    """Eager (default) client on a single-connection pool — the contrast case."""

    client = PostgresClient()
    await client.initialize(
        dsn=_dsn(postgres_container),
        config=PostgresConfig(min_size=1, max_size=1, lazy_transaction=False),
        acquire_timeout=timedelta(seconds=2),
    )
    yield client
    await client.close()


@pytest_asyncio.fixture(scope="function")
async def lazy_table(lazy_single_client: PostgresClient) -> str:
    """Throwaway table created out-of-transaction (autocommit) on the lazy client."""

    table = f"lazy_tx_{uuid4().hex[:12]}"
    await lazy_single_client.execute(
        f"CREATE TABLE {table} (id serial PRIMARY KEY, value integer NOT NULL)",
    )
    return table


# ....................... #
# The headline: a parked lazy scope holds no connection.


@pytest.mark.asyncio
async def test_open_lazy_scope_leaves_pool_connection_free(
    lazy_single_client: PostgresClient,
) -> None:
    """A lazy scope held open WITHOUT a query leaves the sole pooled connection
    free, so a concurrent query proceeds immediately."""

    client = lazy_single_client
    parked = asyncio.Event()
    release = asyncio.Event()

    async def parker() -> None:
        # Own (clean) context: opens a lazy tx and never runs a statement.
        async with client.transaction():
            assert client.is_in_transaction() is True
            parked.set()
            await release.wait()

    task = asyncio.create_task(parker())

    try:
        await asyncio.wait_for(parked.wait(), timeout=2)
        # The single connection was never checked out by the parked scope.
        value = await asyncio.wait_for(client.fetch_value("SELECT 1"), timeout=2)
        assert value == 1
    finally:
        release.set()
        await task


@pytest.mark.asyncio
async def test_eager_scope_holds_connection_contrast(
    eager_single_client: PostgresClient,
) -> None:
    """Contrast / discriminator: the SAME shape on an eager client blocks the
    concurrent query (the scope holds the only connection), so it times out."""

    client = eager_single_client
    parked = asyncio.Event()
    release = asyncio.Event()

    async def parker() -> None:
        async with client.transaction():
            parked.set()
            await release.wait()

    task = asyncio.create_task(parker())

    try:
        await asyncio.wait_for(parked.wait(), timeout=2)
        with pytest.raises((asyncio.TimeoutError, Exception)):
            # Eager scope holds the sole connection: the concurrent query cannot
            # acquire it and times out (wait_for fires before the pool timeout).
            await asyncio.wait_for(client.fetch_value("SELECT 1"), timeout=1)
    finally:
        release.set()
        await task


# ....................... #
# Materialized scope behaves like one transaction.


@pytest.mark.asyncio
async def test_first_query_materializes_one_reused_transaction(
    lazy_single_client: PostgresClient,
) -> None:
    """Statements inside a lazy scope share one backend and one xid (reused
    connection), distinct from out-of-transaction statements."""

    client = lazy_single_client

    out_a = await client.fetch_value("SELECT pg_current_xact_id()::text")
    out_b = await client.fetch_value("SELECT pg_current_xact_id()::text")
    assert out_a != out_b  # each autocommit statement is its own transaction

    async with client.transaction():
        pid_1 = await client.fetch_value("SELECT pg_backend_pid()")
        xid_1 = await client.fetch_value("SELECT pg_current_xact_id()::text")
        pid_2 = await client.fetch_value("SELECT pg_backend_pid()")
        xid_2 = await client.fetch_value("SELECT pg_current_xact_id()::text")

    assert pid_1 == pid_2  # one materialized connection, reused
    assert xid_1 == xid_2  # one transaction


@pytest.mark.asyncio
async def test_clean_exit_commits_and_error_rolls_back(
    lazy_single_client: PostgresClient,
    lazy_table: str,
) -> None:
    """Clean exit commits the materialized writes; an error after materialization
    rolls them back (the bare-aclose-commits-on-error trap)."""

    client = lazy_single_client

    async with client.transaction():
        await client.execute(f"INSERT INTO {lazy_table} (value) VALUES (1)")

    class Boom(Exception):
        pass

    with pytest.raises(Boom):
        async with client.transaction():
            await client.execute(f"INSERT INTO {lazy_table} (value) VALUES (99)")
            raise Boom

    rows = await client.fetch_all(f"SELECT value FROM {lazy_table} ORDER BY value")
    assert [r["value"] for r in rows] == [1]


@pytest.mark.asyncio
async def test_empty_lazy_scope_is_a_noop(
    lazy_single_client: PostgresClient,
    lazy_table: str,
) -> None:
    """A lazy scope with no query commits nothing and leaves the pool usable."""

    client = lazy_single_client

    async with client.transaction():
        pass  # never materialized

    # The connection is still healthy for subsequent work.
    await client.execute(f"INSERT INTO {lazy_table} (value) VALUES (7)")
    assert await client.fetch_value(f"SELECT count(*) FROM {lazy_table}") == 1


# ....................... #
# Options compose into the deferred BEGIN and never leak.


@pytest.mark.asyncio
async def test_isolation_option_applies_and_does_not_leak(
    lazy_single_client: PostgresClient,
) -> None:
    """A serializable lazy scope runs serializable; the next statement on the
    same pooled connection is back to read committed."""

    client = lazy_single_client

    async with client.transaction(
        options=PostgresTransactionOptions(isolation="serializable"),
    ):
        level = await client.fetch_value("SHOW transaction_isolation")
        assert level == "serializable"

    # max_size=1: the very same connection — attributes must have been restored.
    level = await client.fetch_value("SHOW transaction_isolation")
    assert level == "read committed"


@pytest.mark.asyncio
async def test_read_only_option_rejects_writes(
    lazy_single_client: PostgresClient,
    lazy_table: str,
) -> None:
    """``read_only`` composes into ``BEGIN ... READ ONLY`` at materialization, so
    a write inside the scope is rejected and nothing leaks to later writes."""

    client = lazy_single_client

    with pytest.raises(Exception):
        async with client.transaction(
            options=PostgresTransactionOptions(read_only=True),
        ):
            await client.execute(f"INSERT INTO {lazy_table} (value) VALUES (1)")

    # A later read-write statement on the same connection still works.
    await client.execute(f"INSERT INTO {lazy_table} (value) VALUES (2)")
    assert await client.fetch_value(f"SELECT count(*) FROM {lazy_table}") == 1


# ....................... #
# Cancellation.


@pytest.mark.asyncio
async def test_cancel_before_materialization_holds_nothing(
    lazy_single_client: PostgresClient,
) -> None:
    """Cancelling a lazy scope that never queried releases nothing and leaves the
    pool immediately usable."""

    client = lazy_single_client
    parked = asyncio.Event()

    async def worker() -> None:
        async with client.transaction():
            parked.set()
            await asyncio.sleep(30)

    task = asyncio.create_task(worker())
    await asyncio.wait_for(parked.wait(), timeout=2)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # The sole connection was never held; normal work proceeds.
    assert await asyncio.wait_for(client.fetch_value("SELECT 1"), timeout=2) == 1


@pytest.mark.asyncio
async def test_cancel_after_materialization_rolls_back(
    lazy_single_client: PostgresClient,
    lazy_table: str,
) -> None:
    """Cancelling a materialized lazy scope rolls back its writes and returns the
    connection to the pool."""

    client = lazy_single_client
    materialized = asyncio.Event()

    async def worker() -> None:
        async with client.transaction():
            await client.execute(f"INSERT INTO {lazy_table} (value) VALUES (1)")
            materialized.set()
            await asyncio.sleep(30)

    task = asyncio.create_task(worker())
    await asyncio.wait_for(materialized.wait(), timeout=2)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # The insert rolled back and the connection is free again.
    assert await asyncio.wait_for(
        client.fetch_value(f"SELECT count(*) FROM {lazy_table}"), timeout=2
    ) == 0
