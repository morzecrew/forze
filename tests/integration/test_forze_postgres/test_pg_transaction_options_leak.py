"""Regression tests: transaction options must not leak onto pooled connections.

Read-only / isolation options are applied via psycopg connection attributes
(``set_read_only`` / ``set_isolation_level``) so psycopg composes them into the
``BEGIN`` statement itself — zero extra round-trips per root transaction. Those
attributes persist across pool check-ins, which originally leaked: after any
read-only transaction the same pooled connection issued ``BEGIN READ ONLY`` for
unrelated later work and rejected writes. Two belts now close that leak — a
``finally`` restore on every top-level transaction path and a pool ``reset=``
callback clearing the attributes on check-in. A ``max_size=1`` pool guarantees
every operation reuses the *same* connection, so any leak surfaces
deterministically.
"""

import asyncio
from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("psycopg")

from psycopg import IsolationLevel

from forze_postgres.kernel.client.client import (
    PostgresClient,
    PostgresConfig,
    PostgresTransactionOptions,
)

# ----------------------- #


@pytest_asyncio.fixture(scope="function")
async def pg_client_single_conn(postgres_container) -> PostgresClient:
    """PostgresClient with a single-connection pool (max_size=1).

    Every operation checks out the same physical connection, so any state
    leaked by a previous transaction is observed by the next operation.
    """

    url = postgres_container.get_connection_url()

    if url.startswith("postgresql+psycopg://"):
        url = url.replace("postgresql+psycopg://", "postgresql://")

    client = PostgresClient()
    await client.initialize(dsn=url, config=PostgresConfig(min_size=1, max_size=1))

    yield client

    await client.close()


@pytest_asyncio.fixture(scope="function")
async def leak_table(pg_client_single_conn: PostgresClient) -> str:
    """Throwaway table for write attempts after read-only transactions."""

    table = f"tx_leak_{uuid4().hex[:12]}"
    await pg_client_single_conn.execute(
        f"CREATE TABLE {table} (id serial PRIMARY KEY, value integer NOT NULL)",
    )
    return table


# ....................... #


@pytest.mark.asyncio
async def test_read_only_tx_does_not_leak_to_later_work(
    pg_client_single_conn: PostgresClient,
    leak_table: str,
) -> None:
    """THE regression: read-only tx, then a plain write and a read-write tx succeed."""

    client = pg_client_single_conn

    async with client.transaction(
        options=PostgresTransactionOptions(read_only=True),
    ):
        rows = await client.fetch_all(f"SELECT count(*) AS n FROM {leak_table}")
        assert rows[0]["n"] == 0

    # (1) Plain execute outside any transaction on the same pooled connection.
    await client.execute(f"INSERT INTO {leak_table} (value) VALUES (1)")

    # (2) Default (read-write) transaction performing a write.
    async with client.transaction():
        await client.execute(f"INSERT INTO {leak_table} (value) VALUES (2)")

    rows = await client.fetch_all(f"SELECT value FROM {leak_table} ORDER BY value")
    assert [r["value"] for r in rows] == [1, 2]


@pytest.mark.asyncio
async def test_serializable_tx_does_not_leak_isolation(
    pg_client_single_conn: PostgresClient,
) -> None:
    """Serializable tx, then a non-transactional statement sees default isolation."""

    client = pg_client_single_conn

    async with client.transaction(
        options=PostgresTransactionOptions(isolation="serializable"),
    ):
        level = await client.fetch_value("SHOW transaction_isolation")
        assert level == "serializable"

    level = await client.fetch_value("SHOW transaction_isolation")
    assert level == "read committed"


@pytest.mark.asyncio
async def test_read_only_tx_still_rejects_writes(
    pg_client_single_conn: PostgresClient,
    leak_table: str,
) -> None:
    """Read-only enforcement is preserved inside the transaction itself."""

    client = pg_client_single_conn

    with pytest.raises(Exception):  # psycopg errors.ReadOnlySqlTransaction
        async with client.transaction(
            options=PostgresTransactionOptions(read_only=True),
        ):
            await client.execute(f"INSERT INTO {leak_table} (value) VALUES (1)")

    # And the connection is usable read-write afterwards.
    await client.execute(f"INSERT INTO {leak_table} (value) VALUES (2)")
    rows = await client.fetch_all(f"SELECT value FROM {leak_table}")
    assert [r["value"] for r in rows] == [2]


@pytest.mark.asyncio
async def test_nested_tx_after_queries_in_read_only_root(
    pg_client_single_conn: PostgresClient,
) -> None:
    """Savepoints must not emit SET TRANSACTION (illegal after the root's first query)."""

    client = pg_client_single_conn

    async with client.transaction(
        options=PostgresTransactionOptions(
            isolation="serializable",
            read_only=True,
        ),
    ):
        rows = await client.fetch_all("SELECT 1 AS n")
        assert rows[0]["n"] == 1

        # Nested scope opens a savepoint; emitting SET TRANSACTION here would
        # fail (isolation cannot change after the first query of the root tx).
        async with client.transaction(
            options=PostgresTransactionOptions(read_only=True),
        ):
            rows = await client.fetch_all("SELECT 2 AS n")
            assert rows[0]["n"] == 2

    # Root options leaked nothing.
    level = await client.fetch_value("SHOW transaction_isolation")
    assert level == "read committed"


# ....................... #
# Attribute-level belts: options are applied as psycopg connection attributes
# (composed into BEGIN, zero extra round-trips), so these tests inspect the
# RAW attributes on the same pooled connection to prove they never survive a
# root transaction — neither on the happy path, nor on errors/cancellation,
# nor past a pool check-in (reset callback).


@pytest.mark.asyncio
async def test_attributes_restored_on_pooled_connection(
    pg_client_single_conn: PostgresClient,
) -> None:
    """After a read-only/serializable root tx, the raw connection attributes are default."""

    client = pg_client_single_conn

    async with client.transaction(
        options=PostgresTransactionOptions(read_only=True, isolation="serializable"),
    ):
        assert await client.fetch_value("SELECT 1") == 1

    # max_size=1 pool: this checks out the very same physical connection.
    async with client.bound_connection() as conn:
        assert conn.read_only is not True
        assert conn.read_only is None
        assert conn.isolation_level is None


@pytest.mark.asyncio
async def test_attributes_restored_when_tx_body_raises(
    pg_client_single_conn: PostgresClient,
    leak_table: str,
) -> None:
    """A read-only tx that raises mid-body still restores attributes (the finally)."""

    client = pg_client_single_conn

    class Boom(Exception):
        pass

    with pytest.raises(Boom):
        async with client.transaction(
            options=PostgresTransactionOptions(
                read_only=True,
                isolation="serializable",
            ),
        ):
            assert await client.fetch_value("SELECT 1") == 1
            raise Boom

    async with client.bound_connection() as conn:
        assert conn.read_only is None
        assert conn.isolation_level is None

    # And the same pooled connection accepts writes again.
    await client.execute(f"INSERT INTO {leak_table} (value) VALUES (1)")
    assert await client.fetch_value(f"SELECT count(*) FROM {leak_table}") == 1


@pytest.mark.asyncio
async def test_attributes_restored_when_tx_is_cancelled(
    pg_client_single_conn: PostgresClient,
    leak_table: str,
) -> None:
    """Cancellation inside a read-only tx body still restores attributes."""

    client = pg_client_single_conn
    inside_tx = asyncio.Event()

    async def body() -> None:
        async with client.transaction(
            options=PostgresTransactionOptions(
                read_only=True,
                isolation="serializable",
            ),
        ):
            assert await client.fetch_value("SELECT 1") == 1
            inside_tx.set()
            await asyncio.sleep(30)

    task = asyncio.create_task(body())
    await inside_tx.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    async with client.bound_connection() as conn:
        assert conn.read_only is None
        assert conn.isolation_level is None

    await client.execute(f"INSERT INTO {leak_table} (value) VALUES (1)")
    assert await client.fetch_value(f"SELECT count(*) FROM {leak_table}") == 1


@pytest.mark.asyncio
async def test_pool_reset_callback_clears_poisoned_attributes(
    pg_client_single_conn: PostgresClient,
    leak_table: str,
) -> None:
    """Second belt: even attributes set OUTSIDE transaction() are cleared on check-in.

    Poisons the raw connection directly (bypassing the client's finally-restore)
    and proves the pool ``reset=`` callback wipes the attributes when the
    connection returns to the pool.
    """

    client = pg_client_single_conn

    async with client.bound_connection() as conn:
        await conn.set_read_only(True)
        await conn.set_isolation_level(IsolationLevel.SERIALIZABLE)

    # Check-in ran the reset callback; the same connection comes back clean.
    async with client.bound_connection() as conn:
        assert conn.read_only is None
        assert conn.isolation_level is None

    await client.execute(f"INSERT INTO {leak_table} (value) VALUES (1)")
    async with client.transaction():
        level = await client.fetch_value("SHOW transaction_isolation")
        assert level == "read committed"
