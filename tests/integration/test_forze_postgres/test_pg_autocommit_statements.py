"""Regression tests: out-of-transaction statements ride autocommit and leak nothing.

Out-of-transaction query methods (``execute`` / ``execute_many`` / ``fetch_one`` /
``fetch_all`` / ``fetch_value``) switch the pooled connection to autocommit for
the duration of the statement: psycopg skips the implicit ``BEGIN`` and the
client's explicit ``COMMIT``, so one logical operation costs exactly one server
statement instead of ``BEGIN``/statement/``COMMIT``. Two belts guarantee the
autocommit flag never survives the statement — a ``finally`` restore in
``PostgresClient._statement_conn`` and the pool ``reset=`` callback on check-in.

THE regression that matters: autocommit must never leak into transactional
work. A ``max_size=1`` pool guarantees every operation reuses the *same*
physical connection, so any leak surfaces deterministically.
"""

import pytest
import pytest_asyncio
from uuid import uuid4

pytest.importorskip("psycopg")

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
    leaked by a previous out-of-transaction statement is observed by the
    next operation.
    """

    url = postgres_container.get_connection_url()

    if url.startswith("postgresql+psycopg://"):
        url = url.replace("postgresql+psycopg://", "postgresql://")

    client = PostgresClient()
    await client.initialize(dsn=url, config=PostgresConfig(min_size=1, max_size=1))

    yield client

    await client.close()


@pytest_asyncio.fixture(scope="function")
async def ac_table(pg_client_single_conn: PostgresClient) -> str:
    """Throwaway table for write visibility checks around autocommit statements."""

    table = f"ac_stmt_{uuid4().hex[:12]}"
    await pg_client_single_conn.execute(
        f"CREATE TABLE {table} (id serial PRIMARY KEY, value integer NOT NULL)",
    )
    return table


# ....................... #


@pytest.mark.asyncio
async def test_out_of_tx_statements_each_run_in_their_own_transaction(
    pg_client_single_conn: PostgresClient,
) -> None:
    """Autocommit semantics: consecutive out-of-tx statements get distinct xids,
    while statements inside one transaction share a single xid."""

    client = pg_client_single_conn

    xid_a = await client.fetch_value("SELECT pg_current_xact_id()::text")
    xid_b = await client.fetch_value("SELECT pg_current_xact_id()::text")
    assert xid_a != xid_b

    async with client.transaction():
        xid_1 = await client.fetch_value("SELECT pg_current_xact_id()::text")
        xid_2 = await client.fetch_value("SELECT pg_current_xact_id()::text")
        assert xid_1 == xid_2


@pytest.mark.asyncio
async def test_out_of_tx_writes_commit_durably(
    pg_client_single_conn: PostgresClient,
    ac_table: str,
) -> None:
    """execute / execute_many / fetch_one(INSERT..RETURNING) all auto-commit."""

    client = pg_client_single_conn

    await client.execute(f"INSERT INTO {ac_table} (value) VALUES (1)")
    await client.execute_many(
        f"INSERT INTO {ac_table} (value) VALUES (%s)",
        [(2,), (3,)],
    )
    row = await client.fetch_one(
        f"INSERT INTO {ac_table} (value) VALUES (4) RETURNING value",
    )
    assert row == {"value": 4}

    rows = await client.fetch_all(f"SELECT value FROM {ac_table} ORDER BY value")
    assert [r["value"] for r in rows] == [1, 2, 3, 4]


@pytest.mark.asyncio
async def test_transaction_after_out_of_tx_statements_behaves_normally(
    pg_client_single_conn: PostgresClient,
    ac_table: str,
) -> None:
    """THE regression: after out-of-tx statements, a transaction on the SAME
    pooled connection still begins, commits, and rolls back correctly."""

    client = pg_client_single_conn

    # Warm the connection with out-of-tx statements of every flavor.
    await client.execute(f"INSERT INTO {ac_table} (value) VALUES (1)")
    assert await client.fetch_value("SELECT 1") == 1
    assert await client.fetch_one("SELECT 1 AS n") == {"n": 1}
    assert await client.fetch_all("SELECT 1 AS n") == [{"n": 1}]

    # (1) BEGIN works and writes commit.
    async with client.transaction():
        await client.execute(f"INSERT INTO {ac_table} (value) VALUES (2)")

    # (2) Writes roll back on error — would silently persist if autocommit
    #     leaked into the transactional path.
    class Boom(Exception):
        pass

    with pytest.raises(Boom):
        async with client.transaction():
            await client.execute(f"INSERT INTO {ac_table} (value) VALUES (99)")
            raise Boom

    rows = await client.fetch_all(f"SELECT value FROM {ac_table} ORDER BY value")
    assert [r["value"] for r in rows] == [1, 2]

    # (3) Transaction options still compose into BEGIN normally.
    async with client.transaction(
        options=PostgresTransactionOptions(isolation="serializable"),
    ):
        level = await client.fetch_value("SHOW transaction_isolation")
        assert level == "serializable"

    level = await client.fetch_value("SHOW transaction_isolation")
    assert level == "read committed"


@pytest.mark.asyncio
async def test_autocommit_never_survives_the_statement(
    pg_client_single_conn: PostgresClient,
) -> None:
    """Raw attribute check: the same pooled connection comes back with
    ``autocommit`` False (and transaction attributes untouched)."""

    client = pg_client_single_conn

    await client.execute("SELECT 1")
    assert await client.fetch_value("SELECT 1") == 1

    # max_size=1 pool: this checks out the very same physical connection.
    async with client.bound_connection() as conn:
        assert conn.autocommit is False
        assert conn.read_only is None
        assert conn.isolation_level is None


@pytest.mark.asyncio
async def test_statements_inside_transaction_do_not_touch_autocommit(
    pg_client_single_conn: PostgresClient,
    ac_table: str,
) -> None:
    """In-tx query methods run on the bound connection: no autocommit toggles
    (psycopg would raise ProgrammingError if set_autocommit ran mid-tx)."""

    client = pg_client_single_conn

    async with client.transaction():
        await client.execute(f"INSERT INTO {ac_table} (value) VALUES (1)")
        assert await client.fetch_value(f"SELECT count(*) FROM {ac_table}") == 1

    # The pooled connection comes back clean — no autocommit leaked into the tx.
    # (max_size=1: this is the very same physical connection.)
    async with client.bound_connection() as conn:
        assert conn.autocommit is False

    assert await client.fetch_value(f"SELECT count(*) FROM {ac_table}") == 1


@pytest.mark.asyncio
async def test_pool_reset_callback_clears_poisoned_autocommit(
    pg_client_single_conn: PostgresClient,
    ac_table: str,
) -> None:
    """Second belt: autocommit set OUTSIDE the statement helper (bypassing its
    finally-restore) is cleared by the pool ``reset=`` callback on check-in."""

    client = pg_client_single_conn

    async with client.bound_connection() as conn:
        await conn.set_autocommit(True)

    # Check-in ran the reset callback; the same connection comes back clean.
    async with client.bound_connection() as conn:
        assert conn.autocommit is False

    # And transactional work on it behaves normally (rollback verified).
    class Boom(Exception):
        pass

    with pytest.raises(Boom):
        async with client.transaction():
            await client.execute(f"INSERT INTO {ac_table} (value) VALUES (1)")
            raise Boom

    assert await client.fetch_value(f"SELECT count(*) FROM {ac_table}") == 0


@pytest.mark.asyncio
async def test_fetch_all_batched_still_runs_inside_a_transaction(
    pg_client_single_conn: PostgresClient,
    ac_table: str,
) -> None:
    """The named (server-side) cursor path must keep its wrapping transaction
    and stay out of autocommit mode."""

    client = pg_client_single_conn

    await client.execute_many(
        f"INSERT INTO {ac_table} (value) VALUES (%s)",
        [(i,) for i in range(10)],
    )

    seen: list[int] = []

    async for chunk in client.fetch_all_batched(
        f"SELECT value FROM {ac_table} ORDER BY value",
        batch_size=3,
    ):
        seen.extend(r["value"] for r in chunk)

    assert seen == list(range(10))

    async with client.bound_connection() as conn:
        assert conn.autocommit is False
