import asyncio
from datetime import timedelta
from time import monotonic
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

import pytest
from testcontainers.postgres import PostgresContainer

from forze.base.exceptions import CoreException, ExceptionKind
from forze_postgres.kernel.client.client import (
    PostgresClient,
    PostgresConfig,
    PostgresTransactionOptions,
)
from forze_postgres.kernel.client.errors import (
    DATABASE_UNREACHABLE_CODE,
    POOL_ACQUIRE_TIMEOUT_CODE,
)


@pytest.mark.asyncio
async def test_health_reports_ok(pg_client: PostgresClient) -> None:
    """health returns success against the live pool."""
    status, ok = await pg_client.health()
    assert status == "ok"
    assert ok is True

@pytest.mark.asyncio
async def test_execute_and_fetch(pg_client: PostgresClient) -> None:
    await pg_client.execute(
        """
        CREATE TABLE test_table (
            id serial PRIMARY KEY,
            name varchar
        );
        """
    )

    await pg_client.execute(
        "INSERT INTO test_table (name) VALUES (%(name)s)", {"name": "test_name"}
    )

    res = await pg_client.fetch_one("SELECT * FROM test_table")
    assert res is not None
    assert res["name"] == "test_name"

    all_res = await pg_client.fetch_all("SELECT * FROM test_table")
    assert len(all_res) == 1
    assert all_res[0]["name"] == "test_name"

    val = await pg_client.fetch_value("SELECT name FROM test_table")
    assert val == "test_name"

@pytest.mark.asyncio
async def test_transaction_commit(pg_client: PostgresClient) -> None:
    await pg_client.execute(
        """
        CREATE TABLE test_tx_commit (
            id serial PRIMARY KEY,
            value integer
        );
        """
    )

    async with pg_client.transaction():
        await pg_client.execute(
            "INSERT INTO test_tx_commit (value) VALUES (%(val)s)", {"val": 42}
        )

        # Verify inside transaction
        val = await pg_client.fetch_value("SELECT value FROM test_tx_commit")
        assert val == 42

    # Verify after commit
    val_after = await pg_client.fetch_value("SELECT value FROM test_tx_commit")
    assert val_after == 42

@pytest.mark.asyncio
async def test_transaction_rollback(pg_client: PostgresClient) -> None:
    await pg_client.execute(
        """
        CREATE TABLE test_tx_rollback (
            id serial PRIMARY KEY,
            value integer
        );
        """
    )

    try:
        async with pg_client.transaction():
            await pg_client.execute(
                "INSERT INTO test_tx_rollback (value) VALUES (%(val)s)", {"val": 99}
            )
            # simulate error to trigger rollback
            raise ValueError("rollback me")
    except ValueError:
        pass

    # Verify after rollback
    val_after = await pg_client.fetch_value("SELECT value FROM test_tx_rollback")
    assert val_after is None

@pytest.mark.asyncio
async def test_nested_transaction_savepoint(pg_client: PostgresClient) -> None:
    await pg_client.execute(
        """
        CREATE TABLE test_tx_nested (
            id serial PRIMARY KEY,
            value integer
        );
        """
    )

    async with pg_client.transaction():
        await pg_client.execute(
            "INSERT INTO test_tx_nested (value) VALUES (%(val)s)", {"val": 1}
        )

        try:
            # Nested transaction acts as a savepoint
            async with pg_client.transaction():
                await pg_client.execute(
                    "INSERT INTO test_tx_nested (value) VALUES (%(val)s)", {"val": 2}
                )
                raise ValueError("rollback inner")
        except ValueError:
            pass

        # Verify inner was rolled back but outer persists
        res = await pg_client.fetch_all(
            "SELECT value FROM test_tx_nested ORDER BY value"
        )
        assert len(res) == 1
        assert res[0]["value"] == 1

    # Verify outer transaction committed
    res_after = await pg_client.fetch_all(
        "SELECT value FROM test_tx_nested ORDER BY value"
    )
    assert len(res_after) == 1
    assert res_after[0]["value"] == 1

@pytest.mark.asyncio
async def test_initialize_is_idempotent(postgres_container) -> None:
    url = postgres_container.get_connection_url()
    if url.startswith("postgresql+psycopg://"):
        url = url.replace("postgresql+psycopg://", "postgresql://")

    client = PostgresClient()
    await client.initialize(dsn=url, config=PostgresConfig(min_size=1, max_size=3))
    await client.initialize(dsn=url, config=PostgresConfig(min_size=1, max_size=3))
    assert (await client.health())[1] is True
    await client.close()

@pytest.mark.asyncio
async def test_close_without_initialize_is_noop() -> None:
    client = PostgresClient()
    await client.close()

@pytest.mark.asyncio
async def test_health_without_initialize_reports_failure() -> None:
    client = PostgresClient()
    msg, ok = await client.health()
    assert ok is False
    assert msg

@pytest.mark.asyncio
async def test_query_after_close_raises(postgres_container) -> None:
    url = postgres_container.get_connection_url()
    if url.startswith("postgresql+psycopg://"):
        url = url.replace("postgresql+psycopg://", "postgresql://")

    client = PostgresClient()
    await client.initialize(dsn=url, config=PostgresConfig(min_size=1, max_size=3))
    await client.close()

    with pytest.raises(CoreException, match="not initialized"):
        await client.fetch_one("SELECT 1")

@pytest.mark.asyncio
async def test_execute_with_return_rowcount(pg_client: PostgresClient) -> None:
    await pg_client.execute(
        """
        CREATE TABLE test_rowcount (
            id serial PRIMARY KEY,
            v integer
        );
        """
    )
    n = await pg_client.execute(
        "INSERT INTO test_rowcount (v) VALUES (1), (2)",
        return_rowcount=True,
    )
    assert n == 2

@pytest.mark.asyncio
async def test_execute_many(pg_client: PostgresClient) -> None:
    await pg_client.execute(
        """
        CREATE TABLE test_exec_many (
            id serial PRIMARY KEY,
            code text NOT NULL
        );
        """
    )
    await pg_client.execute_many(
        "INSERT INTO test_exec_many (code) VALUES (%(c)s)",
        [{"c": "a"}, {"c": "b"}],
    )
    rows = await pg_client.fetch_all(
        "SELECT code FROM test_exec_many ORDER BY code",
    )
    assert [r["code"] for r in rows] == ["a", "b"]

@pytest.mark.asyncio
async def test_fetch_all_and_one_tuple_factory(pg_client: PostgresClient) -> None:
    one = await pg_client.fetch_one(
        "SELECT 1 AS n, 'x' AS s",
        row_factory="tuple",
    )
    assert one == (1, "x")

    many = await pg_client.fetch_all(
        "SELECT 1 AS n UNION ALL SELECT 2",
        row_factory="tuple",
    )
    assert many == [(1,), (2,)]

@pytest.mark.asyncio
async def test_fetch_one_none(pg_client: PostgresClient) -> None:
    row = await pg_client.fetch_one(
        "SELECT 1 WHERE false",
        row_factory="tuple",
    )
    assert row is None

@pytest.mark.asyncio
async def test_fetch_with_commit_flag_outside_transaction(
    pg_client: PostgresClient,
) -> None:
    """``commit=True`` on fetch methods is a no-op inside a transaction (guards still run)."""
    row = await pg_client.fetch_one("SELECT %(n)s AS x", {"n": 7}, commit=True)
    assert row is not None
    assert row["x"] == 7

    rows = await pg_client.fetch_all(
        "SELECT %(a)s AS x UNION ALL SELECT %(b)s",
        {"a": 1, "b": 2},
        commit=True,
    )
    assert len(rows) == 2

@pytest.mark.asyncio
async def test_nested_transaction_inner_success_releases_savepoint(
    pg_client: PostgresClient,
) -> None:
    await pg_client.execute(
        """
        CREATE TABLE test_tx_nested_ok (
            id serial PRIMARY KEY,
            value integer
        );
        """
    )

    async with pg_client.transaction():
        await pg_client.execute(
            "INSERT INTO test_tx_nested_ok (value) VALUES (%(v)s)",
            {"v": 1},
        )
        async with pg_client.transaction():
            await pg_client.execute(
                "INSERT INTO test_tx_nested_ok (value) VALUES (%(v)s)",
                {"v": 2},
            )

        res = await pg_client.fetch_all(
            "SELECT value FROM test_tx_nested_ok ORDER BY value",
        )
        assert [r["value"] for r in res] == [1, 2]

    outer = await pg_client.fetch_all(
        "SELECT value FROM test_tx_nested_ok ORDER BY value",
    )
    assert [r["value"] for r in outer] == [1, 2]

@pytest.mark.asyncio
async def test_transaction_read_only(pg_client: PostgresClient) -> None:
    async with pg_client.transaction(
        options=PostgresTransactionOptions(read_only=True)
    ):
        rows = await pg_client.fetch_all("SELECT 1 AS n")
        assert rows[0]["n"] == 1

@pytest.mark.asyncio
async def test_transaction_serializable_read_only(pg_client: PostgresClient) -> None:
    async with pg_client.transaction(
        options=PostgresTransactionOptions(
            isolation="serializable",
            read_only=True,
        ),
    ):
        rows = await pg_client.fetch_all("SELECT 1 AS n")
        assert rows[0]["n"] == 1

@pytest.mark.asyncio
async def test_transaction_on_bound_connection_serializable(
    pg_client: PostgresClient,
) -> None:
    """Top-level :meth:`transaction` on a pre-bound pool connection (UoW-style)."""
    async with pg_client.bound_connection(), pg_client.transaction(
        options=PostgresTransactionOptions(
            isolation="serializable",
            read_only=True,
        ),
    ):
        rows = await pg_client.fetch_all("SELECT 1 AS n")
        assert rows[0]["n"] == 1

@pytest.mark.asyncio
async def test_bound_connection_rejects_nested_bind(pg_client: PostgresClient) -> None:
    async with pg_client.bound_connection():
        with pytest.raises(CoreException, match="already bound"):
            async with pg_client.bound_connection():
                pass


@pytest.mark.asyncio
async def test_fetch_all_batched_streams_all_rows(pg_client: PostgresClient) -> None:
    """Server-side cursor streams every row across multiple FETCH FORWARD batches."""

    await pg_client.execute(
        """
        CREATE TABLE test_stream (id serial PRIMARY KEY, v integer NOT NULL);
        """
    )
    await pg_client.execute(
        "INSERT INTO test_stream (v) SELECT g FROM generate_series(1, 250) AS g",
    )

    batches: list[int] = []
    seen: list[int] = []

    async for chunk in pg_client.fetch_all_batched(
        "SELECT v FROM test_stream ORDER BY v",
        batch_size=100,
    ):
        batches.append(len(chunk))
        seen.extend(row["v"] for row in chunk)  # type: ignore[index]

    assert seen == list(range(1, 251))
    # 250 rows / batch 100 -> chunked, not one buffered slab.
    assert batches == [100, 100, 50]


@pytest.mark.asyncio
async def test_fetch_all_batched_reuses_context_transaction(
    pg_client: PostgresClient,
) -> None:
    """Streaming inside an open transaction declares the cursor in that transaction."""

    await pg_client.execute(
        "CREATE TABLE test_stream_tx (id serial PRIMARY KEY, v integer NOT NULL);"
    )

    async with pg_client.transaction():
        await pg_client.execute(
            "INSERT INTO test_stream_tx (v) SELECT g FROM generate_series(1, 30) AS g",
        )

        seen: list[int] = []
        async for chunk in pg_client.fetch_all_batched(
            "SELECT v FROM test_stream_tx ORDER BY v",
            batch_size=10,
        ):
            seen.extend(row["v"] for row in chunk)  # type: ignore[index]

        # Sees uncommitted rows written earlier in the same transaction.
        assert seen == list(range(1, 31))


@pytest.mark.asyncio
async def test_fetch_all_batched_early_break_leaves_conn_usable(
    pg_client: PostgresClient,
) -> None:
    """Abandoning the stream early closes the cursor; the connection stays usable."""

    await pg_client.execute(
        "CREATE TABLE test_stream_break (id serial PRIMARY KEY, v integer NOT NULL);"
    )
    await pg_client.execute(
        "INSERT INTO test_stream_break (v) SELECT g FROM generate_series(1, 500) AS g",
    )

    async for _chunk in pg_client.fetch_all_batched(
        "SELECT v FROM test_stream_break ORDER BY v",
        batch_size=50,
    ):
        break  # consume only the first batch, then abandon the generator

    # The pool connection is clean and immediately reusable.
    total = await pg_client.fetch_value("SELECT count(*) FROM test_stream_break")
    assert total == 500


@pytest.mark.asyncio
async def test_a_busy_pool_is_throttled_not_reported_as_unreachable(
    postgres_container,
) -> None:
    """The saturation half of the acquisition classifier, against a live server.

    Its counterpart lives in the unit tests, where nothing is listening. Both halves are
    worth having on the real thing: the whole classification rests on psycopg_pool leaving
    `connections_errors` alone while a pool is merely busy, and that is a claim about the
    library's bookkeeping rather than about our arithmetic.

    Three waiters rather than one, because the counter is pool-wide. If a saturated pool
    reached for connections behind a waiting caller, one attempt's failure would be read as
    every concurrent caller's cause. The stronger claim is pinned below: a pool at `max_size`
    cannot grow, so it makes no attempt at all and there is nothing to fail.
    """

    url = postgres_container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
    client = PostgresClient()
    await client.initialize(
        dsn=url,
        # One connection, so no other caller has anything to take.
        config=PostgresConfig(min_size=1, max_size=1),
        acquire_timeout=timedelta(milliseconds=300),
    )
    pool = client._PostgresClient__pool  # type: ignore[attr-defined]

    async def hold() -> None:
        async with client.transaction():
            await client.fetch_value("SELECT 1")
            await asyncio.sleep(1.0)

    async def wait_in_vain() -> CoreException:
        with pytest.raises(CoreException) as caught:
            await client.fetch_value("SELECT 2")

        return caught.value

    try:
        holder = asyncio.ensure_future(hold())
        await asyncio.sleep(0.1)  # the holder owns the only connection

        attempts_before = pool.get_stats()["connections_num"]
        failures = await asyncio.gather(*(wait_in_vain() for _ in range(3)))
        attempts_after = pool.get_stats()["connections_num"]

        await holder

    finally:
        await client.close()

    assert [failure.kind for failure in failures] == [ExceptionKind.THROTTLED] * 3
    assert [failure.code for failure in failures] == [POOL_ACQUIRE_TIMEOUT_CODE] * 3

    # Non-vacuous: the pool did open its `min_size` connection, and then opened nothing more
    # while three callers waited on it.
    assert attempts_before > 0
    assert attempts_after == attempts_before


@pytest.mark.asyncio
async def test_session_options_are_in_effect_on_pooled_connections(postgres_container) -> None:
    """Session options set by the pool's ``configure`` reach the connection, and survive the
    reset a commit's and a rollback's check-in run on it. psycopg_pool discards a connection
    ``configure`` leaves in a transaction, so before the fix no connection ever reached the
    pool."""

    url = postgres_container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
    client = PostgresClient()
    await client.initialize(
        dsn=url,
        # One connection, so the second read runs on the one the transaction checked back in.
        config=PostgresConfig(
            min_size=1,
            max_size=1,
            application_name="forze-configure",
            statement_timeout=timedelta(seconds=3),
            lock_timeout=timedelta(milliseconds=1500),
            idle_in_transaction_session_timeout=timedelta(minutes=1),
        ),
        acquire_timeout=timedelta(seconds=2),
    )

    query = (
        "SELECT pg_backend_pid() AS pid, "
        "current_setting('application_name') AS application_name, "
        "current_setting('statement_timeout') AS statement_timeout, "
        "current_setting('lock_timeout') AS lock_timeout, "
        "current_setting('idle_in_transaction_session_timeout') AS idle_timeout"
    )
    expected = {
        "application_name": "forze-configure",
        "statement_timeout": "3s",
        "lock_timeout": "1500ms",
        "idle_timeout": "1min",
    }

    try:
        first = await client.fetch_one(query)
        assert first is not None
        pid = first.pop("pid")
        assert first == expected

        async with client.transaction():
            await client.fetch_value("SELECT 1")

        after_commit = await client.fetch_one(query)

        with pytest.raises(RuntimeError):
            async with client.transaction():
                await client.fetch_value("SELECT 1")
                raise RuntimeError("roll back")

        after_rollback = await client.fetch_one(query)

    finally:
        await client.close()

    # The same server connection throughout: the checks ran on the one checked back in.
    assert after_commit == {"pid": pid, **expected}
    assert after_rollback == {"pid": pid, **expected}


# ....................... #
# Connections the server ended (issue #776)

_ENDED = 4
"""Idle pooled connections the server ends under each client below. Four, because without
the sweep psycopg_pool's backoff between failed checks (0s, ~1s, ~2s, ~4s) outlasts the
default acquire timeout before the fourth."""

_RECOVERY_BOUND = 1.0
"""Seconds the operations after a restart may take in all. Well under the 5s acquire timeout,
and under psycopg_pool's ~1s backoff after a second failed check, so a checkout that meets the
dead connections in turn rather than through one sweep fails it."""


def _dsn(container: Any) -> str:
    return container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")


async def _all_idle(client: PostgresClient, count: int) -> None:
    """Wait until *count* connections sit idle in the client's pool."""

    pool = client._PostgresClient__pool  # type: ignore[attr-defined]

    async with asyncio.timeout(10):
        while pool.get_stats().get("pool_available", 0) < count:
            await asyncio.sleep(0.05)


async def _client_whose_sessions_the_server_ended(
    url: str,
    admin: PostgresClient,
    **config: Any,
) -> PostgresClient:
    """A client whose idle pooled connections the server has terminated, as a restart would."""

    tag = f"forze-ended-{uuid4().hex[:8]}"
    client = PostgresClient()
    await client.initialize(
        dsn=url,
        config=PostgresConfig(min_size=_ENDED, max_size=_ENDED, application_name=tag, **config),
    )
    remaining = "SELECT count(*) FROM pg_stat_activity WHERE application_name = %s"

    # Every connection idle in the pool first, so each one is handed out dead.
    await _all_idle(client, _ENDED)

    async with asyncio.timeout(10):
        await admin.fetch_value(
            "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
            "WHERE application_name = %s",
            (tag,),
        )

        while await admin.fetch_value(remaining, (tag,)):
            await asyncio.sleep(0.05)

    return client


@pytest.mark.asyncio
async def test_a_checkout_skips_connections_the_server_ended(
    postgres_container, pg_client: PostgresClient
) -> None:
    """After a restart, the next operations succeed rather than one failing per dead connection.

    By default the pool checks a connection before handing it out, and one failed check sweeps
    the other idle connections, so the first checkout neither fails nor waits out
    psycopg_pool's backoff on each dead connection in turn.
    """

    client = await _client_whose_sessions_the_server_ended(_dsn(postgres_container), pg_client)

    try:
        started = monotonic()
        values = [await client.fetch_value("SELECT %s::int", (i,)) for i in range(_ENDED + 1)]
        elapsed = monotonic() - started

    finally:
        await client.close()

    assert values == list(range(_ENDED + 1))
    assert elapsed < _RECOVERY_BOUND, elapsed


@pytest.mark.asyncio
async def test_with_the_check_off_each_ended_connection_fails_one_operation(
    postgres_container, pg_client: PostgresClient
) -> None:
    """The opt-out keeps the old behaviour, which is also what makes the test above non-vacuous:
    the setup does hand out dead connections."""

    client = await _client_whose_sessions_the_server_ended(
        _dsn(postgres_container), pg_client, check_on_checkout=False
    )
    outcomes: list[str] = []

    try:
        for _ in range(_ENDED + 1):
            try:
                await client.fetch_value("SELECT 1")
                outcomes.append("ok")

            except CoreException:
                outcomes.append("failed")

    finally:
        await client.close()

    assert outcomes == ["failed"] * _ENDED + ["ok"]


@pytest.mark.asyncio
async def test_a_server_that_takes_no_new_sessions_fails_the_checkout_in_its_timeout(
    postgres_container, pg_client: PostgresClient
) -> None:
    """Every pooled session ended and none accepted since: the caller gets the mapped
    unreachable error at about its acquire timeout, not a hang. ``DROP DATABASE … WITH
    (FORCE)`` does both to the clients of that database."""

    database = f"forze_gone_{uuid4().hex[:8]}"
    await pg_client.execute(f'CREATE DATABASE "{database}"')
    acquire = timedelta(seconds=1)
    client = PostgresClient()
    await client.initialize(
        dsn=urlsplit(_dsn(postgres_container))._replace(path=f"/{database}").geturl(),
        config=PostgresConfig(min_size=2, max_size=2),
        acquire_timeout=acquire,
    )

    try:
        await _all_idle(client, 2)
        await pg_client.execute(f'DROP DATABASE "{database}" WITH (FORCE)')

        started = monotonic()

        with pytest.raises(CoreException) as caught:
            await asyncio.wait_for(client.fetch_value("SELECT 1"), 15)

        elapsed = monotonic() - started

    finally:
        await client.close()

    assert caught.value.code == DATABASE_UNREACHABLE_CODE
    assert elapsed < acquire.total_seconds() + 1, elapsed


@pytest.mark.asyncio
async def test_a_server_that_stops_answering_fails_the_checkout_in_its_timeout() -> None:
    """A ping nobody answers is bounded too. A cancelled one is not: psycopg answers the
    cancellation by waiting for the query again, so without the bound this checkout would wait
    until the server answers. Its own server, because pausing it stops every session on it."""

    acquire = timedelta(seconds=1)

    with PostgresContainer(image="ghcr.io/morzecrew/postgres:18", driver="psycopg") as postgres:
        server = postgres.get_wrapped_container()
        client = PostgresClient()
        await client.initialize(
            dsn=_dsn(postgres),
            config=PostgresConfig(min_size=2, max_size=2),
            acquire_timeout=acquire,
        )

        try:
            await _all_idle(client, 2)
            server.pause()

            try:
                started = monotonic()

                with pytest.raises(CoreException) as caught:
                    await asyncio.wait_for(client.fetch_value("SELECT 1"), 15)

                elapsed = monotonic() - started

            finally:
                server.unpause()

            # The sweep that failure started finishes once the server answers again.
            recovered = await asyncio.wait_for(client.fetch_value("SELECT 1"), 15)

        finally:
            await client.close()

    assert caught.value.code == POOL_ACQUIRE_TIMEOUT_CODE
    assert elapsed < acquire.total_seconds() + 1, elapsed
    assert recovered == 1
