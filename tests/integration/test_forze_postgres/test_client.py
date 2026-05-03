import pytest

from forze.base.errors import InfrastructureError
from forze_postgres.kernel.platform.client import (
    PostgresClient,
    PostgresConfig,
    PostgresTransactionOptions,
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

    with pytest.raises(InfrastructureError, match="not initialized"):
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
    async with pg_client.bound_connection():
        async with pg_client.transaction(
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
        with pytest.raises(InfrastructureError, match="already bound"):
            async with pg_client.bound_connection():
                pass
