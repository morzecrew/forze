import pytest

from forze_postgres.kernel.platform.client import PostgresClient


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
