"""Integration tests for PostgresTxManagerAdapter."""

import pytest

from forze_postgres.adapters import PostgresTxManagerAdapter, PostgresTxScopeKey
from forze_postgres.kernel.platform.client import PostgresClient


@pytest.fixture
def pg_txmanager(pg_client: PostgresClient) -> PostgresTxManagerAdapter:
    """Provide a PostgresTxManagerAdapter backed by the test Postgres client."""
    return PostgresTxManagerAdapter(client=pg_client)


@pytest.mark.asyncio
async def test_scope_key(pg_txmanager: PostgresTxManagerAdapter) -> None:
    """scope_key matches PostgresTxScopeKey."""
    assert pg_txmanager.scope_key == PostgresTxScopeKey
    assert pg_txmanager.scope_key.name == "postgres"


@pytest.mark.asyncio
async def test_transaction_commit(
    pg_client: PostgresClient, pg_txmanager: PostgresTxManagerAdapter
) -> None:
    """Transaction commits when block exits normally."""
    await pg_client.execute(
        """
        CREATE TABLE test_txmanager_commit (
            id serial PRIMARY KEY,
            value integer
        );
        """
    )

    async with pg_txmanager.transaction():
        await pg_client.execute(
            "INSERT INTO test_txmanager_commit (value) VALUES (%(val)s)",
            {"val": 42},
        )
        val = await pg_client.fetch_value("SELECT value FROM test_txmanager_commit")
        assert val == 42

    val_after = await pg_client.fetch_value("SELECT value FROM test_txmanager_commit")
    assert val_after == 42


@pytest.mark.asyncio
async def test_transaction_rollback(
    pg_client: PostgresClient, pg_txmanager: PostgresTxManagerAdapter
) -> None:
    """Transaction rolls back when block raises."""
    await pg_client.execute(
        """
        CREATE TABLE test_txmanager_rollback (
            id serial PRIMARY KEY,
            value integer
        );
        """
    )

    try:
        async with pg_txmanager.transaction():
            await pg_client.execute(
                "INSERT INTO test_txmanager_rollback (value) VALUES (%(val)s)",
                {"val": 99},
            )
            raise ValueError("rollback me")
    except ValueError:
        pass

    val_after = await pg_client.fetch_value("SELECT value FROM test_txmanager_rollback")
    assert val_after is None


@pytest.mark.asyncio
async def test_transaction_nested_savepoint(
    pg_client: PostgresClient, pg_txmanager: PostgresTxManagerAdapter
) -> None:
    """Nested transaction blocks use savepoints; inner rollback does not affect outer."""
    await pg_client.execute(
        """
        CREATE TABLE test_txmanager_nested (
            id serial PRIMARY KEY,
            value integer
        );
        """
    )

    async with pg_txmanager.transaction():
        await pg_client.execute(
            "INSERT INTO test_txmanager_nested (value) VALUES (%(val)s)",
            {"val": 1},
        )

        try:
            async with pg_txmanager.transaction():
                await pg_client.execute(
                    "INSERT INTO test_txmanager_nested (value) VALUES (%(val)s)",
                    {"val": 2},
                )
                raise ValueError("rollback inner")
        except ValueError:
            pass

        res = await pg_client.fetch_all(
            "SELECT value FROM test_txmanager_nested ORDER BY value"
        )
        assert len(res) == 1
        assert res[0]["value"] == 1

    res_after = await pg_client.fetch_all(
        "SELECT value FROM test_txmanager_nested ORDER BY value"
    )
    assert len(res_after) == 1
    assert res_after[0]["value"] == 1


@pytest.mark.asyncio
async def test_transaction_read_only(
    pg_client: PostgresClient,
) -> None:
    """Transaction with read_only option rejects writes."""
    await pg_client.execute(
        """
        CREATE TABLE test_txmanager_readonly (
            id serial PRIMARY KEY,
            value integer
        );
        """
    )

    txmanager = PostgresTxManagerAdapter(
        client=pg_client,
        options={"read_only": True},
    )

    with pytest.raises(Exception):  # psycopg errors.ReadOnlySqlTransaction
        async with txmanager.transaction():
            await pg_client.execute(
                "INSERT INTO test_txmanager_readonly (value) VALUES (%(val)s)",
                {"val": 1},
            )
