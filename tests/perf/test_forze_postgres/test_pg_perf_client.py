"""Performance tests for PostgresClient."""

import pytest

pytest.importorskip("psycopg")

from forze_postgres.kernel.platform.client import PostgresClient

_PG_FETCH_LARGE_ROWS = 10_000
_PG_EXECUTE_MANY_LARGE = 2_000


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_execute_benchmark(async_benchmark, pg_client: PostgresClient) -> None:
    """Benchmark single execute statement."""

    async def run() -> None:
        await pg_client.execute("SELECT 1")

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_fetch_one_benchmark(async_benchmark, pg_client: PostgresClient) -> None:
    """Benchmark fetch_one."""
    await pg_client.execute(
        """
        CREATE TABLE IF NOT EXISTS perf_fetch_one (
            id serial PRIMARY KEY,
            name text
        );
        """
    )
    await pg_client.execute(
        "INSERT INTO perf_fetch_one (name) VALUES (%(name)s)",
        {"name": "bench"},
    )

    async def run() -> None:
        row = await pg_client.fetch_one("SELECT * FROM perf_fetch_one WHERE id = 1")
        assert row is not None

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_fetch_all_small_benchmark(
    async_benchmark, pg_client: PostgresClient
) -> None:
    """Benchmark fetch_all with a small result set (10 rows)."""
    await pg_client.execute(
        """
        CREATE TABLE IF NOT EXISTS perf_fetch_all (
            id serial PRIMARY KEY,
            val integer
        );
        """
    )
    for i in range(10):
        await pg_client.execute(
            "INSERT INTO perf_fetch_all (val) VALUES (%(v)s)",
            {"v": i},
        )

    async def run() -> None:
        rows = await pg_client.fetch_all("SELECT * FROM perf_fetch_all")
        assert len(rows) == 10

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_fetch_all_medium_benchmark(
    async_benchmark, pg_client: PostgresClient
) -> None:
    """Benchmark fetch_all with a medium result set (1000 rows)."""
    await pg_client.execute(
        """
        CREATE TABLE IF NOT EXISTS perf_fetch_medium (
            id serial PRIMARY KEY,
            val integer
        );
        """
    )
    await pg_client.execute_many(
        "INSERT INTO perf_fetch_medium (val) VALUES (%(v)s)",
        [{"v": i} for i in range(1000)],
    )

    async def run() -> None:
        rows = await pg_client.fetch_all("SELECT * FROM perf_fetch_medium")
        assert len(rows) == 1000

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_fetch_all_large_benchmark(
    async_benchmark, pg_client: PostgresClient
) -> None:
    """Benchmark fetch_all with a large result set (10k rows)."""
    await pg_client.execute(
        """
        CREATE TABLE IF NOT EXISTS perf_fetch_large (
            id serial PRIMARY KEY,
            val integer
        );
        """
    )
    await pg_client.execute("TRUNCATE perf_fetch_large")
    await pg_client.execute_many(
        "INSERT INTO perf_fetch_large (val) VALUES (%(v)s)",
        [{"v": i} for i in range(_PG_FETCH_LARGE_ROWS)],
    )

    async def run() -> None:
        rows = await pg_client.fetch_all("SELECT * FROM perf_fetch_large")
        assert len(rows) == _PG_FETCH_LARGE_ROWS

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_execute_many_benchmark(
    async_benchmark, pg_client: PostgresClient
) -> None:
    """Benchmark execute_many with 100 parameter sets."""
    await pg_client.execute(
        """
        CREATE TABLE IF NOT EXISTS perf_exec_many (
            id serial PRIMARY KEY,
            val integer
        );
        """
    )

    params = [{"v": i} for i in range(100)]

    async def run() -> None:
        await pg_client.execute("DELETE FROM perf_exec_many")
        await pg_client.execute_many(
            "INSERT INTO perf_exec_many (val) VALUES (%(v)s)",
            params,
        )

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_execute_many_large_benchmark(
    async_benchmark, pg_client: PostgresClient
) -> None:
    """Benchmark execute_many bulk insert (2k rows)."""
    await pg_client.execute(
        """
        CREATE TABLE IF NOT EXISTS perf_exec_many_large (
            id serial PRIMARY KEY,
            val integer
        );
        """
    )
    params = [{"v": i} for i in range(_PG_EXECUTE_MANY_LARGE)]

    async def run() -> None:
        await pg_client.execute("DELETE FROM perf_exec_many_large")
        await pg_client.execute_many(
            "INSERT INTO perf_exec_many_large (val) VALUES (%(v)s)",
            params,
        )

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_transaction_benchmark(
    async_benchmark, pg_client: PostgresClient
) -> None:
    """Benchmark transaction commit overhead."""
    await pg_client.execute(
        """
        CREATE TABLE IF NOT EXISTS perf_tx (
            id serial PRIMARY KEY,
            val integer
        );
        """
    )

    async def run() -> None:
        async with pg_client.transaction():
            await pg_client.execute(
                "INSERT INTO perf_tx (val) VALUES (1)",
            )

    await async_benchmark(run)
