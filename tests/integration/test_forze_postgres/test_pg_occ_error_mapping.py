"""Regression: live serialization conflicts must surface as CONCURRENCY.

The assembled error chain used to nest ``default_chain_exc_mapper``, which
made ``_psycopg_eh`` unreachable — a real ``SerializationFailure`` surfaced
as a generic INTERNAL "Unhandled exception" instead of CONCURRENCY, so
``occ_retry`` (which only retries CONCURRENCY) never retried serialization
conflicts. This test provokes a real conflict between two SERIALIZABLE
transactions and asserts the kind mapped by the live client.
"""

import asyncio
from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("psycopg")

from forze.base.exceptions import CoreException, ExceptionKind
from forze_postgres.kernel.client.client import (
    PostgresClient,
    PostgresConfig,
    PostgresTransactionOptions,
)

# ----------------------- #


def _dsn(postgres_container) -> str:
    url = postgres_container.get_connection_url()

    if url.startswith("postgresql+psycopg://"):
        url = url.replace("postgresql+psycopg://", "postgresql://")

    return url


# ....................... #


@pytest_asyncio.fixture(scope="function")
async def two_clients(postgres_container):
    """Two independent single-connection clients for concurrent transactions."""

    clients = []

    for _ in range(2):
        client = PostgresClient()
        await client.initialize(
            dsn=_dsn(postgres_container),
            config=PostgresConfig(min_size=1, max_size=1),
        )
        clients.append(client)

    yield clients

    for client in clients:
        await client.close()


# ....................... #


@pytest.mark.asyncio
async def test_serialization_conflict_maps_to_concurrency(two_clients) -> None:
    c1, c2 = two_clients
    table = f"occ_map_{uuid4().hex[:12]}"

    await c1.execute(f"CREATE TABLE {table} (id int PRIMARY KEY, v int NOT NULL)")
    await c1.execute(f"INSERT INTO {table} (id, v) VALUES (1, 0)")

    serializable = PostgresTransactionOptions(isolation="serializable")
    tx1_updated = asyncio.Event()
    tx2_snapshotted = asyncio.Event()

    async def tx1() -> None:
        async with c1.transaction(options=serializable):
            await c1.execute(f"UPDATE {table} SET v = v + 1 WHERE id = 1")
            tx1_updated.set()
            # Hold the row lock until tx2 has taken its snapshot and is
            # blocked on the same row, then commit (releasing the lock and
            # dooming tx2's update).
            await asyncio.wait_for(tx2_snapshotted.wait(), timeout=10)
            await asyncio.sleep(0.2)

    async def tx2() -> None:
        await asyncio.wait_for(tx1_updated.wait(), timeout=10)

        async with c2.transaction(options=serializable):
            # Take the snapshot before tx1 commits.
            await c2.fetch_one(f"SELECT v FROM {table} WHERE id = 1")
            tx2_snapshotted.set()
            # Blocks on tx1's row lock; once tx1 commits this fails with
            # "could not serialize access due to concurrent update".
            await c2.execute(f"UPDATE {table} SET v = v + 1 WHERE id = 1")

    results = await asyncio.gather(tx1(), tx2(), return_exceptions=True)

    errors = [r for r in results if isinstance(r, BaseException)]
    assert len(errors) == 1, f"expected exactly one failed tx, got: {results!r}"

    err = errors[0]
    assert isinstance(err, CoreException), f"unexpected error type: {err!r}"
    assert err.kind == ExceptionKind.CONCURRENCY
    assert err.code != "core.unhandled"
