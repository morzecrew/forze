"""Performance evidence for ``lazy_transaction``: pool pressure under contention.

A handler that does work *before* its first query (parsing, a CPU step, an
external call — here a small ``asyncio.sleep``) and then a single write models the
common case. With **eager** acquisition each handler holds a pooled connection for
the whole pre-query window, so on a small pool the handlers serialize on
connections. With **lazy** acquisition the connection is held only for the write,
so the pre-query windows overlap and throughput is bounded by the work, not the
pool.

This is the quantified counterpart to the integration discriminator test: on a
2-connection pool with 12 concurrent handlers each "computing" for 50 ms, lazy
finishes in roughly one compute window while eager needs ~6 serialized batches.
"""

import asyncio
import time
from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("psycopg")

from datetime import timedelta

from forze_postgres.kernel.client.client import PostgresClient, PostgresConfig

# ----------------------- #

_POOL_SIZE = 2
_CONCURRENCY = 12
_COMPUTE_S = 0.05
"""Pre-query work window per handler (simulated parse / CPU / external call)."""


def _dsn(container) -> str:
    url = container.get_connection_url()
    if url.startswith("postgresql+psycopg://"):
        url = url.replace("postgresql+psycopg://", "postgresql://")
    return url


async def _make_client(container, *, lazy: bool) -> PostgresClient:
    client = PostgresClient()
    await client.initialize(
        dsn=_dsn(container),
        config=PostgresConfig(
            min_size=_POOL_SIZE, max_size=_POOL_SIZE, lazy_transaction=lazy
        ),
        acquire_timeout=timedelta(seconds=30),
    )
    return client


@pytest_asyncio.fixture(scope="function")
async def perf_table_name(postgres_container) -> str:
    """A throwaway table seeded once via a short-lived eager client."""

    table = f"perf_lazy_{uuid4().hex[:12]}"
    client = await _make_client(postgres_container, lazy=False)
    try:
        await client.execute(
            f"CREATE TABLE {table} (id serial PRIMARY KEY, value integer NOT NULL)"
        )
    finally:
        await client.close()
    return table


async def _run_workload(client: PostgresClient, table: str) -> float:
    """Run ``_CONCURRENCY`` compute-then-write handlers; return elapsed seconds."""

    async def handler(i: int) -> None:
        async with client.transaction():
            await asyncio.sleep(_COMPUTE_S)  # pre-query work
            await client.execute(f"INSERT INTO {table} (value) VALUES ({i})")

    start = time.perf_counter()
    await asyncio.gather(*(handler(i) for i in range(_CONCURRENCY)))
    return time.perf_counter() - start


@pytest.mark.perf
@pytest.mark.asyncio
async def test_lazy_transaction_reduces_pool_pressure(
    postgres_container, perf_table_name: str
) -> None:
    """Lazy acquisition is substantially faster than eager under pool contention,
    because pre-query work no longer parks a connection."""

    eager = await _make_client(postgres_container, lazy=False)
    lazy = await _make_client(postgres_container, lazy=True)

    try:
        # Warm both pools so the first checkout isn't on the critical path.
        await eager.fetch_value("SELECT 1")
        await lazy.fetch_value("SELECT 1")

        eager_s = await _run_workload(eager, perf_table_name)
        lazy_s = await _run_workload(lazy, perf_table_name)
    finally:
        await eager.close()
        await lazy.close()

    speedup = eager_s / lazy_s if lazy_s > 0 else float("inf")
    print(
        f"\n[lazy-tx perf] pool={_POOL_SIZE} concurrency={_CONCURRENCY} "
        f"compute={_COMPUTE_S * 1000:.0f}ms  eager={eager_s * 1000:.1f}ms "
        f"lazy={lazy_s * 1000:.1f}ms  speedup={speedup:.2f}x"
    )

    # Eager serializes ~ceil(concurrency/pool) compute windows; lazy ~one window.
    # The real gap is ~5x; assert a conservative bound robust to CI noise.
    assert lazy_s < eager_s * 0.7
    # Eager must pay at least several serialized compute windows.
    assert eager_s >= _COMPUTE_S * (_CONCURRENCY / _POOL_SIZE) * 0.5
