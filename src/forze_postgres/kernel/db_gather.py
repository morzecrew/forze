"""Helpers for running multiple DB coroutines without starving the pool or overloading one connection."""

import asyncio
from typing import Awaitable, Callable, Sequence, TypeVar

from .platform import PostgresClientPort

# ----------------------- #

T = TypeVar("T")


async def gather_db_work(
    client: PostgresClientPort,
    makers: Sequence[Callable[[], Awaitable[T]]],
) -> list[T]:
    """Run *makers* with concurrency rules suited to the Postgres client port.

    * Inside a transaction (context-bound connection): strictly sequential —
      a single :class:`~psycopg.AsyncConnection` cannot serve concurrent queries.
    * Outside a transaction: up to :meth:`PostgresClientPort.query_concurrency_limit`
      tasks at a time to limit pool checkouts.

    Each callable must create a fresh awaitable when invoked (``makers`` are
    invoked lazily under the semaphore).
    """

    if not makers:
        return []

    if client.is_in_transaction():
        return [await m() for m in makers]

    limit = client.query_concurrency_limit()
    if limit <= 1:
        return [await m() for m in makers]

    sem = asyncio.Semaphore(limit)

    async def _bounded(m: Callable[[], Awaitable[T]]) -> T:
        async with sem:
            return await m()

    return list(await asyncio.gather(*(_bounded(m) for m in makers)))
