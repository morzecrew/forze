"""Fencing after Redis loses its data, against a real store.

A Redis that loses its data restarts every fence counter, and a holder may have drawn a token
it never wrote. These tests run the recovery end to end: the counter is advanced to the
store's highest fence, the next holder draws a token equal to the unwritten one, and both
holders race the claim on one Postgres row. The store serializes the two ``UPDATE``\\ s, so
exactly one finds the fence strictly lower.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from forze_redis.adapters import RedisDistributedLockAdapter

psycopg = pytest.importorskip("psycopg")

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #

_CLAIM = (
    "UPDATE guarded SET fence = %(token)s, writer = %(writer)s "
    "WHERE key = %(key)s AND fence < %(token)s"
)
"""A token's first write: the stored fence must be strictly lower."""

_WRITE = (
    "UPDATE guarded SET fence = %(token)s, writer = %(writer)s "
    "WHERE key = %(key)s AND fence <= %(token)s"
)
"""Every later write under a claimed token: an equal fence is the holder's own."""


@pytest.fixture(scope="module")
def postgres_url(docker_available: None) -> Iterator[str]:
    with PostgresContainer(image="ghcr.io/morzecrew/postgres:18", driver="psycopg") as pg:
        yield pg.get_connection_url().replace("postgresql+psycopg://", "postgresql://")


@pytest_asyncio.fixture
async def holders(postgres_url: str) -> AsyncIterator[tuple[Any, Any]]:
    """Two autocommit connections, one per holder, so their claims race in the server."""

    first = await psycopg.AsyncConnection.connect(postgres_url, autocommit=True)
    second = await psycopg.AsyncConnection.connect(postgres_url, autocommit=True)

    try:
        await first.execute(
            "CREATE TABLE IF NOT EXISTS guarded "
            "(key text PRIMARY KEY, fence bigint NOT NULL, writer text NOT NULL)"
        )
        yield first, second

    finally:
        await first.close()
        await second.close()


async def _rows(conn: Any, sql: str, *, key: str, token: int, writer: str) -> int:
    cur = await conn.execute(sql, {"key": key, "token": token, "writer": writer})
    return cur.rowcount


async def _store_fence(conn: Any, key: str) -> int:
    cur = await conn.execute("SELECT fence FROM guarded WHERE key = %s", (key,))
    row = await cur.fetchone()
    return row[0]


async def _draw(lock: RedisDistributedLockAdapter, key: str, owner: str) -> int:
    acquired = await lock.acquire(key, owner)
    assert acquired is not None and acquired.token is not None
    return acquired.token


# ....................... #


async def test_an_advanced_counter_repeats_an_unwritten_token_and_one_claim_wins(
    redis_dlock: RedisDistributedLockAdapter,
    redis_container: RedisContainer,
    holders: tuple[Any, Any],
) -> None:
    stale_conn, fresh_conn = holders
    key = f"invoice:{uuid4().hex[:8]}"
    await stale_conn.execute(
        "INSERT INTO guarded (key, fence, writer) VALUES (%s, 50, 'holder-50')", (key,)
    )

    # Fifty terms wrote up to fence 50; the 51st holder drew its token and has not written.
    for n in range(50):
        await _draw(redis_dlock, key, f"holder-{n + 1}")
        assert await redis_dlock.release(key, f"holder-{n + 1}") is True

    assert await _draw(redis_dlock, key, "stale") == 51

    redis_container.get_client().flushall()

    # Without the advance the counter restarts below the store, which refuses it for ever.
    restarted = await _draw(redis_dlock, key, "too-low")
    assert restarted == 1
    assert await _rows(fresh_conn, _CLAIM, key=key, token=restarted, writer="too-low") == 0
    assert await redis_dlock.release(key, "too-low") is True

    stored = await _store_fence(fresh_conn, key)
    assert await redis_dlock.advance_fence(key, past=stored) == 50

    fresh = await _draw(redis_dlock, key, "fresh")
    assert fresh == 51  # the same token the stale holder drew and never wrote

    claims = await asyncio.gather(
        _rows(stale_conn, _CLAIM, key=key, token=51, writer="stale"),
        _rows(fresh_conn, _CLAIM, key=key, token=fresh, writer="fresh"),
    )
    assert sorted(claims) == [0, 1]

    winner_conn, winner = (stale_conn, "stale") if claims[0] == 1 else (fresh_conn, "fresh")
    loser_conn, loser = (fresh_conn, "fresh") if winner == "stale" else (stale_conn, "stale")

    # The winner keeps writing under its token; the loser's claim stays refused.
    assert await _rows(winner_conn, _WRITE, key=key, token=51, writer=winner) == 1
    assert await _rows(loser_conn, _CLAIM, key=key, token=51, writer=loser) == 0
    # A lower token is refused by both shapes.
    assert await _rows(loser_conn, _WRITE, key=key, token=50, writer=loser) == 0

    cur = await fresh_conn.execute("SELECT fence, writer FROM guarded WHERE key = %s", (key,))
    assert await cur.fetchone() == (51, winner)


async def test_past_zero_after_a_total_loss_still_lets_one_holder_claim(
    redis_dlock: RedisDistributedLockAdapter,
    redis_container: RedisContainer,
    holders: tuple[Any, Any],
) -> None:
    first_conn, second_conn = holders
    key = f"invoice:{uuid4().hex[:8]}"
    # The store holds the resource and no fence at all: nothing was ever written under a token.
    await first_conn.execute(
        "INSERT INTO guarded (key, fence, writer) VALUES (%s, 0, 'nobody')", (key,)
    )

    assert await _draw(redis_dlock, key, "first") == 1

    redis_container.get_client().flushall()

    assert await redis_dlock.advance_fence(key, past=await _store_fence(first_conn, key)) == 0
    assert await _draw(redis_dlock, key, "second") == 1

    claims = await asyncio.gather(
        _rows(first_conn, _CLAIM, key=key, token=1, writer="first"),
        _rows(second_conn, _CLAIM, key=key, token=1, writer="second"),
    )
    assert sorted(claims) == [0, 1]
    assert await _store_fence(first_conn, key) == 1
