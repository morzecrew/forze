"""``SerializeOperation`` on Postgres: the lock holds across sessions until the transaction ends.

Two pools, so the two invocations run on different connections, and the lock is the database's
``pg_advisory_xact_lock``, not the adapter's bookkeeping: one leg reads it out of ``pg_locks``.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import pytest
from pydantic import BaseModel
from testcontainers.postgres import PostgresContainer

from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.application.hooks.locking import SerializeOperation
from forze_postgres.execution.deps import PostgresDepsModule
from forze_postgres.kernel.client import PostgresConfig
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

# ----------------------- #

_OP = "orders.recompute"
_ROUTE = "postgres"

HOLD = 0.6
"""How long the first invocation keeps its transaction open once it holds the lock."""

LAG = 0.15
"""How long the second invocation waits before it starts, so the first has the lock."""


class _Args(BaseModel):
    order_id: str
    name: str


class _Recorder(Handler[_Args, None]):
    def __init__(self, order: list[str], *, fail: bool = False) -> None:
        self.order = order
        self.fail = fail

    async def __call__(self, args: _Args) -> None:
        self.order.append(f"{args.name}:start")

        if args.name == "first":
            await asyncio.sleep(HOLD)

        self.order.append(f"{args.name}:end")

        if self.fail and args.name == "first":
            raise RuntimeError("roll back")


def _registry(handler: Handler[Any, Any], *, query: bool = False) -> FrozenOperationRegistry:
    binder = OperationRegistry(handlers={_OP: lambda _ctx: handler}).bind(_OP)

    if query:
        binder = binder.as_query()

    return (
        binder.bind_tx()
        .set_route(_ROUTE)
        .before(SerializeOperation(key=lambda args: args.order_id).to_step())
        .finish(deep=True)
        .freeze()
    )


def _ctx(client: PostgresClient) -> ExecutionContext:
    return context_from_deps(PostgresDepsModule(client=client, tx={_ROUTE})())


@pytest.fixture
async def second_client(postgres_container: PostgresContainer) -> AsyncIterator[PostgresClient]:
    """A second pool, so the second invocation is a different connection — a different session."""

    url = postgres_container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
    client = PostgresClient()
    await client.initialize(dsn=url, config=PostgresConfig(min_size=1, max_size=3))

    yield client

    await client.close()


async def _run_both(
    pg_client: PostgresClient,
    second_client: PostgresClient,
    first_key: str,
    second_key: str,
    *,
    fail: bool = False,
    query: bool = False,
) -> list[str]:
    order: list[str] = []
    registry = _registry(_Recorder(order, fail=fail), query=query)
    first = registry.resolve(_OP, _ctx(pg_client))
    second = registry.resolve(_OP, _ctx(second_client))

    async def run_first() -> None:
        with contextlib.suppress(RuntimeError):
            await first(_Args(order_id=first_key, name="first"))

        order.append("first:closed")

    async def run_second() -> None:
        await asyncio.sleep(LAG)
        await second(_Args(order_id=second_key, name="second"))

    await asyncio.wait_for(asyncio.gather(run_first(), run_second()), timeout=15)

    return order


# ....................... #


class TestSerializesAcrossSessions:
    async def test_the_same_key_waits_until_the_first_commits(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        key = uuid4().hex
        order = await _run_both(pg_client, second_client, key, key)

        assert order.index("second:start") > order.index("first:closed"), order

    async def test_a_different_key_does_not_wait(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        order = await _run_both(pg_client, second_client, uuid4().hex, uuid4().hex)

        assert order.index("second:start") < order.index("first:end"), order

    async def test_a_rollback_releases_the_key(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        key = uuid4().hex
        order = await _run_both(pg_client, second_client, key, key, fail=True)

        assert order.index("second:start") > order.index("first:closed"), order
        assert order[-1] == "second:end"

    async def test_a_read_only_query_takes_it_too(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        # An advisory lock is no write, so a QUERY's READ ONLY transaction can hold it.
        key = uuid4().hex
        order = await _run_both(pg_client, second_client, key, key, query=True)

        assert order.index("second:start") > order.index("first:closed"), order


# ....................... #


class TestTheLockIsTheTransactions:
    async def test_pg_locks_shows_it_until_commit(self, pg_client: PostgresClient) -> None:
        ctx = _ctx(pg_client)
        key = 0x1234_5678_9ABC
        query = (
            "SELECT count(*) AS n FROM pg_locks WHERE locktype = 'advisory' AND objsubid = 1 "
            "AND granted AND classid::bigint = %s AND objid::bigint = %s"
        )
        parts = [key >> 32, key & 0xFFFF_FFFF]

        async def held() -> int:
            async with pg_client.detached():
                row = await pg_client.fetch_one(query, parts, row_factory="dict")

            assert row is not None
            return int(row["n"])

        async with ctx.tx_ctx.scope(_ROUTE):
            await ctx.tx_locks(_ROUTE).hold(key)

            assert await held() == 1

        assert await held() == 0
