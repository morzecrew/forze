"""``SerializeOperation`` on Postgres: the lock holds across sessions until the transaction ends.

Two pools, so the two invocations run on different connections, and the lock is the database's
``pg_advisory_xact_lock``, not the adapter's bookkeeping: one leg reads it out of ``pg_locks``.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Callable
from typing import Any
from uuid import uuid4

import pytest
from pydantic import BaseModel
from testcontainers.postgres import PostgresContainer

from forze.application.contracts.execution import Handler
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.application.hooks.locking import SerializeOperation
from forze.application.hooks.resilience import ResilienceWrap
from forze.base.exceptions import exc
from forze.base.primitives import advisory_lock_key
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters import MockTxLockAdapter
from forze_postgres.adapters import PostgresTxLockAdapter
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
    def __init__(
        self, order: list[str], *, fail: bool = False, meet: asyncio.Event | None = None
    ) -> None:
        self.order = order
        self.fail = fail
        # With *meet*, the first holds until the second has started: a second that waits on
        # the first's key never starts, and the wait times out instead of passing on a margin.
        self.meet = meet

    async def __call__(self, args: _Args) -> None:
        self.order.append(f"{args.name}:start")

        if args.name == "second" and self.meet is not None:
            self.meet.set()

        if args.name == "first":
            if self.meet is None:
                await asyncio.sleep(HOLD)

            else:
                await asyncio.wait_for(self.meet.wait(), timeout=5)

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
    meet: bool = False,
) -> list[str]:
    order: list[str] = []
    recorder = _Recorder(order, fail=fail, meet=asyncio.Event() if meet else None)
    registry = _registry(recorder, query=query)
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


WAIT = 0.3
"""How long a contender is given to take a key it must not get, before it counts as waiting."""

_OUTER = "orders.settle"


class _Rollback(RuntimeError):
    """Raised inside a nested scope to roll it back."""


def _key() -> int:
    return uuid4().int >> 66


async def _held(client: PostgresClient, key: int, *, granted: bool = True) -> int:
    """How many sessions hold *key* as a granted advisory lock (or wait for it, with
    ``granted=False``), read outside any transaction."""

    async with client.detached():
        row = await client.fetch_one(
            "SELECT count(*) AS n FROM pg_locks WHERE locktype = 'advisory' AND objsubid = 1 "
            "AND granted = %s AND classid::bigint = %s AND objid::bigint = %s",
            [granted, (key >> 32) & 0xFFFF_FFFF, key & 0xFFFF_FFFF],
            row_factory="dict",
        )

    assert row is not None
    return int(row["n"])


async def _contend(ctx: ExecutionContext, key: int, taken: asyncio.Event) -> None:
    async with ctx.tx_ctx.scope(_ROUTE):
        await ctx.tx_locks(_ROUTE).hold(key)
        taken.set()


def _nested_registry(
    inner: Handler[Any, Any], outer: Callable[[ExecutionContext], Handler[Any, Any]]
) -> FrozenOperationRegistry:
    """:data:`_OP` serialized by its key, and :data:`_OUTER`, which calls it in its own
    transaction on the same route."""

    return (
        OperationRegistry(handlers={_OP: lambda _ctx: inner, _OUTER: outer})
        .bind(_OP)
        .bind_tx()
        .set_route(_ROUTE)
        .before(SerializeOperation(key=lambda args: args.order_id).to_step())
        .finish(deep=True)
        .bind(_OUTER)
        .bind_tx()
        .set_route(_ROUTE)
        .finish(deep=True)
        .freeze()
    )


# ....................... #


class TestSerializesAcrossSessions:
    # Ordered against the handler's own end, inside the transaction: the commit wakes the
    # waiter and the first task's next line in the same instant, in either order. That the
    # lock outlasts the handler until the root commits is pinned across sessions by the
    # nested-scope tests below (an operation nested in another holds its key until the outer
    # commits).

    async def test_the_same_key_waits_until_the_first_commits(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        key = uuid4().hex
        order = await _run_both(pg_client, second_client, key, key)

        assert order.index("second:start") > order.index("first:end"), order

    async def test_a_different_key_does_not_wait(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        order = await _run_both(pg_client, second_client, uuid4().hex, uuid4().hex, meet=True)

        assert order.index("second:start") < order.index("first:end"), order

    async def test_a_rollback_releases_the_key(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        key = uuid4().hex
        order = await _run_both(pg_client, second_client, key, key, fail=True)

        assert order.index("second:start") > order.index("first:end"), order
        assert order[-1] == "second:end"

    async def test_a_read_only_query_takes_it_too(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        # An advisory lock is no write, so a QUERY's READ ONLY transaction can hold it.
        key = uuid4().hex
        order = await _run_both(pg_client, second_client, key, key, query=True)

        assert order.index("second:start") > order.index("first:end"), order


# ....................... #


class TestTheLockIsTheTransactions:
    async def test_pg_locks_shows_it_until_commit(self, pg_client: PostgresClient) -> None:
        ctx = _ctx(pg_client)
        key = 0x1234_5678_9ABC

        async with ctx.tx_ctx.scope(_ROUTE):
            await ctx.tx_locks(_ROUTE).hold(key)

            assert await _held(pg_client, key) == 1

        assert await _held(pg_client, key) == 0

    async def test_a_hybrid_context_takes_the_lock_of_the_backend_that_runs_the_route(
        self, pg_client: PostgresClient
    ) -> None:
        # The mock composed behind Postgres as a fallback: the Postgres route's lock is the
        # Postgres one, and only a route the mock's own manager runs gets the mock's.
        ctx = context_from_deps(
            PostgresDepsModule(client=pg_client, tx={_ROUTE})(),
            MockDepsModule(state=MockState())(),
        )
        key = _key()

        assert isinstance(ctx.tx_locks(_ROUTE), PostgresTxLockAdapter)
        assert isinstance(ctx.tx_locks("mock-only"), MockTxLockAdapter)

        async with ctx.tx_ctx.scope(_ROUTE):
            await ctx.tx_locks(_ROUTE).hold(key)

            assert await _held(pg_client, key) == 1

    async def test_a_nested_scope_that_succeeds_keeps_it_until_the_root_commits(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        # RELEASE SAVEPOINT hands the lock to the enclosing transaction.
        ctx, key, taken = _ctx(pg_client), _key(), asyncio.Event()

        async with ctx.tx_ctx.scope(_ROUTE):
            async with ctx.tx_ctx.scope(_ROUTE):
                await ctx.tx_locks(_ROUTE).hold(key)

            task = asyncio.create_task(_contend(_ctx(second_client), key, taken))

            with pytest.raises(TimeoutError):
                await asyncio.wait_for(taken.wait(), timeout=WAIT)

        await asyncio.wait_for(task, timeout=5)

        assert taken.is_set()

    async def test_a_nested_scope_that_rolls_back_releases_it(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        # ROLLBACK TO SAVEPOINT releases the lock with the writes it guarded; the root goes on.
        ctx, key, taken = _ctx(pg_client), _key(), asyncio.Event()

        async with ctx.tx_ctx.scope(_ROUTE):
            with pytest.raises(_Rollback):
                async with ctx.tx_ctx.scope(_ROUTE):
                    await ctx.tx_locks(_ROUTE).hold(key)
                    raise _Rollback

            await asyncio.wait_for(_contend(_ctx(second_client), key, taken), timeout=5)

            assert taken.is_set()
            assert await _held(pg_client, key) == 0


# ....................... #


class TestNestedRetriedAndTenanted:
    async def test_an_operation_nested_in_another_holds_its_key_until_the_outer_commits(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        order: list[str] = []
        inner_done, release = asyncio.Event(), asyncio.Event()
        ref: dict[str, FrozenOperationRegistry] = {}

        class _Inner(Handler[_Args, None]):
            async def __call__(self, args: _Args) -> None:
                order.append(f"{args.name}:inner")

        class _Outer(Handler[_Args, None]):
            def __init__(self, ctx: ExecutionContext) -> None:
                self.ctx = ctx

            async def __call__(self, args: _Args) -> None:
                await ref["registry"].resolve(_OP, self.ctx)(args)
                inner_done.set()
                await release.wait()
                order.append("outer:end")

        ref["registry"] = _nested_registry(_Inner(), _Outer)
        registry = ref["registry"]
        key = uuid4().hex

        outer = asyncio.create_task(
            registry.resolve(_OUTER, _ctx(pg_client))(_Args(order_id=key, name="outer"))
        )
        await asyncio.wait_for(inner_done.wait(), timeout=5)
        contender = asyncio.create_task(
            registry.resolve(_OP, _ctx(second_client))(_Args(order_id=key, name="contender"))
        )
        await asyncio.sleep(WAIT)

        assert "contender:inner" not in order

        release.set()
        await asyncio.wait_for(asyncio.gather(outer, contender), timeout=5)

        assert order.index("contender:inner") > order.index("outer:end"), order

    async def test_a_retry_after_a_serialization_failure_takes_the_key_again(
        self, pg_client: PostgresClient
    ) -> None:
        key = uuid4().hex
        lock = advisory_lock_key("forze.operation", _OP, None, key)
        held_per_attempt: list[int] = []

        class _Flaky(Handler[_Args, None]):
            async def __call__(self, args: _Args) -> None:
                held_per_attempt.append(await _held(pg_client, lock))

                if len(held_per_attempt) == 1:
                    raise exc.concurrency("serialization failure", code="serialization_failure")

        registry = (
            OperationRegistry(handlers={_OP: lambda _ctx: _Flaky()})
            .bind(_OP)
            .bind_outer()
            .wrap(ResilienceWrap(policy="occ").to_step())
            .finish()
            .bind_tx()
            .set_route(_ROUTE)
            .before(SerializeOperation(key=lambda args: args.order_id).to_step())
            .finish(deep=True)
            .freeze()
        )

        await registry.resolve(_OP, _ctx(pg_client))(_Args(order_id=key, name="flaky"))

        # The first attempt's rollback gave it back; the second took it once, then committed.
        assert held_per_attempt == [1, 1]
        assert await _held(pg_client, lock) == 0

    async def test_two_tenants_with_one_key_do_not_wait(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        order: list[str] = []
        registry = _registry(_Recorder(order, meet=asyncio.Event()))
        key = uuid4().hex

        async def as_tenant(client: PostgresClient, name: str, lag: float) -> None:
            ctx = _ctx(client)
            await asyncio.sleep(lag)

            with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=uuid4())):
                await registry.resolve(_OP, ctx)(_Args(order_id=key, name=name))

        await asyncio.wait_for(
            asyncio.gather(
                as_tenant(pg_client, "first", 0), as_tenant(second_client, "second", LAG)
            ),
            timeout=15,
        )

        assert order.index("second:start") < order.index("first:end"), order


# ....................... #

_PEER = "orders.reprice"


async def _until_waiting(client: PostgresClient, key: int) -> None:
    while not await _held(client, key, granted=False):
        await asyncio.sleep(0.01)


class TestGroupAcrossSessions:
    async def test_two_operations_in_one_group_take_turns(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        order: list[str] = []
        started, release = asyncio.Event(), asyncio.Event()
        key = uuid4().hex

        class _Park(Handler[_Args, None]):
            async def __call__(self, args: _Args) -> None:
                order.append(f"{args.name}:start")

                if args.name == "first":
                    started.set()
                    await release.wait()

                order.append(f"{args.name}:end")

        registry = OperationRegistry(
            handlers={_OP: lambda _ctx: _Park(), _PEER: lambda _ctx: _Park()}
        )

        for op in (_OP, _PEER):
            registry = (
                registry.bind(op)
                .bind_tx()
                .set_route(_ROUTE)
                .before(
                    SerializeOperation(key=lambda args: args.order_id, group="orders").to_step()
                )
                .finish(deep=True)
            )

        frozen = registry.freeze()
        first = asyncio.create_task(
            frozen.resolve(_OP, _ctx(pg_client))(_Args(order_id=key, name="first"))
        )
        await asyncio.wait_for(started.wait(), timeout=5)
        second = asyncio.create_task(
            frozen.resolve(_PEER, _ctx(second_client))(_Args(order_id=key, name="second"))
        )

        # Rendezvous, not a margin: the other session is queued on the group's lock.
        lock = advisory_lock_key("forze.operation.group", "orders", None, key)
        await asyncio.wait_for(_until_waiting(pg_client, lock), timeout=5)

        assert "second:start" not in order

        release.set()
        await asyncio.wait_for(asyncio.gather(first, second), timeout=5)

        assert order == ["first:start", "first:end", "second:start", "second:end"]
