"""``SerializeOperation``: invocations sharing a key do not overlap, each until its transaction ends.

Run against the in-memory store, whose transaction-scoped locks are the per-owner write locks a
``SerializedBy`` write takes, so the deadlock refusal the store already has covers them too.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta, timezone
from enum import StrEnum
from typing import Any, Literal
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.contracts.transaction import (
    TX_LOCK_UNSUPPORTED_CODE,
    TransactionManagerDepKey,
    TransactionScopeKey,
)
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.operations import check_wiring
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.application.hooks.locking import SerializeOperation
from forze.application.hooks.resilience import ResilienceWrap
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import str_key_selector
from forze_mock import MockDepsModule, MockState
from forze_mock.execution.factories import mock_strict_txmanager
from tests.support.execution_context import context_from_deps, context_from_modules

# ----------------------- #

_OP = "orders.recompute"
_ROUTE = "tx"


class _Args(BaseModel):
    order_id: Any


class _Named(_Args):
    name: str


class _Gate(Handler[_Named, None]):
    """Records each invocation's start and end, and parks it until its own gate opens."""

    def __init__(self) -> None:
        self.events: list[str] = []
        self.started: dict[str, asyncio.Event] = {}
        self.release: dict[str, asyncio.Event] = {}

    def gate(self, name: str) -> None:
        self.started[name] = asyncio.Event()
        self.release[name] = asyncio.Event()

    async def __call__(self, args: _Named) -> None:
        self.events.append(f"{args.name}:start")
        self.started[args.name].set()
        await self.release[args.name].wait()
        self.events.append(f"{args.name}:end")


class _ForeignTxManager:
    """Another backend's transaction manager, with no transaction-scoped lock (as Mongo's)."""

    @property
    def scope_key(self) -> TransactionScopeKey:
        return TransactionScopeKey("foreign")

    @asynccontextmanager
    async def transaction(self, *, read_only: bool = False) -> AsyncIterator[None]:
        yield


def _registry(
    handler: Handler[Any, Any],
    key: Callable[[Any], object] = lambda args: args.order_id,
) -> FrozenOperationRegistry:
    return (
        OperationRegistry(handlers={_OP: lambda _ctx: handler})
        .bind(_OP)
        .bind_tx()
        .set_route(_ROUTE)
        .before(SerializeOperation(key=key).to_step())
        .finish(deep=True)
        .freeze()
    )


def _ctx(transactions: Literal["journal", "strict", "none"] = "journal") -> ExecutionContext:
    return context_from_modules(MockDepsModule(state=MockState(), transactions=transactions))


async def _run_pair(
    ctx: ExecutionContext,
    registry: FrozenOperationRegistry,
    gate: _Gate,
    first: _Named,
    second: _Named,
) -> bool:
    """Start *first*, let it take its lock, start *second*; whether *second* entered while
    *first* was still running."""

    gate.gate(first.name)
    gate.gate(second.name)
    run = registry.resolve(_OP, ctx)

    a = asyncio.create_task(run(first))
    await asyncio.wait_for(gate.started[first.name].wait(), timeout=2)

    b = asyncio.create_task(run(second))

    try:
        await asyncio.wait_for(gate.started[second.name].wait(), timeout=0.2)
        overlapped = True

    except TimeoutError:
        overlapped = False

    gate.release[first.name].set()
    gate.release[second.name].set()
    await asyncio.wait_for(asyncio.gather(a, b), timeout=2)

    return overlapped


# ....................... #


class TestSerializes:
    async def test_the_same_key_waits_until_the_first_commits(self) -> None:
        gate = _Gate()
        order = uuid4()

        overlapped = await _run_pair(
            _ctx(),
            _registry(gate),
            gate,
            _Named(order_id=order, name="a"),
            _Named(order_id=order, name="b"),
        )

        assert not overlapped
        assert gate.events == ["a:start", "a:end", "b:start", "b:end"]

    @pytest.mark.parametrize("transactions", ["journal", "strict"])
    async def test_the_root_transaction_holds_the_key_until_it_ends(
        self, transactions: Literal["journal", "strict"]
    ) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state, transactions=transactions))

        async with ctx.tx_ctx.scope(_ROUTE):
            async with ctx.tx_ctx.scope(_ROUTE):
                await ctx.tx_locks(_ROUTE).hold(7)

            # A nested scope ending is no transaction ending.
            assert state.write_serialization.holder_of(7) is not None

        assert state.write_serialization.held() == ()

    async def test_different_keys_run_at_once(self) -> None:
        gate = _Gate()

        overlapped = await _run_pair(
            _ctx(),
            _registry(gate),
            gate,
            _Named(order_id=uuid4(), name="a"),
            _Named(order_id=uuid4(), name="b"),
        )

        assert overlapped

    async def test_different_tenants_run_at_once(self) -> None:
        gate = _Gate()
        ctx = _ctx()
        registry = _registry(gate)
        order = uuid4()
        gate.gate("a")
        gate.gate("b")

        async def as_tenant(args: _Named) -> None:
            with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=uuid4())):
                await registry.resolve(_OP, ctx)(args)

        a = asyncio.create_task(as_tenant(_Named(order_id=order, name="a")))
        await asyncio.wait_for(gate.started["a"].wait(), timeout=2)
        b = asyncio.create_task(as_tenant(_Named(order_id=order, name="b")))

        await asyncio.wait_for(gate.started["b"].wait(), timeout=2)
        gate.release["a"].set()
        gate.release["b"].set()
        await asyncio.gather(a, b)

    async def test_a_rollback_releases_the_key(self) -> None:
        ctx = _ctx()
        calls: list[str] = []

        class _Failing(Handler[_Named, None]):
            async def __call__(self, args: _Named) -> None:
                calls.append(args.name)

                if args.name == "a":
                    raise RuntimeError("boom")

        registry = _registry(_Failing())
        run = registry.resolve(_OP, ctx)

        with pytest.raises(RuntimeError):
            await run(_Named(order_id=1, name="a"))

        await asyncio.wait_for(run(_Named(order_id=1, name="b")), timeout=2)

        assert calls == ["a", "b"]

    async def test_an_instant_locks_alike_at_any_offset(self) -> None:
        gate = _Gate()
        at = datetime(2026, 10, 29, 12, 0, tzinfo=UTC)

        overlapped = await _run_pair(
            _ctx(),
            _registry(gate),
            gate,
            _Named(order_id=at, name="a"),
            _Named(order_id=at.astimezone(timezone(timedelta(hours=3))), name="b"),
        )

        assert not overlapped

    async def test_a_tuple_key_locks_on_every_part(self) -> None:
        gate = _Gate()
        registry = _registry(gate, key=lambda args: (args.order_id, args.name))

        overlapped = await _run_pair(
            _ctx(), registry, gate, _Named(order_id=1, name="a"), _Named(order_id=1, name="b")
        )

        assert overlapped


# ....................... #

_OUTER = "orders.settle"


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


async def _nested_then_contended(*, inner_fails: bool) -> list[str]:
    """Run :data:`_OP` nested in :data:`_OUTER`, then contend for its key from another session
    while the outer transaction is still open; the order things happened in."""

    state = MockState()
    ctx = context_from_modules(MockDepsModule(state=state))
    other = context_from_modules(MockDepsModule(state=state))
    events: list[str] = []
    inner_done, release = asyncio.Event(), asyncio.Event()
    ref: dict[str, FrozenOperationRegistry] = {}

    class _Inner(Handler[_Named, None]):
        async def __call__(self, args: _Named) -> None:
            events.append(f"{args.name}:inner")

            if inner_fails and args.name == "outer":
                raise RuntimeError("nested failure")

    class _Outer(Handler[_Named, None]):
        def __init__(self, ctx: ExecutionContext) -> None:
            self.ctx = ctx

        async def __call__(self, args: _Named) -> None:
            try:
                await ref["registry"].resolve(_OP, self.ctx)(args)

            except RuntimeError:
                events.append("outer:caught")

            inner_done.set()
            await release.wait()
            events.append("outer:end")

    ref["registry"] = registry = _nested_registry(_Inner(), _Outer)

    outer = asyncio.create_task(registry.resolve(_OUTER, ctx)(_Named(order_id=1, name="outer")))
    await asyncio.wait_for(inner_done.wait(), timeout=2)
    contender = asyncio.create_task(
        registry.resolve(_OP, other)(_Named(order_id=1, name="contender"))
    )
    await asyncio.sleep(0.2)
    release.set()
    await asyncio.wait_for(asyncio.gather(outer, contender), timeout=2)

    assert state.write_serialization.held() == ()

    return events


class TestNestedAndRetried:
    async def test_an_operation_nested_in_another_holds_its_key_until_the_outer_commits(
        self,
    ) -> None:
        events = await _nested_then_contended(inner_fails=False)

        assert events.index("contender:inner") > events.index("outer:end"), events

    async def test_a_nested_operation_that_fails_keeps_its_key_until_the_root_ends(
        self,
    ) -> None:
        # Stricter than Postgres, which releases it with the savepoint: the mock models none.
        events = await _nested_then_contended(inner_fails=True)

        assert "outer:caught" in events
        assert events.index("contender:inner") > events.index("outer:end"), events

    async def test_a_retry_after_a_serialization_failure_takes_the_key_again(self) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state, resilience="real"))
        held_per_attempt: list[int] = []

        class _Flaky(Handler[_Args, None]):
            async def __call__(self, args: _Args) -> None:
                held_per_attempt.append(len(state.write_serialization.held()))

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

        await registry.resolve(_OP, ctx)(_Args(order_id=1))

        # The first attempt's rollback gave it back; the second took it once, then committed.
        assert held_per_attempt == [1, 1]
        assert state.write_serialization.held() == ()


# ....................... #


class _Region(StrEnum):
    EU = "eu"


class TestRefusesAKeyThatCannotBeDerived:
    @pytest.mark.parametrize(
        "value",
        [1.5, True, _Region.EU, [1, 2], None, b"raw"],
        ids=["float", "bool", "enum", "list", "none", "bytes"],
    )
    async def test_an_unsupported_type_is_configuration(self, value: object) -> None:
        class _Noop(Handler[_Args, None]):
            async def __call__(self, args: _Args) -> None: ...

        registry = _registry(_Noop(), key=lambda _args: value)

        with pytest.raises(CoreException) as caught:
            await registry.resolve(_OP, _ctx())(_Args(order_id=1))

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert caught.value.code == "operation_lock_key_unsupported"
        # Known only per call, so the refusal names the operation it came from.
        assert repr(_OP) in caught.value.summary

    async def test_a_naive_datetime_is_validation(self) -> None:
        class _Noop(Handler[_Args, None]):
            async def __call__(self, args: _Args) -> None: ...

        registry = _registry(_Noop())

        with pytest.raises(CoreException) as caught:
            await registry.resolve(_OP, _ctx())(_Args(order_id=datetime(2026, 10, 29, 12, 0)))

        assert caught.value.kind is ExceptionKind.VALIDATION
        assert caught.value.code == "operation_lock_key_naive_datetime"
        assert repr(_OP) in caught.value.summary

    @pytest.mark.parametrize(
        "value",
        ["key", 7, UUID(int=7), datetime(2026, 1, 1, tzinfo=UTC), ()],
        ids=["str", "int", "uuid", "aware", "empty"],
    )
    async def test_a_supported_key_runs(self, value: object) -> None:
        calls: list[object] = []

        class _Record(Handler[_Args, None]):
            async def __call__(self, args: _Args) -> None:
                calls.append(args.order_id)

        await _registry(_Record(), key=lambda _args: value).resolve(_OP, _ctx())(_Args(order_id=1))

        assert calls == [1]


# ....................... #


class TestRefusesWhereNoTransactionCanHoldIt:
    def test_a_lock_in_the_outer_scope_is_refused_at_freeze(self) -> None:
        registry = (
            OperationRegistry(handlers={_OP: lambda _ctx: None})
            .bind(_OP)
            .bind_tx()
            .set_route(_ROUTE)
            .finish()
            .bind_outer()
            .before(SerializeOperation(key=lambda args: args.order_id).to_step())
            .finish(deep=True)
        )

        with pytest.raises(CoreException) as caught:
            registry.freeze()

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert caught.value.code == "tx_lock_outside_transaction_scope"

    def test_a_lock_without_a_route_is_refused_at_freeze(self) -> None:
        registry = (
            OperationRegistry(handlers={_OP: lambda _ctx: None})
            .bind(_OP)
            .bind_tx()
            .before(SerializeOperation(key=lambda args: args.order_id).to_step())
            .finish(deep=True)
        )

        with pytest.raises(CoreException) as caught:
            registry.freeze()

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert caught.value.code == "tx_lock_without_route"

    def test_a_lock_a_patch_adds_is_checked_as_well(self) -> None:
        registry = (
            OperationRegistry(handlers={_OP: lambda _ctx: None})
            .patch(str_key_selector.prefix("orders."))
            .bind_tx()
            .before(SerializeOperation(key=lambda args: args.order_id).to_step())
            .finish(deep=True)
        )

        with pytest.raises(CoreException) as caught:
            registry.freeze()

        assert caught.value.code == "tx_lock_without_route"

    def test_a_backend_without_transactions_is_refused_at_resolve(self) -> None:
        registry = _registry(_Gate())

        with pytest.raises(CoreException) as caught:
            registry.resolve(_OP, _ctx("none"))

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert caught.value.code == TX_LOCK_UNSUPPORTED_CODE

        report = check_wiring(registry, lambda: _ctx("none"))

        assert [failure.code for failure in report.failures] == [TX_LOCK_UNSUPPORTED_CODE]

    @pytest.mark.parametrize("bound", ["catch-all", "routed"])
    def test_a_manager_without_a_lock_is_refused_whatever_else_is_wired(self, bound: str) -> None:
        # Another backend's manager runs the route, the mock composed behind it as a fallback.
        # The mock's locks would guard a store that transaction never touches, so the lock is
        # asked of the manager that runs the route, and that one has none.
        factory = {TransactionManagerDepKey: lambda _ctx: _ForeignTxManager()}
        foreign = (
            Deps.plain(factory)
            if bound == "catch-all"
            else Deps.routed(
                {TransactionManagerDepKey: {_ROUTE: factory[TransactionManagerDepKey]}}
            )
        )
        ctx = context_from_deps(foreign, MockDepsModule(state=MockState())())

        with pytest.raises(CoreException) as caught:
            _registry(_Gate()).resolve(_OP, ctx)

        assert caught.value.code == TX_LOCK_UNSUPPORTED_CODE

    async def test_the_lock_comes_from_the_manager_that_runs_the_route(self) -> None:
        # A strict manager bound for the route over the journal catch-all: the lock it hands
        # out is held by the strict root, so it is gone once that root ends.
        state = MockState()
        ctx = context_from_deps(
            MockDepsModule(state=state)(),
            Deps.routed({TransactionManagerDepKey: {_ROUTE: mock_strict_txmanager}}),
        )

        async with ctx.tx_ctx.scope(_ROUTE):
            await ctx.tx_locks(_ROUTE).hold(5)

            assert state.write_serialization.holder_of(5) is not None

        assert state.write_serialization.held() == ()

    def test_an_unbound_hook_refuses_to_build(self) -> None:
        with pytest.raises(CoreException) as caught:
            SerializeOperation(key=lambda args: args)(_ctx())

        assert caught.value.kind is ExceptionKind.CONFIGURATION
