"""``SerializeOperation``: invocations sharing a key do not overlap, each until its transaction ends.

Run against the in-memory store, whose transaction-scoped locks are the per-owner write locks a
``SerializedBy`` write takes, so the deadlock refusal the store already has covers them too.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
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
)
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.operations import check_wiring
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.application.hooks.locking import SerializeOperation
from forze.base.exceptions import CoreException, ExceptionKind
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

    async def test_a_naive_datetime_is_validation(self) -> None:
        class _Noop(Handler[_Args, None]):
            async def __call__(self, args: _Args) -> None: ...

        registry = _registry(_Noop())

        with pytest.raises(CoreException) as caught:
            await registry.resolve(_OP, _ctx())(_Args(order_id=datetime(2026, 10, 29, 12, 0)))

        assert caught.value.kind is ExceptionKind.VALIDATION
        assert caught.value.code == "operation_lock_key_naive_datetime"

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

    def test_a_route_bound_to_another_backend_is_not_served_by_the_catch_all(self) -> None:
        # A route whose transaction manager is bound for it alone (as Mongo binds one) and no
        # lock port: the mock's catch-all lock port would lock a store it does not run on.
        deps = Deps.merge(
            MockDepsModule(state=MockState())(),
            Deps.routed({TransactionManagerDepKey: {_ROUTE: mock_strict_txmanager}}),
        )

        with pytest.raises(CoreException) as caught:
            _registry(_Gate()).resolve(_OP, context_from_deps(deps))

        assert caught.value.code == TX_LOCK_UNSUPPORTED_CODE

    def test_an_unbound_hook_refuses_to_build(self) -> None:
        with pytest.raises(CoreException) as caught:
            SerializeOperation(key=lambda args: args)(_ctx())

        assert caught.value.kind is ExceptionKind.CONFIGURATION
