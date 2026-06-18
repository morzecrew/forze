"""Crash & restart (WS4) — a process crash mid-operation, healed by a restart over the store.

``CrashInterceptor`` raises a ``SimulatedCrash`` (a :class:`BaseException`) at a matched port
boundary, modeling the process dying mid-I/O. Because it is not an ``Exception``, it bypasses
the handler's own ``except Exception`` — the operation gets no inline recovery. The in-flight
transaction rolls back (the store's crash recovery), so committed state stays consistent; the
operation's own uncommitted work is gone until a **restart** (a fresh context over the same
persisted :class:`MockState`) re-drives it.
"""

from __future__ import annotations

import asyncio
from random import Random
from uuid import UUID

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.application.execution.interception import PortCall, PortInterceptor
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import (
    BaseDTO,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)
from forze_dst import CrashInterceptor, SimulatedCrash
from forze_mock import MockDepsModule
from forze_mock.state import MockState

# ----------------------- #
# Domain — orders and their items, through the document port.


class Order(Document):
    item_count: int = 0


class OrderCreate(CreateDocumentCmd):
    item_count: int = 0


class OrderUpdate(BaseDTO):
    item_count: int | None = None


class OrderRead(ReadDocument):
    item_count: int


class Item(Document):
    order_id: UUID


class ItemCreate(CreateDocumentCmd):
    order_id: UUID


class ItemRead(ReadDocument):
    order_id: UUID


ORDER_SPEC = DocumentSpec(
    name="orders",
    read=OrderRead,
    write=DocumentWriteTypes(domain=Order, create_cmd=OrderCreate, update_cmd=OrderUpdate),
)
ITEM_SPEC = DocumentSpec(
    name="items",
    read=ItemRead,
    write=DocumentWriteTypes(domain=Item, create_cmd=ItemCreate),
)


class AddItem(BaseModel):
    order_id: UUID


# ....................... #


@attrs.define(slots=True, kw_only=True)
class _CreateOrder(Handler[None, UUID]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> UUID:
        order = await self.ctx.document.command(ORDER_SPEC).create(OrderCreate())
        return order.id


@attrs.define(slots=True, kw_only=True)
class _AddItem(Handler[AddItem, None]):
    ctx: ExecutionContext

    async def __call__(self, args: AddItem) -> None:
        order = await self.ctx.document.query(ORDER_SPEC).get(args.order_id)

        try:
            # Two writes in one transaction: the item, then the order's running count. A crash
            # on the second rolls both back. The ``except Exception`` proves a crash is *not*
            # an Exception — it propagates past this compensation path.
            await self.ctx.document.command(ITEM_SPEC).create(
                ItemCreate(order_id=args.order_id)
            )
            await self.ctx.document.command(ORDER_SPEC).update(
                args.order_id, order.rev, OrderUpdate(item_count=order.item_count + 1)
            )

        except Exception:  # pragma: no cover - never runs for a BaseException crash
            return


_TX_PLAN = OperationPlan().bind_tx().set_route("mock").finish(deep=False)


def _registry() -> OperationRegistry:
    handlers = {
        "create_order": lambda ctx: _CreateOrder(ctx=ctx),
        "add_item": lambda ctx: _AddItem(ctx=ctx),
    }
    return OperationRegistry(
        handlers=handlers,
        plans={op: _TX_PLAN for op in handlers},
        descriptors={
            "create_order": OperationDescriptor(
                input_type=None, output_type=None, description="x"
            ),
            "add_item": OperationDescriptor(
                input_type=AddItem, output_type=None, description="x"
            ),
        },
    ).freeze()


def _ctx(state: MockState, *interceptors: PortInterceptor) -> ExecutionContext:
    """A fresh execution context over *state* — a new one models a restart."""

    registry = DepsRegistry.from_modules(MockDepsModule(state=state))
    if interceptors:
        registry = registry.with_interceptors(*interceptors)
    return ExecutionContext(deps=registry.freeze().resolve())


async def _order_state(state: MockState, order_id: UUID) -> tuple[int, int]:
    ctx = _ctx(state)
    order = await ctx.document.query(ORDER_SPEC).get(order_id)
    items = await ctx.document.query(ITEM_SPEC).count()
    return order.item_count, items


# ....................... #


def test_crash_rolls_back_in_flight_transaction_then_restart_recovers() -> None:
    state = MockState()
    registry = _registry()

    async def commit_order() -> UUID:
        ctx = _ctx(state)
        return await run_operation(registry, "create_order", None, ctx)

    order_id = asyncio.run(commit_order())

    # A crash on the order ``update`` (after the item was created) kills add_item.
    async def crash() -> None:
        ctx = _ctx(
            state,
            CrashInterceptor(rng=Random(0), route="orders", op="update"),
        )
        with pytest.raises(SimulatedCrash):
            await run_operation(registry, "add_item", AddItem(order_id=order_id), ctx)

    asyncio.run(crash())

    # The whole transaction rolled back: no item, order count unchanged.
    assert asyncio.run(_order_state(state, order_id)) == (0, 0)

    # Restart — a fresh context over the same persisted state — re-drives the work cleanly.
    async def recover() -> None:
        ctx = _ctx(state)
        await run_operation(registry, "add_item", AddItem(order_id=order_id), ctx)

    asyncio.run(recover())
    assert asyncio.run(_order_state(state, order_id)) == (1, 1)


def test_committed_state_survives_a_later_crash() -> None:
    # A crash in one operation never touches another operation's already-committed writes.
    state = MockState()
    registry = _registry()

    async def commit_order() -> UUID:
        ctx = _ctx(state)
        return await run_operation(registry, "create_order", None, ctx)

    order_id = asyncio.run(commit_order())

    async def crash() -> None:
        ctx = _ctx(
            state,
            CrashInterceptor(rng=Random(1), route="orders", op="update"),
        )
        with pytest.raises(SimulatedCrash):
            await run_operation(registry, "add_item", AddItem(order_id=order_id), ctx)

    asyncio.run(crash())

    # The committed order is still readable after the crash (restart sees consistent state).
    async def read() -> UUID:
        ctx = _ctx(state)
        return (await ctx.document.query(ORDER_SPEC).get(order_id)).id

    assert asyncio.run(read()) == order_id


def test_crash_interceptor_matches_and_passes_through() -> None:
    async def go() -> None:
        crash = CrashInterceptor(rng=Random(0), op="update")

        async def nxt(_call: PortCall) -> str:
            return "ok"

        with pytest.raises(SimulatedCrash):
            await crash.around(
                PortCall(surface="document_command", route="orders", op="update"), nxt
            )

        passed = await crash.around(
            PortCall(surface="document_command", route="orders", op="get"), nxt
        )
        assert passed == "ok"

    asyncio.run(go())
