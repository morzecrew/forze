"""Deterministic simulation of a real forze app: a concurrent double-charge.

This is ordinary forze code — handlers talk to **ports** (``ctx.document``) and emit a
**domain event**; nothing here knows about DST. Under simulation, ``MockDepsModule`` covers
every port in-memory (fresh per run), the engine trace captures the activity, and the
*Simulation* (deps + an ``observe`` hook + an invariant) is the only test-side code. No DST
calls leak into the handlers.

The bug: ``pay_order`` reads the order, checks "not paid", *then* (after an await) marks it
paid and writes a payment. Two concurrent payments both pass the check → two payment rows
for one order. The test-side ``observe`` counts payments and the invariant flags the
duplicate; DST minimizes to the two racing payments.

Try it (from the repo root)::

    forze dst run      examples.recipes.dst_payments.app:simulation --strategy dpor
    forze dst topology examples.recipes.dst_payments.app:simulation
    forze dst derive   examples.recipes.dst_payments.app:simulation

    # ad-hoc — point at just the registry; the CLI auto-mocks deps and applies the built-in
    # "no unexpected error" safety net (domain rules like double-charge need the Simulation):
    forze dst run examples.recipes.dst_payments.app:registry
"""

from __future__ import annotations

from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.domain.deps import DomainDeps
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.domain.handler import DomainEventRegistry
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import (
    BaseDTO,
    CreateDocumentCmd,
    Document,
    DomainEvent,
    ReadDocument,
)
from forze_dst import Simulation, expect, record_event
from forze_mock import MockDepsModule

# ----------------------- #
# Domain — orders and payments, persisted through the document port.


class Order(Document):
    paid: bool = False


class OrderCreate(CreateDocumentCmd):
    paid: bool = False


class OrderUpdate(BaseDTO):
    paid: bool | None = None


class OrderRead(ReadDocument):
    paid: bool


class Payment(Document):
    order_id: UUID


class PaymentCreate(CreateDocumentCmd):
    order_id: UUID


class PaymentRead(ReadDocument):
    order_id: UUID


ORDER_SPEC = DocumentSpec(
    name="orders",
    read=OrderRead,
    write=DocumentWriteTypes(
        domain=Order, create_cmd=OrderCreate, update_cmd=OrderUpdate
    ),
)
PAYMENT_SPEC = DocumentSpec(
    name="payments",
    read=PaymentRead,
    write=DocumentWriteTypes(domain=Payment, create_cmd=PaymentCreate),
)


class PayCmd(BaseModel):
    order_id: UUID


class NotifyCmd(BaseModel):
    order_id: UUID


class OrderPaid(DomainEvent):
    order_id: UUID


# ....................... #
# Operations — plain forze handlers over ports. No DST awareness.


@attrs.define(slots=True, kw_only=True)
class _CreateOrder(Handler[None, UUID]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> UUID:
        order = await self.ctx.document.command(ORDER_SPEC).create(OrderCreate())
        return order.id  # the handle DST threads into pay_order


@attrs.define(slots=True, kw_only=True)
class _PayOrder(Handler[PayCmd, None]):
    ctx: ExecutionContext

    async def __call__(self, args: PayCmd) -> None:
        order = await self.ctx.document.query(ORDER_SPEC).get(args.order_id)
        if order is None or order.paid:  # pyright: ignore[reportUnnecessaryComparison]
            return

        # BUG: the charge (a side effect) happens *before* the optimistic-concurrency-guarded
        # transition. Two payments that interleave at the port boundaries above both read
        # rev=0 and both create a payment row; the rev guard then lets only one ``update``
        # win (the other conflicts) — but both already charged. The fix is to update first
        # and charge only once the guarded write succeeds. (No artificial yield — the real
        # ``await`` port calls are the interleaving points under simulation.)
        await self.ctx.document.command(PAYMENT_SPEC).create(
            PaymentCreate(order_id=args.order_id)
        )
        await self.ctx.document.command(ORDER_SPEC).update(
            args.order_id, order.rev, OrderUpdate(paid=True)
        )
        # Emitting a domain event triggers the registered handler (a saga-style cascade).
        await DomainDeps(ctx=self.ctx)().dispatch([OrderPaid(order_id=args.order_id)])


@attrs.define(slots=True, kw_only=True)
class _Notify(Handler[NotifyCmd, None]):
    ctx: ExecutionContext

    async def __call__(self, args: NotifyCmd) -> None:
        await self.ctx.document.query(ORDER_SPEC).get(
            args.order_id
        )  # "notify the customer"


# ....................... #
# Wiring — a mutable holder breaks the registry↔handler cycle so the OrderPaid handler can
# invoke the `notify` operation reactively.

_HOLDER: dict[str, object] = {}


def _on_order_paid(ctx: ExecutionContext):  # type: ignore[no-untyped-def]
    async def handle(event: DomainEvent) -> None:
        order_id = event.order_id  # type: ignore[attr-defined]
        await run_operation(_HOLDER["registry"], "notify", NotifyCmd(order_id=order_id), ctx)  # type: ignore[arg-type]

    return handle


_EVENTS = DomainEventRegistry()
_EVENTS.register(OrderPaid, _on_order_paid)


registry = OperationRegistry(
    handlers={
        "create_order": lambda ctx: _CreateOrder(ctx=ctx),
        "pay_order": lambda ctx: _PayOrder(ctx=ctx),
        "notify": lambda ctx: _Notify(ctx=ctx),
    },
    descriptors={
        "create_order": OperationDescriptor(
            input_type=None, output_type=None, description="Create an order."
        ),
        "pay_order": OperationDescriptor(
            input_type=PayCmd, output_type=None, description="Pay an order."
        ),
        "notify": OperationDescriptor(
            input_type=NotifyCmd,
            output_type=None,
            description="Notify (reactive on OrderPaid).",
        ),
    },
).freeze()
_HOLDER["registry"] = registry


# ....................... #
# Simulation — the only test-side code: auto-mocked deps (fresh per run), a test-side
# observe hook reading final state via the same ports, and the domain invariant.


async def _observe(ctx: ExecutionContext) -> None:
    payments = await ctx.document.query(PAYMENT_SPEC).count()
    record_event("payments", total=payments)


simulation = Simulation(
    operations=registry,
    deps=lambda: MockDepsModule(domain_events=_EVENTS),
    observe=_observe,
    invariants=[
        expect(
            "payments",
            lambda event: event.fields["total"] <= 1,
            message="an order was charged more than once",
        )
    ],
)
