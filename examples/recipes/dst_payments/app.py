"""Deterministic simulation of a real forze app — and why faithful transactions matter.

This is ordinary forze code: handlers talk to **ports** (``ctx.document``), emit a **domain
event**, and run **inside a transaction** (the operations carry a tx route, as real write
operations do). Nothing here knows about DST; the *Simulation* (deps + ``observe`` + an
invariant) is the only test-side code.

``pay_order`` charges (writes a payment row) and flips the order to paid in **one
transaction**, guarded by the order's ``rev`` (optimistic concurrency). Two concurrent
payments race: both read the order unpaid, both write a payment, both try the ``rev``-guarded
update — the loser's update conflicts, so its **whole transaction rolls back, including its
payment row**. Exactly one charge. The app is *correct*, and DST reports **no violation**.

The point: this is only trustworthy because the mock models transactions faithfully. Under
the legacy no-op manager (``MockDepsModule(transactions="none")``) the loser's payment would
*not* roll back, and DST would report a **false** double-charge. Faithful, concurrency-
preserving atomicity (the default) is what keeps DST's findings honest.

Try it (from the repo root)::

    forze dst run      examples.recipes.dst_payments.app:simulation   # ✓ no violation
    forze dst topology examples.recipes.dst_payments.app:simulation
    forze dst derive   examples.recipes.dst_payments.app:simulation
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
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import (
    BaseDTO,
    CreateDocumentCmd,
    Document,
    DomainEvent,
    ReadDocument,
)
from forze_dst import Simulation
from forze_dst.markers import record_event
from forze_dst.invariants import expect
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


# --8<-- [start:handler]
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
# --8<-- [end:handler]


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


# Each operation runs in a transaction (as a real write operation does), so the faithful
# mock transaction manager gives it atomicity: a failed/aborted operation leaves no writes.
_TX_PLAN = OperationPlan().bind_tx().set_route("mock").finish(deep=False)


registry = OperationRegistry(
    handlers={
        "create_order": lambda ctx: _CreateOrder(ctx=ctx),
        "pay_order": lambda ctx: _PayOrder(ctx=ctx),
        "notify": lambda ctx: _Notify(ctx=ctx),
    },
    plans={op: _TX_PLAN for op in ("create_order", "pay_order", "notify")},
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


# --8<-- [start:simulation]
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
# --8<-- [end:simulation]
