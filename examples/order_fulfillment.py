"""End-to-end worked example: order fulfillment across the whole Forze stack.

One story, in-process (``forze_mock``), exercising how the pieces **compose**:

    place order  ──▶  checkout saga  ──▶  Order aggregate confirmed
                       (reserve, pivot)     │  @event_emitter → OrderConfirmed
                                            │  dispatched IN the step's transaction
                                            ▼
                                      outbox: order.confirmed staged + flushed
                                            │
                                   (relay: claim → deliver)        ← a broker hop in prod
                                            ▼
                                      inbox: dedup (exactly-once)
                                            ▼
                                      Shipment aggregate created

Run it:  ``python -m examples.order_fulfillment``
It is also executed by ``tests/unit/test_examples/test_order_fulfillment.py`` — the example
is the spec, and the test proves the composition (happy path, idempotent redelivery,
compensation).
"""

import asyncio
from typing import Self
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.outbox import OutboxSpec
from forze.application.contracts.saga import SagaDefinition, SagaStep, SagaStepKind
from forze.application.execution import (
    DomainEventRegistry,
    ExecutionContext,
    run_saga,
)
from forze.application.execution.deps import DepsRegistry
from forze.application.execution.domain import outbox_event_handler
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import PydanticModelCodec
from forze.domain.models import (
    AggregateRoot,
    BaseDTO,
    CreateDocumentCmd,
    Document,
    DomainEvent,
    ReadDocument,
    event_emitter,
)
from forze_kits.integrations.inbox import process_with_inbox
from forze_mock import MockDepsModule

# ----------------------- #
# Domain — Order is an aggregate that reacts to its own state change.


class OrderConfirmed(DomainEvent):
    aggregate_id: UUID


class Order(Document, AggregateRoot):
    status: str = "pending"

    @event_emitter(fields={"status"})
    def _on_confirm(before, after: Self, diff: JsonDict) -> DomainEvent | None:  # type: ignore[no-untyped-def]
        if after.status == "confirmed" and before.status != "confirmed":
            return OrderConfirmed(aggregate_id=after.id)

        return None


class OrderCreate(CreateDocumentCmd):
    status: str = "pending"


class OrderUpdate(BaseDTO):
    status: str | None = None


class OrderRead(ReadDocument):
    status: str


ORDER_SPEC = DocumentSpec(
    name="orders",
    read=OrderRead,
    write=DocumentWriteTypes(
        domain=Order, create_cmd=OrderCreate, update_cmd=OrderUpdate
    ),
)

# ....................... #
# Domain — Inventory (reserve / release) and Shipment (downstream effect).


class Inventory(Document):
    sku: str
    reserved: int = 0


class InventoryCreate(CreateDocumentCmd):
    sku: str
    reserved: int = 0


class InventoryUpdate(BaseDTO):
    reserved: int | None = None


class InventoryRead(ReadDocument):
    sku: str
    reserved: int


INVENTORY_SPEC = DocumentSpec(
    name="inventory",
    read=InventoryRead,
    write=DocumentWriteTypes(
        domain=Inventory,
        create_cmd=InventoryCreate,
        update_cmd=InventoryUpdate,
    ),
)


class Shipment(Document):
    order_id: UUID


class ShipmentCreate(CreateDocumentCmd):
    order_id: UUID


class ShipmentUpdate(BaseDTO):
    pass


class ShipmentRead(ReadDocument):
    order_id: UUID


SHIPMENT_SPEC = DocumentSpec(
    name="shipments",
    read=ShipmentRead,
    write=DocumentWriteTypes(
        domain=Shipment,
        create_cmd=ShipmentCreate,
        update_cmd=ShipmentUpdate,
    ),
)

# ....................... #
# Integration contracts — the outbox event and the inbox.


class OrderConfirmedPayload(BaseModel):
    order_id: str


OUTBOX_SPEC = OutboxSpec(
    name="order-events",
    codec=PydanticModelCodec(OrderConfirmedPayload),
)
INBOX_SPEC = InboxSpec(name="fulfillment")

# ----------------------- #
# Producer — a checkout saga: reserve inventory (compensatable), confirm order (pivot).


@attrs.define(frozen=True, kw_only=True)
class CheckoutCtx:
    order_id: UUID
    inventory_id: UUID
    qty: int
    simulate_failure: bool = False


async def _reserve(ctx: ExecutionContext, s: CheckoutCtx) -> CheckoutCtx:
    inv = await ctx.document.query(INVENTORY_SPEC).get(s.inventory_id)

    await ctx.document.command(INVENTORY_SPEC).update(
        s.inventory_id,
        inv.rev,
        InventoryUpdate(reserved=inv.reserved + s.qty),
    )

    return s


async def _release(ctx: ExecutionContext, s: CheckoutCtx) -> None:
    inv = await ctx.document.query(INVENTORY_SPEC).get(s.inventory_id)

    await ctx.document.command(INVENTORY_SPEC).update(
        s.inventory_id,
        inv.rev,
        InventoryUpdate(reserved=max(0, inv.reserved - s.qty)),
    )


async def _confirm(ctx: ExecutionContext, s: CheckoutCtx) -> CheckoutCtx:
    # Pivot. A pre-commit failure (e.g. payment declined) compensates `reserve`.
    if s.simulate_failure:
        raise exc.domain("payment declined")

    order = await ctx.document.query(ORDER_SPEC).get(s.order_id)
    # The status transition fires @event_emitter -> OrderConfirmed, dispatched in THIS
    # step's transaction (the command flow), which the outbox bridge stages.
    await ctx.document.command(ORDER_SPEC).update(
        s.order_id, order.rev, OrderUpdate(status="confirmed")
    )
    # Transactional outbox: flush the staged event within the same transaction.
    await ctx.outbox.command(OUTBOX_SPEC).flush()
    return s


def build_checkout_saga() -> SagaDefinition[CheckoutCtx]:
    return SagaDefinition(
        name="checkout",
        steps=(
            SagaStep(
                name="reserve", action=_reserve, compensation=_release, tx_route="mock"
            ),
            SagaStep(
                name="confirm",
                action=_confirm,
                kind=SagaStepKind.PIVOT,
                tx_route="mock",
            ),
        ),
    )


async def run_checkout(
    ctx: ExecutionContext,
    *,
    qty: int = 2,
    simulate_failure: bool = False,
) -> tuple[UUID, UUID]:
    """Place an order + stock, then run the checkout saga. Returns (order_id, inventory_id)."""

    order = await ctx.document.command(ORDER_SPEC).create(OrderCreate())
    inventory = await ctx.document.command(INVENTORY_SPEC).create(
        InventoryCreate(sku="WIDGET", reserved=0)
    )

    saga_ctx = CheckoutCtx(
        order_id=order.id,
        inventory_id=inventory.id,
        qty=qty,
        simulate_failure=simulate_failure,
    )
    await run_saga(ctx, build_checkout_saga(), saga_ctx)

    return order.id, inventory.id


# ----------------------- #
# Relay — in production a broker + the outbox relay worker move staged events to the
# consumer. Here we claim the pending rows and hand them over in-process.


@attrs.define(frozen=True, kw_only=True)
class RelayMessage:
    key: str  # the integration event id — process_with_inbox dedups on it
    order_id: UUID


async def relay_once(ctx: ExecutionContext) -> list[RelayMessage]:
    query = ctx.outbox.query(OUTBOX_SPEC)
    claims = await query.claim_pending()

    messages = [
        RelayMessage(key=str(c.event_id), order_id=UUID(c.payload["order_id"]))
        for c in claims
    ]

    if claims:
        await query.mark_published([c.id for c in claims])

    return messages


# ----------------------- #
# Consumer — exactly-once via the inbox; the effect is creating a Shipment.


async def _fulfill(ctx: ExecutionContext, message: RelayMessage) -> None:
    await ctx.document.command(SHIPMENT_SPEC).create(
        ShipmentCreate(order_id=message.order_id)
    )


async def deliver(ctx: ExecutionContext, message: RelayMessage) -> bool:
    """Process one relayed message exactly-once. Returns False if it was a duplicate."""

    return await process_with_inbox(
        ctx,
        message,
        inbox_spec=INBOX_SPEC,
        handler=lambda m: _fulfill(ctx, m),
        tx_route="mock",
    )


# ----------------------- #
# Context wiring — register the domain-event → outbox bridge, build an in-process context.


def build_context() -> ExecutionContext:
    registry = DomainEventRegistry()
    registry.register(
        OrderConfirmed,
        outbox_event_handler(
            OUTBOX_SPEC,
            "order.confirmed",
            lambda e: OrderConfirmedPayload(order_id=str(e.aggregate_id)),
        ),
    )
    module = MockDepsModule(domain_events=registry)
    return ExecutionContext(deps=DepsRegistry.from_modules(module).freeze().resolve())


async def main() -> None:
    ctx = build_context()

    order_id, inventory_id = await run_checkout(ctx)
    messages = await relay_once(ctx)
    for message in messages:
        await deliver(ctx, message)

    order = await ctx.document.query(ORDER_SPEC).get(order_id)
    inventory = await ctx.document.query(INVENTORY_SPEC).get(inventory_id)
    shipments = await ctx.document.query(SHIPMENT_SPEC).find_many()

    print(
        f"order={order.status} reserved={inventory.reserved} "
        f"relayed={len(messages)} shipments={len(shipments.hits)}"
    )


if __name__ == "__main__":
    asyncio.run(main())
