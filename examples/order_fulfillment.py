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
import sys
from typing import Self, cast
from uuid import UUID

import attrs
import structlog
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
from forze.base.exceptions import CoreException, exc
from forze.base.logging import configure_logging
from forze.base.logging.constants import LogLevel
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

_LOGGER_NAME = "order_fulfillment"
log = structlog.get_logger(_LOGGER_NAME)


def _setup_logging(level: LogLevel) -> None:
    configure_logging(level=level, logger_names=[_LOGGER_NAME])


# Configure logging early **only when run as a script**, so the framework's verbose
# class-scaffolding trace logs (emitted as the domain classes below are defined) are
# filtered from the start. When this module is imported (e.g. by the test), we leave
# global logging untouched so we don't disturb other tests.
if __name__ == "__main__":
    _setup_logging("info")

# ----------------------- #
# Domain — Order is an aggregate that reacts to its own state change.


class OrderConfirmed(DomainEvent):
    aggregate_id: UUID


# --8<-- [start:order-aggregate]
class Order(Document, AggregateRoot):
    status: str = "pending"

    @event_emitter(fields={"status"})
    def _on_confirm(before, after: Self, diff: JsonDict) -> DomainEvent | None:  # type: ignore[no-untyped-def]
        if after.status == "confirmed" and before.status != "confirmed":
            return OrderConfirmed(aggregate_id=after.id)

        return None


# --8<-- [end:order-aggregate]


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
        domain=Order,
        create_cmd=OrderCreate,
        update_cmd=OrderUpdate,
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

    log.info("inventory reserved", sku=inv.sku, qty=s.qty)
    log.debug(
        "reserve detail",
        inventory_id=str(s.inventory_id),
        new_reserved=inv.reserved + s.qty,
    )
    return s


async def _release(ctx: ExecutionContext, s: CheckoutCtx) -> None:
    inv = await ctx.document.query(INVENTORY_SPEC).get(s.inventory_id)

    await ctx.document.command(INVENTORY_SPEC).update(
        s.inventory_id,
        inv.rev,
        InventoryUpdate(reserved=max(0, inv.reserved - s.qty)),
    )

    # Compensation — a notable event worth surfacing above info.
    log.warning("compensation: inventory released", sku=inv.sku, qty=s.qty)


# --8<-- [start:confirm-step]
async def _confirm(ctx: ExecutionContext, s: CheckoutCtx) -> CheckoutCtx:
    # Pivot. A pre-commit failure (e.g. payment declined) compensates `reserve`.
    if s.simulate_failure:
        log.error("payment declined — failing the pivot step")
        raise exc.domain("payment declined")

    order = await ctx.document.query(ORDER_SPEC).get(s.order_id)
    # The status transition fires @event_emitter -> OrderConfirmed, dispatched in THIS
    # step's transaction (the command flow), which the outbox bridge stages.
    await ctx.document.command(ORDER_SPEC).update(
        s.order_id, order.rev, OrderUpdate(status="confirmed")
    )
    # Transactional outbox: flush the staged event within the same transaction.
    await ctx.outbox.command(OUTBOX_SPEC).flush()

    log.info(
        "order confirmed — OrderConfirmed staged to outbox", order_id=str(s.order_id)
    )
    return s


# --8<-- [end:confirm-step]


# --8<-- [start:saga]
def build_checkout_saga() -> SagaDefinition[CheckoutCtx]:
    return SagaDefinition(
        name="checkout",
        steps=(
            SagaStep(
                name="reserve",
                action=_reserve,
                compensation=_release,
                tx_route="mock",
            ),
            SagaStep(
                name="confirm",
                action=_confirm,
                kind=SagaStepKind.PIVOT,
                tx_route="mock",
            ),
        ),
    )


# --8<-- [end:saga]


async def place_order(ctx: ExecutionContext) -> tuple[UUID, UUID]:
    """Create the order (pending) and the stock it draws on. Returns their ids."""

    order = await ctx.document.command(ORDER_SPEC).create(OrderCreate())
    inventory = await ctx.document.command(INVENTORY_SPEC).create(
        InventoryCreate(sku="WIDGET", reserved=0)
    )

    log.info("order placed", order_id=str(order.id), inventory_id=str(inventory.id))
    return order.id, inventory.id


async def run_checkout(
    ctx: ExecutionContext,
    order_id: UUID,
    inventory_id: UUID,
    *,
    qty: int = 2,
    simulate_failure: bool = False,
) -> None:
    """Run the checkout saga over an existing order + inventory. Raises on failure."""

    saga_ctx = CheckoutCtx(
        order_id=order_id,
        inventory_id=inventory_id,
        qty=qty,
        simulate_failure=simulate_failure,
    )
    log.info("running checkout saga", simulate_failure=simulate_failure)
    await run_saga(ctx, build_checkout_saga(), saga_ctx)


# ----------------------- #
# Relay — in production a broker + the outbox relay worker move staged events to the
# consumer. Here we claim the pending rows and hand them over in-process.


@attrs.define(frozen=True, kw_only=True)
class RelayMessage:
    key: str  # the integration event id — process_with_inbox dedups on it
    order_id: UUID


# --8<-- [start:relay]
async def relay_once(ctx: ExecutionContext) -> list[RelayMessage]:
    query = ctx.outbox.query(OUTBOX_SPEC)
    claims = await query.claim_pending()

    messages = [
        RelayMessage(key=str(c.event_id), order_id=UUID(c.payload["order_id"]))
        for c in claims
    ]

    if claims:
        await query.mark_published([c.id for c in claims])

    log.info("relayed events from outbox", count=len(messages))
    return messages


# --8<-- [end:relay]


# ----------------------- #
# Consumer — exactly-once via the inbox; the effect is creating a Shipment.


async def _fulfill(ctx: ExecutionContext, message: RelayMessage) -> None:
    await ctx.document.command(SHIPMENT_SPEC).create(
        ShipmentCreate(order_id=message.order_id)
    )
    log.info("shipment created", order_id=str(message.order_id))


# --8<-- [start:inbox]
async def deliver(ctx: ExecutionContext, message: RelayMessage) -> bool:
    """Process one relayed message exactly-once. Returns False if it was a duplicate."""

    processed = await process_with_inbox(
        ctx,
        message,
        inbox_spec=INBOX_SPEC,
        handler=lambda m: _fulfill(ctx, m),
        tx_route="mock",
    )

    if not processed:
        log.debug("duplicate message skipped by inbox", key=message.key)

    return processed


# --8<-- [end:inbox]


# ----------------------- #
# Context wiring — register the domain-event → outbox bridge, build an in-process context.


# --8<-- [start:outbox-bridge]
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


# --8<-- [end:outbox-bridge]


async def _demo_happy() -> None:
    log.info("──────── happy path ────────")
    ctx = build_context()

    order_id, inventory_id = await place_order(ctx)
    await run_checkout(ctx, order_id, inventory_id)

    for message in await relay_once(ctx):
        await deliver(ctx, message)

    order = await ctx.document.query(ORDER_SPEC).get(order_id)
    inventory = await ctx.document.query(INVENTORY_SPEC).get(inventory_id)
    shipments = await ctx.document.query(SHIPMENT_SPEC).find_many()
    log.info(
        "happy path complete",
        order=order.status,
        reserved=inventory.reserved,
        shipments=len(shipments.hits),
    )


async def _demo_compensation() -> None:
    log.info("──────── compensation path (payment declined) ────────")
    ctx = build_context()

    order_id, inventory_id = await place_order(ctx)
    try:
        await run_checkout(ctx, order_id, inventory_id, simulate_failure=True)
    except CoreException as error:
        log.info("saga failed and rolled back", code=error.code)

    # The pivot never committed: inventory is released, nothing was staged/relayed/shipped.
    inventory = await ctx.document.query(INVENTORY_SPEC).get(inventory_id)
    relayed = await relay_once(ctx)
    shipments = await ctx.document.query(SHIPMENT_SPEC).find_many()
    log.info(
        "compensation path complete",
        reserved=inventory.reserved,
        relayed=len(relayed),
        shipments=len(shipments.hits),
    )


async def main(level: LogLevel = "info") -> None:
    # Quiet the framework's verbose trace logs; show this example's narrative instead.
    # Run with ``python -m examples.order_fulfillment debug`` to also see debug lines.
    _setup_logging(level)
    await _demo_happy()
    await _demo_compensation()


if __name__ == "__main__":
    chosen = cast(LogLevel, sys.argv[1]) if len(sys.argv) > 1 else "info"
    asyncio.run(main(chosen))
