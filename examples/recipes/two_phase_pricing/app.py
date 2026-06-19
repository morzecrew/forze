"""Recipe: call an external service before the write, with a minimal transaction.

A two-phase handler splits the work the engine wraps in a transaction. ``prepare``
runs **outside** the transaction — the place for the slow pricing call — and
returns a payload the engine threads into ``apply``, which runs **inside** the
transaction and writes the priced order. So the connection is held only for the
write, never across the external call, and ``prepare`` runs exactly once even if a
retry or hedge wrap re-runs ``apply``.

Contrast with a plain handler that does the call and the write together: there the
transaction would stay open across the pricing call. Reach for two-phase when an
external call must precede the write; lazy acquisition already covers pure
compute before the first query.

Run it:  uv run python -m examples.recipes.two_phase_pricing.app   (no infra — mock store)
Exercised by tests/unit/test_examples/test_two_phase_pricing.py.
"""

from __future__ import annotations

import asyncio

import attrs

from forze.application.contracts.document import DocumentSpec
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_kits.aggregates.document import TwoPhaseDocumentHandler
from forze_mock import MockDepsModule

# --8<-- [start:domain]
class Order(Document):
    item: str
    price: int


class CreateOrder(CreateDocumentCmd):
    item: str
    price: int


class ReadOrder(ReadDocument):
    item: str
    price: int


class QuoteRequest(BaseDTO):
    item: str
# --8<-- [end:domain]


ORDER_SPEC = DocumentSpec(
    name="orders",
    read=ReadOrder,
    write={"domain": Order, "create_cmd": CreateOrder},
)


# --8<-- [start:service]
class PricingService:
    """Stands in for a remote pricing API — a slow call you don't want in a tx."""

    async def quote(self, item: str) -> int:
        await asyncio.sleep(0)  # a network round trip in real life
        return len(item) * 100


PRICING = PricingService()
# --8<-- [end:service]


# --8<-- [start:handler]
@attrs.define(slots=True, kw_only=True, frozen=True)
class PriceAndCreate(
    TwoPhaseDocumentHandler[QuoteRequest, int, ReadOrder, CreateOrder]
):
    """Quote the price (outside the tx), then create the priced order (inside it)."""

    pricing: PricingService

    async def prepare(self, args: QuoteRequest) -> int:
        # OUTSIDE the transaction — no connection held across this call.
        return await self.pricing.quote(args.item)

    async def apply(self, args: QuoteRequest, payload: int) -> ReadOrder:
        # INSIDE the transaction — self.writer() is the command port.
        return await self.writer().create(CreateOrder(item=args.item, price=payload))
# --8<-- [end:handler]


# --8<-- [start:registry]
PRICE_AND_CREATE = "orders.price_and_create"

# The handler holds the context (ports resolve per phase) and the pricing service.
# .two_phase() splits prepare/apply; the tx route scopes apply's transaction.
REGISTRY = (
    OperationRegistry(
        handlers={
            PRICE_AND_CREATE: lambda ctx: PriceAndCreate(
                ctx=ctx, spec=ORDER_SPEC, pricing=PRICING
            )
        }
    )
    .bind(PRICE_AND_CREATE)
    .two_phase()
    .bind_tx()
    .set_route("mock")
    .finish(deep=True)
    .freeze()
)
# --8<-- [end:registry]


# --8<-- [start:scenario]
async def place_priced_order(ctx: ExecutionContext) -> ReadOrder:
    created = await REGISTRY.resolve(PRICE_AND_CREATE, ctx)(QuoteRequest(item="widget"))

    # The write committed and is readable afterwards.
    stored = await ctx.document.query(ORDER_SPEC).get(created.id)
    assert stored is not None and stored.price == created.price

    return created
# --8<-- [end:scenario]


async def main() -> None:
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())
    async with runtime.scope():
        order = await place_priced_order(runtime.get_context())
        print(f"priced and created: item={order.item!r} price={order.price}")


if __name__ == "__main__":
    asyncio.run(main())
