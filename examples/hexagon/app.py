"""The smallest complete slice: a domain, a port, and the wiring that binds an adapter.

No web framework, no ORM, no transport — this is the part of a service that survives
changing any of them. ``main`` runs it on the in-memory mock adapter; pointing the same
code at Postgres is one edit, in the one place an adapter is named.

Run it:  uv run python -m examples.hexagon.app
Exercised by tests/unit/test_examples/test_hexagon.py.
"""

import asyncio
from uuid import UUID

import structlog

from forze import (
    CreateDocumentCmd,
    Document,
    DocumentSpec,
    DocumentWriteTypes,
    ExecutionContext,
    ReadDocument,
    build_runtime,
    configure_logging,
)
from forze_mock import MockDepsModule

log = structlog.get_logger("hexagon")


# Domain — plain models. Nothing here knows about HTTP, SQL, or a broker.
class Order(Document):
    item: str


class CreateOrder(CreateDocumentCmd):
    item: str


class ReadOrder(ReadDocument):  # adds id, rev, created_at, last_update_at
    item: str


# The port — one spec names the aggregate and the types that cross its boundary.
ORDERS = DocumentSpec(
    name="orders",
    read=ReadOrder,
    write=DocumentWriteTypes(domain=Order, create_cmd=CreateOrder),
)


# Application — speaks to the port, never learns which storage answers it.
async def place_order(ctx: ExecutionContext, item: str) -> ReadOrder:
    return await ctx.document.command(ORDERS).create(CreateOrder(item=item))


async def read_order(ctx: ExecutionContext, order_id: UUID) -> ReadOrder:
    return await ctx.document.query(ORDERS).get(order_id)


async def main() -> None:
    # Wiring — the only place an adapter is named. A real backend replaces this module
    # (Postgres takes a client, its relation config and a lifecycle module — see
    # examples/recipes/crud_fastapi), and nothing above this line changes.
    runtime = build_runtime(MockDepsModule())
    async with runtime.scope():
        ctx = runtime.get_context()
        placed = await place_order(ctx, item="widget")
        order = await read_order(ctx, placed.id)
        log.info("stored and read back", id=str(order.id), item=order.item, rev=order.rev)


if __name__ == "__main__":
    # Configure logging only when run as a script, so imports and tests stay unaffected.
    configure_logging(level="info", logger_names=["hexagon", "forze"])
    asyncio.run(main())
