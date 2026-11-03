"""Recipe: save an order's lines — reconcile a child collection against the list a client sends.

A client edits an order and sends its lines back whole. The handler makes the stored lines
match: a line no longer sent is deleted, a new one created, and a kept one updated only where
its quantity or position changed, so an untouched line keeps its id and revision.

``save_lines_by_hand`` is that diff written out; ``ChildCollectionSync`` is the same procedure
as one declaration. The handler touches the order first, at the revision the client read, so a
save based on an older order is refused before any line is written.

Run it:  uv run python -m examples.recipes.child_collection.app   (no infra — mock store)
Exercised by tests/unit/test_examples/test_child_collection.py.
"""

from __future__ import annotations

import asyncio
from uuid import UUID

import attrs
import structlog

from forze.application.contracts.document import DocumentSpec, KeyedUpdate
from forze.application.contracts.execution import Handler
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.logging import configure_logging
from forze.base.logging.constants import LogLevel
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_kits.aggregates import ChildCollectionSync
from forze_mock import MockDepsModule

_LOGGER_NAME = "child_collection"
log = structlog.get_logger(_LOGGER_NAME)


def _setup_logging(level: LogLevel) -> None:
    configure_logging(level=level, logger_names=[_LOGGER_NAME, "forze"])


# --8<-- [start:domain]
class Order(Document):
    customer: str


class CreateOrder(CreateDocumentCmd):
    customer: str


class ReadOrder(ReadDocument):
    customer: str


class Line(Document):
    order_id: UUID
    sku: str
    quantity: int
    position: int


class CreateLine(CreateDocumentCmd):
    order_id: UUID
    sku: str
    quantity: int
    position: int


class UpdateLine(BaseDTO):
    quantity: int | None = None
    position: int | None = None


class ReadLine(ReadDocument):
    order_id: UUID
    sku: str
    quantity: int
    position: int


ORDER_SPEC = DocumentSpec(
    name="orders",
    read=ReadOrder,
    write={"domain": Order, "create_cmd": CreateOrder},
)
LINE_SPEC = DocumentSpec(
    name="order_lines",
    read=ReadLine,
    write={"domain": Line, "create_cmd": CreateLine, "update_cmd": UpdateLine},
)
# --8<-- [end:domain]


# --8<-- [start:request]
class LineIn(BaseDTO):
    sku: str
    quantity: int


class SaveLines(BaseDTO):
    order_id: UUID
    rev: int
    """The order's revision the client read."""

    lines: list[LineIn]
    """Every line the order should have, in display order."""


def to_commands(args: SaveLines) -> list[CreateLine]:
    """One create command per line; a line's position is its place in the list."""

    return [
        CreateLine(order_id=args.order_id, sku=x.sku, quantity=x.quantity, position=i)
        for i, x in enumerate(args.lines)
    ]


# --8<-- [end:request]


# --8<-- [start:by_hand]
async def save_lines_by_hand(ctx: ExecutionContext, args: SaveLines) -> None:
    rows = await ctx.document.query(LINE_SPEC).find_many(
        filters={"$values": {"order_id": args.order_id}}
    )
    current = {row.sku: row for row in rows.hits}
    wanted = {cmd.sku: cmd for cmd in to_commands(args)}
    lines = ctx.document.command(LINE_SPEC)

    # Deletes first: a new line may take the position a removed one held.
    if gone := [row.id for sku, row in current.items() if sku not in wanted]:
        await lines.kill_many(gone)

    if new := [cmd for sku, cmd in wanted.items() if sku not in current]:
        await lines.create_many(new, return_new=False)

    changed = [
        KeyedUpdate(
            id=row.id,
            rev=row.rev,
            dto=UpdateLine(quantity=cmd.quantity, position=cmd.position),
        )
        for sku, row in current.items()
        if (cmd := wanted.get(sku)) is not None
        and (row.quantity, row.position) != (cmd.quantity, cmd.position)
    ]

    if changed:
        await lines.update_many(changed, return_new=False)


# --8<-- [end:by_hand]


# --8<-- [start:sync]
LINES = ChildCollectionSync(
    child_spec=LINE_SPEC,
    parent_fk="order_id",
    key_fields=("sku",),
    compare_fields=frozenset({"quantity", "position"}),
)


@attrs.define(slots=True, kw_only=True, frozen=True)
class SaveOrderLines(Handler[SaveLines, ReadOrder]):
    ctx: ExecutionContext

    async def __call__(self, args: SaveLines) -> ReadOrder:
        # First, at the client's revision: a stale save is refused before any line is written.
        order = await self.ctx.document.command(ORDER_SPEC).touch(args.order_id, rev=args.rev)
        await LINES.reconcile(self.ctx, args.order_id, to_commands(args))

        return order


# --8<-- [end:sync]


# --8<-- [start:registry]
SAVE_LINES = "orders.save_lines"

REGISTRY = (
    OperationRegistry(handlers={SAVE_LINES: lambda ctx: SaveOrderLines(ctx=ctx)})
    .bind(SAVE_LINES)
    .bind_tx()  # reconcile refuses to run outside a transaction
    .set_route("mock")
    .finish(deep=True)
    .freeze()
)
# --8<-- [end:registry]


# --8<-- [start:scenario]
async def edit_an_order(ctx: ExecutionContext) -> dict[str, tuple[int, int]]:
    """Create an order with three lines, then save it with one dropped, one added, one moved.

    :returns: Each stored line's ``(quantity, position)``, by sku.
    """

    order = await ctx.document.command(ORDER_SPEC).create(CreateOrder(customer="acme"))
    save = REGISTRY.resolve(SAVE_LINES, ctx)
    first = [
        LineIn(sku="bolt", quantity=10),
        LineIn(sku="nut", quantity=10),
        LineIn(sku="washer", quantity=5),
    ]

    order = await save(SaveLines(order_id=order.id, rev=order.rev, lines=first))

    # "nut" is dropped, "screw" added, "washer" moves up; "bolt" is not written.
    await save(
        SaveLines(
            order_id=order.id,
            rev=order.rev,
            lines=[
                LineIn(sku="bolt", quantity=10),
                LineIn(sku="washer", quantity=5),
                LineIn(sku="screw", quantity=20),
            ],
        )
    )

    rows = await ctx.document.query(LINE_SPEC).find_many(
        filters={"$values": {"order_id": order.id}}
    )

    return {row.sku: (row.quantity, row.position) for row in rows.hits}


# --8<-- [end:scenario]


async def main() -> None:
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        lines = await edit_an_order(runtime.get_context())
        log.info("saved", lines=lines)


if __name__ == "__main__":
    _setup_logging("info")
    asyncio.run(main())
