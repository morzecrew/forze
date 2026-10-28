"""Orders, their lines and notes, and the paired system invariants that relate them.

Shared by the runtime, final-state oracle, per-commit oracle and backend tests, so every evaluator
is checked against the same laws: a sum bounded by a parent's field (``qty``, an ``int``), the same
bound over exact decimals (``amount`` against ``total``), at least one line per order, and at least
one note per order, where a note refers to its order by the id's string (``order_ref: str``).
"""

from __future__ import annotations

from decimal import Decimal
from uuid import UUID

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.invariants import (
    CountAll,
    Measure,
    ReadSet,
    SumOf,
    SystemInvariant,
)
from forze.application.execution import ExecutionContext
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_dst.invariants import check, compile_oracle
from forze_dst.oracle.recorder import Recorder, bind_recorder
from forze_dst.oracle.system_invariants import SYSTEM_INVARIANT_KIND
from forze_kits.invariants import evaluate

# ----------------------- #


class Order(Document):
    qty: int = 0
    total: Decimal = Decimal(0)


class OrderCreate(CreateDocumentCmd):
    qty: int = 0
    total: Decimal = Decimal(0)


class OrderRead(ReadDocument):
    qty: int = 0
    total: Decimal = Decimal(0)


class Line(Document):
    order_id: UUID
    qty: int = 0
    amount: Decimal = Decimal(0)
    kind: str = "sale"


class LineCreate(CreateDocumentCmd):
    order_id: UUID
    qty: int = 0
    amount: Decimal = Decimal(0)
    kind: str = "sale"


class LineRead(ReadDocument):
    order_id: UUID
    qty: int = 0
    amount: Decimal = Decimal(0)
    kind: str = "sale"


ORDERS = DocumentSpec(
    name="paired_orders",
    read=OrderRead,
    write={"domain": Order, "create_cmd": OrderCreate, "update_cmd": OrderCreate},
)

LINES = DocumentSpec(
    name="paired_lines",
    read=LineRead,
    write={"domain": Line, "create_cmd": LineCreate, "update_cmd": LineCreate},
)


class Note(Document):
    order_ref: str


class NoteCreate(CreateDocumentCmd):
    order_ref: str


class NoteRead(ReadDocument):
    order_ref: str


NOTES = DocumentSpec(
    name="paired_notes",
    read=NoteRead,
    write={"domain": Note, "create_cmd": NoteCreate, "update_cmd": NoteCreate},
)

_LINES_BY_ORDER = ReadSet(spec=LINES, scope_keys=("order_id",))
_ORDER_BY_ID = ReadSet(spec=ORDERS, scope_keys=("id",))

# ....................... #

LINES_WITHIN_ORDER_QTY = SystemInvariant(
    name="lines_within_order_qty",
    read_set=_LINES_BY_ORDER,
    aggregate=SumOf("qty"),
    against=Measure(read_set=_ORDER_BY_ID, aggregate=SumOf("qty")),
    holds=lambda lines, order: lines <= order,
)

LINES_WITHIN_ORDER_TOTAL = SystemInvariant(
    name="lines_within_order_total",
    read_set=_LINES_BY_ORDER,
    aggregate=SumOf("amount"),
    against=Measure(read_set=_ORDER_BY_ID, aggregate=SumOf("total")),
    holds=lambda lines, order: lines <= order,
)

ORDER_HAS_A_LINE = SystemInvariant(
    name="order_has_a_line",
    read_set=_LINES_BY_ORDER,
    aggregate=CountAll(),
    against=Measure(read_set=_ORDER_BY_ID, aggregate=CountAll()),
    holds=lambda lines, orders: orders == 0 or lines >= 1,
)

REFUNDS_WITHIN_SALES = SystemInvariant(
    name="refunds_within_sales",
    read_set=ReadSet(spec=LINES, scope_keys=("order_id",), where={"$values": {"kind": "refund"}}),
    aggregate=SumOf("amount"),
    against=Measure(
        read_set=ReadSet(spec=LINES, scope_keys=("order_id",), where={"$values": {"kind": "sale"}}),
        aggregate=SumOf("amount"),
    ),
    holds=lambda refunds, sales: refunds <= sales,
)
"""A sum bounded by another sum over the same collection."""

ORDER_HAS_A_NOTE = SystemInvariant(
    name="order_has_a_note",
    read_set=ReadSet(spec=NOTES, scope_keys=("order_ref",)),
    aggregate=CountAll(),
    against=Measure(read_set=_ORDER_BY_ID, aggregate=CountAll()),
    holds=lambda notes, orders: orders == 0 or notes >= 1,
)
"""A ``str`` reference paired with a ``UUID`` id: one scope per order, whichever form is read."""


# ....................... #


async def assert_paired_laws_on(ctx: ExecutionContext) -> None:
    """Check the laws over a real backend: exact decimals, an int field, an order with no lines.

    A backend's own sum types differ (Postgres sums ``bigint`` to ``numeric``, Mongo returns
    ``Decimal128``), so the value a law receives must follow the read model's declaration. The
    runtime check and the final-state oracle must also agree on which order breaks the law.
    """

    full = await ctx.document.command(ORDERS).create(OrderCreate(qty=3, total=Decimal("0.3")))
    empty = await ctx.document.command(ORDERS).create(OrderCreate(qty=1, total=Decimal("1")))

    for qty, amount in ((1, "0.1"), (2, "0.2")):
        await ctx.document.command(LINES).create(
            LineCreate(order_id=full.id, qty=qty, amount=Decimal(amount))
        )

    await ctx.document.command(NOTES).create(NoteCreate(order_ref=str(full.id)))

    exact = await evaluate(LINES_WITHIN_ORDER_TOTAL, ctx, {"order_id": full.id})
    counted = await evaluate(LINES_WITHIN_ORDER_QTY, ctx, {"order_id": full.id})

    assert exact.held
    assert (exact.observed, exact.against) == (Decimal("0.3"), Decimal("0.3"))
    assert isinstance(exact.observed, Decimal)
    assert (counted.observed, counted.against) == (3.0, 3.0)
    assert type(counted.observed) is float

    assert (await evaluate(ORDER_HAS_A_LINE, ctx, {"order_id": full.id})).held
    assert not (await evaluate(ORDER_HAS_A_LINE, ctx, {"order_id": empty.id})).held
    # A string reference binds the UUID id on the other side.
    assert (await evaluate(ORDER_HAS_A_NOTE, ctx, {"order_ref": str(full.id)})).held
    assert not (await evaluate(ORDER_HAS_A_NOTE, ctx, {"order_ref": str(empty.id)})).held

    oracle = compile_oracle(ORDER_HAS_A_LINE, LINES_WITHIN_ORDER_TOTAL, ORDER_HAS_A_NOTE)
    recorder = Recorder(seed=0)

    with bind_recorder(recorder):
        await oracle.observe(ctx)

    violations = check(recorder.history, list(oracle.invariants))
    totals = [
        event.fields["observed"]
        for event in recorder.history.of_kind(SYSTEM_INVARIANT_KIND)
        if event.fields["name"] == LINES_WITHIN_ORDER_TOTAL.name
    ]

    # Only the order with neither a line nor a note breaks a law; the note's string reference and
    # the order's UUID id are one scope, not two.
    assert [violation.invariant for violation in violations] == [
        ORDER_HAS_A_LINE.name,
        ORDER_HAS_A_NOTE.name,
    ]
    assert all(str(empty.id) in violation.message for violation in violations)
    # The order with no lines is a scope too: its lines total an exact zero.
    assert totals == [Decimal("0.3"), Decimal(0)]
    assert all(isinstance(total, Decimal) for total in totals)
