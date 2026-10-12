"""A ``Decimal`` property filter matches whichever text the property was stored as.

A forze model writes a ``Decimal`` in fixed point (``0.00000000``); a plain pydantic model, and
data written before forze did, holds ``str``'s text (``0E-8``). Graph properties are stored as
that text, so one scenario runs both kinds of model on each engine and filters every value.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel

from forze.application.contracts.graph import GraphModuleSpec, GraphNodeSpec, VertexRef
from forze.base.serialization import decimal_text
from forze.domain.models import BaseDTO

# ----------------------- #

AMOUNTS = (Decimal("0E-8"), Decimal("2.5E-7"), Decimal("1E+3"), Decimal("1.5"))


class ModeledAmount(BaseDTO):
    id: str
    amount: Decimal
    amounts: list[Decimal] = []
    label: str = "a"


class PlainAmount(BaseModel):
    id: str
    amount: Decimal
    amounts: list[Decimal] = []
    label: str = "a"


class KeyedAmount(BaseDTO):
    id: Decimal


DECIMAL_SPEC = GraphModuleSpec(
    name="decimal_filters",
    nodes=(
        GraphNodeSpec(name="DecModeled", read=ModeledAmount, create=ModeledAmount),
        GraphNodeSpec(name="DecPlain", read=PlainAmount, create=PlainAmount),
        GraphNodeSpec(name="DecKeyed", read=KeyedAmount, create=KeyedAmount),
    ),
    edges=(),
)


async def assert_decimal_filters(command: Any, query: Any) -> None:
    """Store every amount through both models, then filter each one back."""

    listed = [Decimal("1E+3"), Decimal("0E-8")]

    for i, amount in enumerate(AMOUNTS):
        await command.create_vertex("DecModeled", ModeledAmount(id=f"m{i}", amount=amount))
        await command.create_vertex("DecPlain", PlainAmount(id=f"p{i}", amount=amount))

    await command.create_vertex("DecModeled", ModeledAmount(id="ml", amount=1, amounts=listed))
    await command.create_vertex("DecPlain", PlainAmount(id="pl", amount=1, amounts=listed))

    for kind in ("DecModeled", "DecPlain"):
        for amount in AMOUNTS:
            found = await query.count_vertices(kind, property_filter={"amount": amount})
            assert found == 1, (kind, amount, found)

        # A list value is compared whole, its Decimals in the text they were stored as.
        found = await query.count_vertices(kind, property_filter={"amounts": listed})
        assert found == 1, (kind, found)

        # A list never matches a scalar that is one of its items.
        found = await query.count_vertices(kind, property_filter={"label": ["a", "x"]})
        assert found == 0, (kind, found)

    # A forze model's Decimal key is its fixed-point text.
    await command.create_vertex("DecKeyed", KeyedAmount(id=Decimal("1E+3")))
    assert await query.vertex_exists(VertexRef(kind="DecKeyed", key=decimal_text(Decimal("1E+3"))))
