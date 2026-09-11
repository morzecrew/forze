"""A derived read field may be *marked* without declaring how to resolve it.

Marking is the floor, and the reason it is the floor is coverage: a nested reference
object, a ``COALESCE`` over sibling rows and a ``CASE`` expression are all the same
declaration, because none of them is computed here. A joined column can additionally be
resolved (see ``test_document_derived_read``), which is the narrow case.

The value of a marked field comes from the stored row, which is where ``SpecSeed.derived``
puts it. That is fixture data and not a derivation — nothing recomputes it — and for
testing the handler around the read, which is what these aggregates need, that is the
whole requirement.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.conformity import DerivedReadField
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.exceptions import CoreException
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule
from forze_mock.seeding import SeedPlan, SpecSeed
from forze_mock.seeding.apply import apply_seed

# ----------------------- #
# The origin application's real shape: a nested reference and an aggregate total.


class _SupplierRef(BaseModel):
    id: UUID
    rev: int
    name: str
    number_id: int


class _Order(Document):
    supplier_id: UUID


class _OrderCreate(CreateDocumentCmd):
    supplier_id: UUID


class _OrderRead(ReadDocument):
    supplier_id: UUID
    supplier: _SupplierRef
    """Required nested object — `entity_json(s)` in the view. No join expresses it."""

    stock_quantity: float
    """`COALESCE(sa.free_quantity, 0)` — an aggregate over sibling rows."""


ORDERS = DocumentSpec(
    name="orders",
    read=_OrderRead,
    write=DocumentWriteTypes(domain=_Order, create_cmd=_OrderCreate),
    derived_read_fields={"supplier": None, "stock_quantity": None},
)

SUPPLIER_ID = UUID("00000000-0000-4000-8000-000000000001")
DERIVED = {
    "supplier": {"id": str(SUPPLIER_ID), "rev": 1, "name": "Acme", "number_id": 7},
    "stock_quantity": 12.5,
}


def _plan(*, count: int = 2, derived: dict[str, object] | None = None) -> SeedPlan:
    return SeedPlan(
        specs=(
            SpecSeed(
                spec=ORDERS,
                count=count,
                derived=DERIVED if derived is None else derived,
            ),
        ),
        rng_seed=7,
    )


async def _seeded(plan: SeedPlan, body: Callable[[Any, Any], Awaitable[None]]) -> None:
    """Seed *plan* and run *body* inside the runtime scope.

    A callback rather than a generator: an `async for` helper that yields exits its
    scope in a different context when the body raises, and the contextvar reset then
    fails instead of the assertion.
    """

    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        result = await apply_seed(ctx, plan)
        await body(ctx, result)


# ----------------------- #


class TestMarkedAndSeeded:
    async def test_a_required_nested_object_round_trips(self) -> None:
        """The case that could not round-trip at all before: no default, no join."""

        async def body(ctx: Any, result: Any) -> None:
            row = await ctx.doc.query(ORDERS).get(result.ids["orders"][0])

            assert row.supplier.name == "Acme"
            assert row.supplier.number_id == 7
            assert row.stock_quantity == 12.5

        await _seeded(_plan(), body)

    async def test_every_read_path_serves_it(self) -> None:
        async def body(ctx: Any, result: Any) -> None:
            query = ctx.doc.query(ORDERS)

            page = await query.find_many()
            assert [r.supplier.name for r in page.hits] == ["Acme", "Acme"]

            many = await query.get_many(list(result.ids["orders"]))
            assert [r.stock_quantity for r in many] == [12.5, 12.5]

            cursor = await query.find_cursor(cursor={"limit": 10})
            assert len(cursor.hits) == 2

        await _seeded(_plan(), body)

    async def test_without_the_seeded_value_the_read_still_refuses(self) -> None:
        """The marker declares the field; it does not invent a value for it."""

        async def body(ctx: Any, result: Any) -> None:
            # `stock_quantity` was declared derived and never supplied. The refusal
            # names the declaration, rather than letting a raw pydantic error escape.
            with pytest.raises(CoreException, match="derived_unsupplied"):
                await ctx.doc.query(ORDERS).get(result.ids["orders"][0])

        await _seeded(_plan(derived={"supplier": DERIVED["supplier"]}), body)


class TestSeedDeterminismIsConditional:
    async def test_the_wall_clock_plan_mints_fresh_ids(self) -> None:
        """`instant=None` opts out of the pinned clock *and* the seeded entropy."""

        runs: list[tuple[UUID, ...]] = []

        async def body(_ctx: Any, result: Any) -> None:
            runs.append(result.ids["orders"])

        for _ in range(2):
            plan = SeedPlan(
                specs=(SpecSeed(spec=ORDERS, count=1, derived=DERIVED),),
                rng_seed=7,
                instant=None,
            )
            await _seeded(plan, body)

        assert runs[0] != runs[1]


class TestOptionalMarkers:
    """An optional marked field needs no value: absence is what `None` is for."""

    async def test_an_unsupplied_optional_marker_reads_as_none(self) -> None:
        class _OptRead(ReadDocument):
            supplier_id: UUID
            supplier: _SupplierRef | None = None
            note: str = ""

        spec = DocumentSpec(
            name="orders",
            read=_OptRead,
            write=DocumentWriteTypes(domain=_Order, create_cmd=_OrderCreate),
            derived_read_fields={"supplier": None, "note": None},
        )

        async def body(ctx: Any, result: Any) -> None:
            row = await ctx.doc.query(spec).get(result.ids["orders"][0])

            assert row.supplier is None
            assert row.note == ""

        plan = SeedPlan(specs=(SpecSeed(spec=spec, count=1),), rng_seed=7)
        await _seeded(plan, body)


class TestSeedGuards:
    def test_an_undeclared_derived_value_is_refused(self) -> None:
        with pytest.raises(CoreException, match="does not declare"):
            SpecSeed(spec=ORDERS, count=1, derived={"ghost": 1})

    async def test_ids_are_deterministic_across_runs(self) -> None:
        """The id is minted by the seeder here, so determinism has to be re-proven.

        It holds because `SeedPlan.instant` defaults to a pinned clock, which binds the
        seeded entropy source the mint draws from — not because minting is deterministic
        in itself. The next case pins that dependency, so this one cannot be read as an
        unconditional guarantee.
        """

        runs: list[tuple[UUID, ...]] = []

        async def body(_ctx: Any, result: Any) -> None:
            runs.append(result.ids["orders"])

        for _ in range(2):
            await _seeded(_plan(), body)

        assert runs[0] == runs[1]


class TestDeclaration:
    def test_a_partly_resolved_declaration_is_refused(self) -> None:
        with pytest.raises(CoreException, match="partly resolved"):
            DerivedReadField(source="suppliers", via="supplier_id")

    def test_optional_without_a_join_is_refused(self) -> None:
        with pytest.raises(CoreException, match="optional without a join"):
            DerivedReadField(optional=True)

    def test_a_marker_carries_no_join(self) -> None:
        assert DerivedReadField().resolved is False
        assert DerivedReadField(source="s", via="v", field="f").resolved is True

    def test_a_marked_field_is_still_excluded_from_the_query_axes(self) -> None:
        assert "supplier" not in ORDERS.filterable_fields()
        assert "stock_quantity" not in ORDERS.sortable_fields()
        assert "supplier_id" in ORDERS.filterable_fields()
