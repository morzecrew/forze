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
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.conformity import DerivedReadField
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.application.execution.port_proxy_base import PortProxy
from forze.base.exceptions import CoreException
from forze.base.primitives import JsonDict
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule
from forze_mock.adapters import MockDocumentAdapter, MockState
from forze_mock.adapters._derived import (  # pyright: ignore[reportPrivateUsage]
    ResolvedDerivedRead,
    staged_derived,
)
from forze_mock.seeding import SeedPlan, SpecSeed
from forze_mock.seeding.apply import (  # pyright: ignore[reportPrivateUsage]
    _mock_adapter,
    apply_seed,
)

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


class _AggRead(ReadDocument):
    supplier_id: UUID
    supplier: str
    qty: int = 1


def _agg_spec(derived: dict[str, object]) -> DocumentSpec:
    return DocumentSpec(
        name="orders",
        read=_AggRead,
        write=DocumentWriteTypes(domain=_Order, create_cmd=_OrderCreate),
        derived_read_fields=derived,  # type: ignore[arg-type]
    )


_AGG_MARKED = _agg_spec({"supplier": None})
_AGG_RESOLVED = _agg_spec(
    {"supplier": DerivedReadField(source="suppliers", via="supplier_id", field="name")}
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


# ----------------------- #
# Round-1 review findings


class TestProjectionScope:
    """A projection that does not name a marked field must not be refused for it."""

    async def test_a_projection_excluding_the_marked_field_is_served(self) -> None:
        state = MockState()
        pk = uuid4()
        state.documents["orders"] = {
            pk: {
                "id": str(pk),
                "rev": 1,
                "created_at": "2026-01-01T00:00:00Z",
                "last_update_at": "2026-01-01T00:00:00Z",
                "supplier_id": str(SUPPLIER_ID),
            }
        }
        adapter = MockDocumentAdapter(
            spec=ORDERS,
            state=state,
            namespace="orders",
            read_model=_OrderRead,
            domain_model=_Order,
            derived_marked=frozenset({"supplier", "stock_quantity"}),
        )

        page = await adapter.project_many(["id", "supplier_id"])
        assert [row["supplier_id"] for row in page.hits] == [str(SUPPLIER_ID)]

        # Naming it back brings the refusal back — the scope is the projection, not a
        # blanket relaxation.
        with pytest.raises(CoreException, match="derived_unsupplied"):
            await adapter.project_many(["id", "supplier"])


class TestLenientComposition:
    """`read_conformity="lenient"` must not auto-derive a field that is declared derived."""

    def test_a_defaulted_derived_field_under_lenient_conformity_builds(self) -> None:
        class _LenientRead(ReadDocument):
            supplier_id: UUID
            total: int = 0

        spec = DocumentSpec(
            name="orders",
            read=_LenientRead,
            read_conformity="lenient",
            derived_read_fields={"total": None},
        )

        assert "total" not in spec.resolved_lenient_read_fields
        assert sorted(spec.derived_read_fields) == ["total"]

    def test_an_explicit_lenient_overlap_is_still_refused(self) -> None:
        class _LenientRead(ReadDocument):
            supplier_id: UUID
            total: int = 0

        with pytest.raises(CoreException, match="cannot be both derived"):
            DocumentSpec(
                name="orders",
                read=_LenientRead,
                lenient_read_fields={"total"},
                derived_read_fields={"total": None},
            )


class TestStagedValues:
    """The window `create` opens by draining events before it returns.

    `create` stores the row, awaits `drain_domain_events`, and only then returns — so a
    handler reading the new aggregate runs before the seeder can write its derived
    values, and would see a required marked field as missing. Staging makes those values
    read as though they were already on the row for the duration of the create.

    Tested at the mechanism rather than by wiring a dispatcher: what has to hold is that
    a staged value satisfies the read and an unstaged one does not.
    """

    def _adapter(self, state: MockState) -> MockDocumentAdapter:
        return MockDocumentAdapter(
            spec=ORDERS,
            state=state,
            namespace="orders",
            read_model=_OrderRead,
            domain_model=_Order,
            derived_marked=frozenset({"supplier", "stock_quantity"}),
        )

    def _bare_row(self, pk: UUID) -> dict[str, object]:
        return {
            "id": str(pk),
            "rev": 1,
            "created_at": "2026-01-01T00:00:00Z",
            "last_update_at": "2026-01-01T00:00:00Z",
            "supplier_id": str(SUPPLIER_ID),
        }

    async def test_a_staged_value_satisfies_the_read(self) -> None:
        state = MockState()
        pk = uuid4()
        state.documents["orders"] = {pk: self._bare_row(pk)}
        adapter = self._adapter(state)

        # Unstaged: the row carries nothing, so the read refuses.
        with pytest.raises(CoreException, match="derived_unsupplied"):
            await adapter.get(pk)

        with staged_derived({("orders", pk): dict(DERIVED)}):
            row = await adapter.get(pk)
            assert row.supplier.name == "Acme"
            assert row.stock_quantity == 12.5

        # And the staging is scoped: outside it the row is bare again.
        with pytest.raises(CoreException, match="derived_unsupplied"):
            await adapter.get(pk)

    async def test_a_stored_value_wins_over_a_staged_one(self) -> None:
        """Staging covers a window; it does not override what was persisted."""

        state = MockState()
        pk = uuid4()
        stored = {**self._bare_row(pk), **DERIVED}
        stored["stock_quantity"] = 99.0
        state.documents["orders"] = {pk: stored}
        adapter = self._adapter(state)

        with staged_derived({("orders", pk): {"stock_quantity": 12.5}}):
            assert (await adapter.get(pk)).stock_quantity == 99.0

    async def test_a_malformed_id_on_the_row_does_not_match_staging(self) -> None:
        """Probed rather than assumed reachable: a stored row can hold anything."""

        state = MockState()
        pk = uuid4()
        row = self._bare_row(pk)
        row["id"] = "not-a-uuid"
        state.documents["orders"] = {pk: row}
        adapter = self._adapter(state)

        with (
            staged_derived({("orders", pk): dict(DERIVED)}),
            pytest.raises(CoreException, match="derived_unsupplied"),
        ):
            await adapter.get(pk)

    async def test_staging_another_row_does_not_satisfy_this_one(self) -> None:
        state = MockState()
        pk = uuid4()
        state.documents["orders"] = {pk: self._bare_row(pk)}
        adapter = self._adapter(state)

        with (
            staged_derived({("orders", uuid4()): dict(DERIVED)}),
            pytest.raises(CoreException, match="derived_unsupplied"),
        ):
            await adapter.get(pk)


class TestQueryAxesAgreeWithTheSpec:
    """The runtime validators must refuse what `filterable_fields()` already excludes.

    Without this the mock accepts a direct filter the governed path refuses — and for a
    *resolved* field it is worse than inconsistent: the value exists only after
    hydration, so the filter runs against a row that does not have it and matches
    nothing. Silently.
    """

    def _adapter(self, state: MockState) -> MockDocumentAdapter:
        return MockDocumentAdapter(
            spec=ORDERS,
            state=state,
            namespace="orders",
            read_model=_OrderRead,
            domain_model=_Order,
            derived_marked=frozenset({"supplier", "stock_quantity"}),
        )

    def _seeded_state(self) -> tuple[MockState, UUID]:
        state = MockState()
        pk = uuid4()
        state.documents["orders"] = {
            pk: {
                "id": str(pk),
                "rev": 1,
                "created_at": "2026-01-01T00:00:00Z",
                "last_update_at": "2026-01-01T00:00:00Z",
                "supplier_id": str(SUPPLIER_ID),
                **DERIVED,
            }
        }
        return state, pk

    def test_the_spec_excludes_them_from_every_axis(self) -> None:
        for axis in (
            ORDERS.filterable_fields(),
            ORDERS.sortable_fields(),
            ORDERS.aggregatable_fields(),
        ):
            assert "supplier" not in axis
            assert "stock_quantity" not in axis
            assert "supplier_id" in axis

    async def test_a_filter_on_a_derived_field_is_refused(self) -> None:
        state, _pk = self._seeded_state()

        # The value is right there on the stored row, which is exactly why this has to
        # be a policy refusal rather than a lookup that happens to miss.
        with pytest.raises(CoreException, match="field_not_on_read_model"):
            await self._adapter(state).find_many(filters={"$values": {"supplier": {"$eq": "Acme"}}})

    async def test_a_sort_on_a_derived_field_is_refused(self) -> None:
        state, _pk = self._seeded_state()

        with pytest.raises(CoreException):
            await self._adapter(state).find_many(sorts={"supplier": "asc"})

    async def test_a_cursor_sort_on_a_derived_field_is_refused(self) -> None:
        state, _pk = self._seeded_state()

        with pytest.raises(CoreException):
            await self._adapter(state).find_cursor(sorts={"supplier": "asc"})

    async def test_the_stored_join_key_stays_queryable(self) -> None:
        """The exclusion is the derived field, not everything near it."""

        state, _pk = self._seeded_state()
        page = await self._adapter(state).find_many(
            filters={"$values": {"supplier_id": {"$eq": SUPPLIER_ID}}}
        )

        assert [row.supplier.name for row in page.hits] == ["Acme"]


class TestAggregatesAgreeWithTheSpec:
    """The aggregate branch runs before hydration and skipped validation entirely.

    `aggregatable_fields()` excludes derived names, and the aggregate arm of
    `_mock_offset_page` reached `_aggregate_docs` without any field check — so a group
    key or a metric source could name one. For a marked field that groups by whatever
    the row happens to carry; for a resolved field it groups every document under
    `None`, because the value does not exist until the read.
    """

    def _adapter(self, state: MockState, spec: DocumentSpec, **kw: Any) -> MockDocumentAdapter:
        return MockDocumentAdapter(
            spec=spec,
            state=state,
            namespace="orders",
            read_model=_AggRead,
            domain_model=_Order,
            **kw,
        )

    def _state(self, *, with_supplier: bool) -> MockState:
        state = MockState()
        pk = uuid4()
        row: dict[str, object] = {
            "id": str(pk),
            "rev": 1,
            "created_at": "2026-01-01T00:00:00Z",
            "last_update_at": "2026-01-01T00:00:00Z",
            "supplier_id": str(SUPPLIER_ID),
            "qty": 3,
        }

        if with_supplier:
            row["supplier"] = "Acme"

        state.documents["orders"] = {pk: row}
        state.documents["suppliers"] = {
            SUPPLIER_ID: {
                "id": str(SUPPLIER_ID),
                "rev": 1,
                "created_at": "2026-01-01T00:00:00Z",
                "last_update_at": "2026-01-01T00:00:00Z",
                "name": "Acme",
            }
        }
        return state

    async def test_a_group_key_may_not_be_derived(self) -> None:
        adapter = self._adapter(
            self._state(with_supplier=True),
            _AGG_MARKED,
            derived_marked=frozenset({"supplier"}),
        )

        with pytest.raises(CoreException, match="field_not_aggregatable"):
            await adapter.aggregate_many(
                {"$groups": {"s": "supplier"}, "$computed": {"n": {"$count": {}}}}
            )

    async def test_a_metric_source_may_not_be_derived(self) -> None:
        adapter = self._adapter(
            self._state(with_supplier=True),
            _AGG_MARKED,
            derived_marked=frozenset({"supplier"}),
        )

        with pytest.raises(CoreException, match="field_not_aggregatable"):
            await adapter.aggregate_many(
                {"$groups": {"k": "supplier_id"}, "$computed": {"m": {"$max": "supplier"}}}
            )

    async def test_a_per_metric_filter_may_not_name_a_derived_field(self) -> None:
        adapter = self._adapter(
            self._state(with_supplier=True),
            _AGG_MARKED,
            derived_marked=frozenset({"supplier"}),
        )

        with pytest.raises(CoreException):
            await adapter.aggregate_many(
                {
                    "$groups": {"k": "supplier_id"},
                    "$computed": {
                        "n": {
                            "$sum": {
                                "field": "qty",
                                "filter": {"$values": {"supplier": {"$eq": "Acme"}}},
                            }
                        }
                    },
                }
            )

    async def test_a_resolved_group_key_is_refused_rather_than_grouped_as_none(self) -> None:
        """The serious half: the row has no `supplier` until the join runs."""

        adapter = self._adapter(
            self._state(with_supplier=False),
            _AGG_RESOLVED,
            derived={
                "supplier": ResolvedDerivedRead(
                    namespace="suppliers", via="supplier_id", field="name"
                )
            },
        )

        with pytest.raises(CoreException, match="field_not_aggregatable"):
            await adapter.aggregate_many(
                {"$groups": {"s": "supplier"}, "$computed": {"n": {"$count": {}}}}
            )

    async def test_a_stored_field_still_aggregates(self) -> None:
        """The exclusion is the derived field, not the aggregate surface."""

        adapter = self._adapter(
            self._state(with_supplier=True),
            _AGG_MARKED,
            derived_marked=frozenset({"supplier"}),
        )

        page = await adapter.aggregate_many(
            {"$groups": {"k": "supplier_id"}, "$computed": {"total": {"$sum": "qty"}}}
        )

        assert [dict(row)["total"] for row in page.hits] == [3]


class TestARegisteredSource:
    """A stand-in for the relation, consulted for every row rather than written onto one.

    What seeding cannot do: a row created after the seed ran — every row under simulation —
    has no seeded value, and a marked field travels through no command. The source is the
    only thing that can serve those, and its precedence is what keeps a deliberately
    written value authoritative.
    """

    def _adapter(
        self,
        state: MockState,
        source: Any,
    ) -> MockDocumentAdapter:
        return MockDocumentAdapter(
            spec=ORDERS,
            state=state,
            namespace="orders",
            read_model=_OrderRead,
            domain_model=_Order,
            derived_marked=frozenset({"supplier", "stock_quantity"}),
            derived_source=source,
        )

    def _row(self, pk: UUID, **extra: object) -> dict[str, object]:
        return {
            "id": str(pk),
            "rev": 1,
            "created_at": "2026-01-01T00:00:00Z",
            "last_update_at": "2026-01-01T00:00:00Z",
            "supplier_id": str(SUPPLIER_ID),
            **extra,
        }

    def _view(self, row: JsonDict) -> dict[str, Any]:
        return {
            "supplier": {
                "id": str(SUPPLIER_ID),
                "rev": 1,
                "name": f"from-view-{str(row['id'])[:4]}",
                "number_id": 1,
            },
            "stock_quantity": 1.0,
        }

    async def test_a_source_serves_a_row_nothing_seeded(self) -> None:
        state = MockState()
        pk = uuid4()
        state.documents["orders"] = {pk: self._row(pk)}

        row = await self._adapter(state, self._view).get(pk)

        assert row.supplier.name.startswith("from-view-")
        assert row.stock_quantity == 1.0

    async def test_it_is_per_row(self) -> None:
        """The distinction from `SpecSeed.derived`, which is one value for every row."""

        state = MockState()
        first, second = uuid4(), uuid4()
        state.documents["orders"] = {
            first: self._row(first),
            second: self._row(second),
        }

        page = await self._adapter(state, self._view).find_many()

        assert len({row.supplier.name for row in page.hits}) == 2

    async def test_the_stored_row_outranks_the_source(self) -> None:
        """A seeded or persisted value is what somebody wrote on purpose."""

        state = MockState()
        pk = uuid4()
        state.documents["orders"] = {pk: self._row(pk, **DERIVED)}

        row = await self._adapter(state, self._view).get(pk)

        assert row.supplier.name == "Acme"
        assert row.stock_quantity == 12.5

    async def test_a_staged_value_outranks_the_source_too(self) -> None:
        """Staging covers the create window, and a source must not reopen it."""

        state = MockState()
        pk = uuid4()
        state.documents["orders"] = {pk: self._row(pk)}
        adapter = self._adapter(state, self._view)

        with staged_derived({("orders", pk): dict(DERIVED)}):
            row = await adapter.get(pk)

        assert row.supplier.name == "Acme"

    async def test_it_cannot_fill_a_field_the_spec_did_not_declare_derived(self) -> None:
        """A source is a stand-in for the relation, not a hook that rewrites the row.

        The read model's own defaults are the case that bites: an undeclared name absent
        from the stored row would otherwise take the source's value silently, so a typo in
        a source would read as data. Extra keys pydantic simply drops prove nothing here —
        it drops them with or without the scoping.
        """

        class _WithDefault(ReadDocument):
            supplier_id: UUID
            supplier: _SupplierRef
            stock_quantity: float
            note: str = "from the model"

        spec = DocumentSpec(
            name="orders",
            read=_WithDefault,
            write=DocumentWriteTypes(domain=_Order, create_cmd=_OrderCreate),
            derived_read_fields={"supplier": None, "stock_quantity": None},
        )
        state = MockState()
        pk = uuid4()
        state.documents["orders"] = {pk: self._row(pk)}

        def noisy(row: JsonDict) -> dict[str, Any]:
            return {**self._view(row), "note": "from the source"}

        adapter = MockDocumentAdapter(
            spec=spec,
            state=state,
            namespace="orders",
            read_model=_WithDefault,
            domain_model=_Order,
            derived_marked=frozenset({"supplier", "stock_quantity"}),
            derived_source=noisy,
        )

        row = await adapter.get(pk)

        assert row.note == "from the model"
        # And the declared ones still arrive, so this is scoping rather than a source
        # that was ignored wholesale.
        assert row.stock_quantity == 1.0

    async def test_a_partial_source_still_refuses_the_rest(self) -> None:
        """Supplying one of two required marked fields is not supplying them."""

        state = MockState()
        pk = uuid4()
        state.documents["orders"] = {pk: self._row(pk)}

        def half(row: JsonDict) -> dict[str, Any]:
            return {"supplier": self._view(row)["supplier"]}

        with pytest.raises(CoreException, match="derived_unsupplied"):
            await self._adapter(state, half).get(pk)

    async def test_a_projection_hands_the_source_the_whole_row(self) -> None:
        """A source reads the fields it joins on, and a narrow projection must not hide them.

        The projection is applied *after* hydration, so the source sees the stored row
        rather than the caller's field list. Pinned because the reverse order would break
        every source that reads a join key the caller did not ask for — silently, and only
        on the projection path.
        """

        state = MockState()
        pk = uuid4()
        state.documents["orders"] = {pk: self._row(pk)}
        seen: list[frozenset[str]] = []

        def watching(row: JsonDict) -> dict[str, Any]:
            seen.append(frozenset(row))
            return self._view(row)

        page = await self._adapter(state, watching).project_many(["id", "supplier"])

        assert seen and "supplier_id" in seen[0]
        assert dict(page.hits[0])["supplier"]["name"].startswith("from-view-")

    async def test_no_source_keeps_the_refusal(self) -> None:
        """The default posture: an unsupplied value stays discoverable."""

        state = MockState()
        pk = uuid4()
        state.documents["orders"] = {pk: self._row(pk)}

        with pytest.raises(CoreException, match="derived_unsupplied"):
            await self._adapter(state, None).get(pk)


class TestTheSeederFindsTheAdapterBehindAWrap:
    """`SpecSeed.derived` writes through a port the runtime has wrapped.

    A handler never holds the adapter itself — tracing, resilience and a simulation's fault
    interceptors each wrap it — so the seeder's narrowing has to look behind the chain. The
    end-to-end case is under simulation, where the wrap is the real one; these pin the
    lookup itself, including what it still refuses.
    """

    def test_it_unwraps_a_proxy(self) -> None:
        adapter = MockDocumentAdapter(
            spec=ORDERS,
            state=MockState(),
            namespace="orders",
            read_model=_OrderRead,
            domain_model=_Order,
        )
        wrapped = PortProxy(inner=PortProxy(inner=adapter))

        assert _mock_adapter(wrapped, "orders") is adapter

    def test_it_still_refuses_a_port_that_is_not_the_mock(self) -> None:
        """The narrowing exists because the seeder writes onto the store directly."""

        with pytest.raises(CoreException, match="needs the mock document adapter"):
            _mock_adapter(PortProxy(inner=object()), "orders")
