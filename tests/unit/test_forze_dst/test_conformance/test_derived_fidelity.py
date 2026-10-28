"""Controls for the derived-read differential, with a mock standing in for the real view.

The Postgres leg (``test_pg_derived_fidelity``) is where the comparison means something;
these pin the helper itself without Docker. A second mock context plays the "real" side, so
each case can put one side in a deliberately wrong state and check it is reported as itself:
a wrong hand-written join per row and per field, a refused read as a value rather than a
crash, and the pairing refusals that keep two unrelated seeds from being compared at all.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import attrs
import pytest
from pydantic import BaseModel, Field

from forze.application.contracts.conformity import DerivedReadField
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze.base.primitives import JsonDict
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze.testing import context_from_modules
from forze_dst.conformance import ReadRefused, Unserializable, run_derived_fidelity
from forze_mock import MockDepsModule, MockDerivedRegistry
from forze_mock.execution.configs import MockRouteConfig
from forze_mock.seeding import SeedPlan, SeedResult, SpecSeed
from forze_mock.seeding.apply import apply_seed

# ----------------------- #


class _Warehouse(Document):
    name: str


class _WarehouseCreate(CreateDocumentCmd):
    name: str


class _WarehouseRead(ReadDocument):
    name: str


WAREHOUSES = DocumentSpec(
    name="warehouses",
    read=_WarehouseRead,
    write=DocumentWriteTypes(domain=_Warehouse, create_cmd=_WarehouseCreate),
)


class _Ref(BaseModel):
    id: UUID
    name: str


class _Order(Document):
    warehouse_id: UUID | None = None


class _OrderCreate(CreateDocumentCmd):
    warehouse_id: UUID | None = None


class _OrderRead(ReadDocument):
    warehouse_id: UUID | None = None
    warehouse: _Ref | None = None


def _orders(derived: DerivedReadField | None) -> DocumentSpec:
    return DocumentSpec(
        name="orders",
        read=_OrderRead,
        write=DocumentWriteTypes(domain=_Order, create_cmd=_OrderCreate),
        derived_read_fields={"warehouse": derived},
    )


RESOLVED = _orders(
    DerivedReadField(source="warehouses", via="warehouse_id", fields=("id", "name"), optional=True)
)
MARKED = _orders(None)


class _StrictRead(ReadDocument):
    warehouse_id: UUID
    warehouse: _Ref


STRICT = DocumentSpec(
    name="orders",
    read=_StrictRead,
    write=DocumentWriteTypes(domain=_Order, create_cmd=_OrderCreate),
    derived_read_fields={
        "warehouse": DerivedReadField(
            source="warehouses", via="warehouse_id", fields=("id", "name")
        )
    },
)
"""Non-optional: a key whose target is gone is a refused read, not a ``None``."""

NORTH = UUID("00000000-0000-4000-8000-0000000000a1")
SOUTH = UUID("00000000-0000-4000-8000-0000000000a2")
NAMES = {NORTH: "North", SOUTH: "South"}

PLAN = SeedPlan(
    specs=(
        SpecSeed(
            spec=WAREHOUSES,
            fixtures=({"id": NORTH, "name": "North"}, {"id": SOUTH, "name": "South"}),
        ),
        SpecSeed(
            spec=MARKED,
            fixtures=({"warehouse_id": NORTH}, {"warehouse_id": SOUTH}, {"warehouse_id": None}),
        ),
    ),
    links={"orders": {"warehouse_id": None}},
)


def _plan(spec: DocumentSpec, **over: Any) -> SeedPlan:
    orders = attrs.evolve(PLAN.specs[1], spec=spec)
    return attrs.evolve(PLAN, specs=(PLAN.specs[0], orders), **over)


def _ctx(source: Any = None) -> ExecutionContext:
    registry = MockDerivedRegistry().on("orders", source) if source is not None else None
    return context_from_modules(MockDepsModule(derived_values=registry))


def _ref(row: JsonDict, names: dict[UUID, str]) -> dict[str, Any]:
    key = row.get("warehouse_id")

    if key is None:
        return {"warehouse": None}

    return {"warehouse": {"id": key, "name": names[UUID(str(key))]}}


def _seed(plan: SeedPlan):
    async def seed(ctx: ExecutionContext) -> SeedResult:
        return await apply_seed(ctx, plan)

    return seed


def _strict_plan() -> SeedPlan:
    orders = attrs.evolve(PLAN.specs[1], spec=STRICT, fixtures=PLAN.specs[1].fixtures[:2])
    return attrs.evolve(PLAN, specs=(PLAN.specs[0], orders))


def _seed_and_drop(plan: SeedPlan, *modules: MockDepsModule):
    """Seed, then delete the South warehouse from the stores of *modules* only."""

    async def seed(ctx: ExecutionContext) -> SeedResult:
        result = await apply_seed(ctx, plan)

        for module in modules:
            module.state.documents.get("warehouses", {}).pop(SOUTH, None)

        return result

    return seed


# ----------------------- #


class TestAgreement:
    async def test_the_same_join_on_both_sides_agrees(self) -> None:
        report = await run_derived_fidelity(
            spec=RESOLVED, seed=_seed(_plan(RESOLVED)), real=_ctx(), mock=_ctx()
        )

        assert report.agrees
        assert report.fields == ("warehouse",)
        assert len(report.ids) == 3
        [tally] = report.tallies
        assert (tally.compared, tally.null_on_both, tally.refused_on_both) == (2, 1, 0)

    async def test_a_tenant_aware_spec_is_seeded_and_read_in_the_tenant(self) -> None:
        """Both contexts carry their own identity, so the tenant is bound on each."""

        def tenanted() -> ExecutionContext:
            route = MockRouteConfig(tenant_aware=True)
            return context_from_modules(
                MockDepsModule(routes={"orders": route, "warehouses": route})
            )

        report = await run_derived_fidelity(
            spec=RESOLVED,
            seed=_seed(_plan(RESOLVED)),
            real=tenanted(),
            mock=tenanted(),
            tenant=TenantIdentity(tenant_id=uuid4()),
        )

        assert report.agrees
        assert report.tallies[0].compared == 2

    async def test_a_faithful_callback_agrees(self) -> None:
        source = lambda row: _ref(row, NAMES)  # noqa: E731
        report = await run_derived_fidelity(
            spec=MARKED, seed=_seed(PLAN), real=_ctx(source), mock=_ctx(source)
        )

        assert report.agrees


class TestDivergence:
    async def test_a_wrong_callback_is_reported_per_row_and_field(self) -> None:
        # The usual hand-written join mistake: one name for every reference.
        wrong = {NORTH: "North", SOUTH: "North"}
        report = await run_derived_fidelity(
            spec=MARKED,
            seed=_seed(PLAN),
            real=_ctx(lambda row: _ref(row, NAMES)),
            mock=_ctx(lambda row: _ref(row, wrong)),
        )

        assert not report.agrees
        [mismatch] = report.mismatches
        assert mismatch.field == "warehouse"
        assert mismatch.id == report.ids[1]
        assert mismatch.real == {"id": str(SOUTH), "name": "South"}
        assert mismatch.mock == {"id": str(SOUTH), "name": "North"}

        rendered = report.render_markdown()
        assert "| `warehouse` | 2 | 1 | 0 | 1 |" in rendered
        assert str(report.ids[1]) in rendered

    async def test_a_refused_read_is_a_value_not_a_crash(self) -> None:
        """An inner-join view drops a row whose reference is gone; the mock still has it."""

        module = MockDepsModule()
        report = await run_derived_fidelity(
            spec=STRICT,
            seed=_seed_and_drop(_strict_plan(), module),
            real=context_from_modules(module),
            mock=_ctx(),
        )

        [mismatch] = report.mismatches
        assert mismatch.id == report.ids[1]
        assert mismatch.real == ReadRefused(kind="internal")
        assert mismatch.mock == {"id": str(SOUTH), "name": "South"}

    async def test_a_non_framework_exception_is_a_value_too(self) -> None:
        armed = False

        def flaky(row: JsonDict) -> dict[str, Any]:
            if armed and str(row.get("warehouse_id")) == str(SOUTH):
                raise RuntimeError("callback bug")

            return _ref(row, NAMES)

        mock = _ctx(flaky)

        async def seed(ctx: ExecutionContext) -> SeedResult:
            nonlocal armed
            result = await apply_seed(ctx, PLAN)
            # Armed after seeding: the seed's own read-back must not trip it.
            armed = ctx is mock
            return result

        report = await run_derived_fidelity(
            spec=MARKED, seed=seed, real=_ctx(lambda row: _ref(row, NAMES)), mock=mock
        )

        [mismatch] = report.mismatches
        assert mismatch.id == report.ids[1]
        assert mismatch.mock == ReadRefused(kind="RuntimeError")


class _AnyRead(ReadDocument):
    warehouse_id: UUID | None = None
    warehouse: Any = None


class _HiddenRead(ReadDocument):
    warehouse_id: UUID | None = None
    warehouse: _Ref | None = Field(default=None, exclude=True)


def _marked(read: type[ReadDocument]) -> DocumentSpec:
    return attrs.evolve(MARKED, read=read)


class TestSerialization:
    async def test_an_unserializable_value_is_a_mismatch_not_a_crash(self) -> None:
        def odd(row: JsonDict) -> dict[str, Any]:
            if str(row.get("warehouse_id")) == str(SOUTH):
                return {"warehouse": object()}

            return _ref(row, NAMES)

        spec = _marked(_AnyRead)
        report = await run_derived_fidelity(
            spec=spec,
            seed=_seed(_plan(spec)),
            real=_ctx(lambda row: _ref(row, NAMES)),
            mock=_ctx(odd),
        )

        [mismatch] = report.mismatches
        assert mismatch.id == report.ids[1]
        assert isinstance(mismatch.mock, Unserializable)
        [tally] = report.tallies
        assert (tally.compared, tally.refused_on_both, tally.diverged) == (2, 0, 1)

    async def test_two_unserializable_values_never_agree(self) -> None:
        spec = _marked(_AnyRead)
        odd = lambda row: {"warehouse": object()}  # noqa: E731
        report = await run_derived_fidelity(
            spec=spec, seed=_seed(_plan(spec)), real=_ctx(odd), mock=_ctx(odd)
        )

        assert not report.agrees
        assert report.tallies[0].diverged == 3

    async def test_a_derived_field_excluded_from_serialization_is_refused(self) -> None:
        spec = _marked(_HiddenRead)
        source = lambda row: _ref(row, NAMES)  # noqa: E731

        with pytest.raises(CoreException, match="excluded from the read model"):
            await run_derived_fidelity(
                spec=spec, seed=_seed(_plan(spec)), real=_ctx(source), mock=_ctx(source)
            )


class TestVacuousAgreement:
    async def test_a_row_refused_on_both_sides_agrees_but_is_not_compared(self) -> None:
        real, mock = MockDepsModule(), MockDepsModule()
        report = await run_derived_fidelity(
            spec=STRICT,
            seed=_seed_and_drop(_strict_plan(), real, mock),
            real=context_from_modules(real),
            mock=context_from_modules(mock),
        )

        # `agrees` still means "no mismatch"; the tally says how little that covers.
        assert report.agrees
        [tally] = report.tallies
        assert (tally.compared, tally.refused_on_both, tally.diverged) == (1, 1, 0)
        assert "| `warehouse` | 1 | 0 | 1 | 0 |" in report.render_markdown()

    async def test_a_field_null_on_every_row_compares_nothing(self) -> None:
        nothing = lambda row: {"warehouse": None}  # noqa: E731
        report = await run_derived_fidelity(
            spec=MARKED, seed=_seed(PLAN), real=_ctx(nothing), mock=_ctx(nothing)
        )

        assert report.agrees
        assert report.tallies[0].compared == 0
        assert report.tallies[0].null_on_both == 3


class TestPairing:
    async def test_a_spec_without_derived_fields_is_refused(self) -> None:
        with pytest.raises(CoreException, match="declares no derived read fields"):
            await run_derived_fidelity(spec=WAREHOUSES, seed=_seed(PLAN), real=_ctx(), mock=_ctx())

    async def test_a_seed_with_no_rows_of_the_spec_is_refused(self) -> None:
        plan = attrs.evolve(PLAN, specs=(PLAN.specs[0],), links={})

        with pytest.raises(CoreException, match="creates no rows"):
            await run_derived_fidelity(spec=RESOLVED, seed=_seed(plan), real=_ctx(), mock=_ctx())

    async def test_unpaired_ids_are_refused(self) -> None:
        """On the wall clock the two stores mint different ids, so no row has a partner."""

        with pytest.raises(CoreException, match="minted different ids"):
            await run_derived_fidelity(
                spec=RESOLVED,
                seed=_seed(_plan(RESOLVED, instant=None)),
                real=_ctx(),
                mock=_ctx(),
            )
