"""Controls for the derived-read differential, with a mock standing in for the real view.

The Postgres leg (``test_pg_derived_fidelity``) is where the comparison means something;
these pin the helper itself without Docker. A second mock context plays the "real" side, so
each case can put one side in a deliberately wrong state and check it is reported as itself:
a wrong hand-written join per row and per field, a refused read as a value rather than a
crash, and the pairing refusals that keep two unrelated seeds from being compared at all.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.conformity import DerivedReadField
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze.base.primitives import JsonDict
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze.testing import context_from_modules
from forze_dst.conformance import ReadRefused, run_derived_fidelity
from forze_mock import MockDepsModule, MockDerivedRegistry
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


# ----------------------- #


class TestAgreement:
    async def test_the_same_join_on_both_sides_agrees(self) -> None:
        report = await run_derived_fidelity(
            spec=RESOLVED, seed=_seed(_plan(RESOLVED)), real=_ctx(), mock=_ctx()
        )

        assert report.agrees
        assert report.fields == ("warehouse",)
        assert len(report.ids) == 3

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
        assert "| `warehouse` | 2 | 1 |" in rendered
        assert str(report.ids[1]) in rendered

    async def test_a_refused_read_is_a_value_not_a_crash(self) -> None:
        """An inner-join view drops a row whose reference is gone; the mock still has it."""

        class _StrictRead(ReadDocument):
            warehouse_id: UUID
            warehouse: _Ref

        strict = DocumentSpec(
            name="orders",
            read=_StrictRead,
            write=DocumentWriteTypes(domain=_Order, create_cmd=_OrderCreate),
            derived_read_fields={
                "warehouse": DerivedReadField(
                    source="warehouses", via="warehouse_id", fields=("id", "name")
                )
            },
        )
        orders = attrs.evolve(PLAN.specs[1], spec=strict, fixtures=PLAN.specs[1].fixtures[:2])
        plan = attrs.evolve(PLAN, specs=(PLAN.specs[0], orders))
        module = MockDepsModule()
        real = context_from_modules(module)

        async def seed_and_drop(ctx: ExecutionContext) -> SeedResult:
            result = await apply_seed(ctx, plan)

            if ctx is real:
                del module.state.documents["warehouses"][SOUTH]

            return result

        report = await run_derived_fidelity(spec=strict, seed=seed_and_drop, real=real, mock=_ctx())

        [mismatch] = report.mismatches
        assert mismatch.id == report.ids[1]
        assert mismatch.real == ReadRefused(kind="internal")
        assert mismatch.mock == {"id": str(SOUTH), "name": "South"}


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
