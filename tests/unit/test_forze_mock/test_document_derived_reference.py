"""Resolved *expanded references*: ``DerivedReadField(fields=...)`` against the mock.

The most common derived field in a view-backed read model is a reference expanded from a
few columns of one joined row — ``warehouse: {id, name}``. A single-field join cannot build
it, so before ``fields`` it had to be marked and every suite rewrote the join by hand. These
cases pin the multi-field mode to the same one-hop rule and the same ``optional`` semantics
as the single-field one (``test_document_derived_read``), which stays unchanged.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.conformity import DerivedReadField
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.base.exceptions import CoreException
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze.testing import context_from_modules
from forze_mock import MockDepsModule
from forze_mock.adapters import MockDocumentAdapter, MockState
from forze_mock.adapters._derived import ResolvedDerivedRead  # pyright: ignore[reportPrivateUsage]
from forze_mock.execution.factories import ConfigurableMockDocument
from forze_mock.seeding import SeedPlan, SpecSeed
from forze_mock.seeding.apply import apply_seed

# ----------------------- #


class Warehouse(Document):
    name: str
    code: str


class WarehouseCreate(CreateDocumentCmd):
    name: str
    code: str


class WarehouseRead(ReadDocument):
    name: str
    code: str


WAREHOUSES = DocumentSpec(
    name="warehouses",
    read=WarehouseRead,
    write=DocumentWriteTypes(domain=Warehouse, create_cmd=WarehouseCreate),
)


class WarehouseRef(BaseModel):
    id: UUID
    name: str


class Order(Document):
    warehouse_id: UUID | None = None


class OrderCreate(CreateDocumentCmd):
    warehouse_id: UUID | None = None


class OrderRead(ReadDocument):
    warehouse_id: UUID
    warehouse: WarehouseRef
    """Required and expanded from the joined row — the shape `field=` could not build."""


class OrderReadOptional(ReadDocument):
    warehouse_id: UUID | None = None
    warehouse: WarehouseRef | None = None


class OrderReadMapping(ReadDocument):
    warehouse_id: UUID
    warehouse: dict[str, Any]


REF = DerivedReadField(source="warehouses", via="warehouse_id", fields=("id", "name"))

ORDERS = DocumentSpec(
    name="orders",
    read=OrderRead,
    write=DocumentWriteTypes(domain=Order, create_cmd=OrderCreate),
    derived_read_fields={"warehouse": REF},
)

ORDERS_MAPPING = DocumentSpec(
    name="orders",
    read=OrderReadMapping,
    write=DocumentWriteTypes(domain=Order, create_cmd=OrderCreate),
    derived_read_fields={"warehouse": REF},
)

ORDERS_OPTIONAL = DocumentSpec(
    name="orders",
    read=OrderReadOptional,
    write=DocumentWriteTypes(domain=Order, create_cmd=OrderCreate),
    derived_read_fields={
        "warehouse": DerivedReadField(
            source="warehouses", via="warehouse_id", fields=("id", "name"), optional=True
        ),
    },
)


# ----------------------- #


def _orders(
    state: MockState,
    *,
    optional: bool = False,
    spec: DocumentSpec | None = None,
) -> MockDocumentAdapter:
    spec = spec or (ORDERS_OPTIONAL if optional else ORDERS)

    return MockDocumentAdapter(
        spec=spec,
        state=state,
        namespace="orders",
        read_model=spec.read,
        domain_model=Order,
        derived={
            "warehouse": ResolvedDerivedRead(
                namespace="warehouses",
                via="warehouse_id",
                field=("id", "name"),
                optional=optional,
            )
        },
    )


def _stamp() -> str:
    return datetime.now(UTC).isoformat()


def _warehouse(state: MockState, *, name: str = "North", code: str = "N1") -> UUID:
    pk = uuid4()
    state.documents.setdefault("warehouses", {})[pk] = {
        "id": str(pk),
        "rev": 1,
        "created_at": _stamp(),
        "last_update_at": _stamp(),
        "name": name,
        "code": code,
    }
    return pk


def _order(state: MockState, *, warehouse_id: UUID | None) -> UUID:
    pk = uuid4()
    state.documents.setdefault("orders", {})[pk] = {
        "id": str(pk),
        "rev": 1,
        "created_at": _stamp(),
        "last_update_at": _stamp(),
        "warehouse_id": str(warehouse_id) if warehouse_id is not None else None,
    }
    return pk


# ----------------------- #


class TestDeclaration:
    def test_field_and_fields_are_exclusive(self) -> None:
        with pytest.raises(CoreException, match="both field and fields"):
            DerivedReadField(source="s", via="v", field="name", fields=("id", "name"))

    def test_a_bare_string_is_refused(self) -> None:
        with pytest.raises(CoreException, match="is a string"):
            DerivedReadField(source="s", via="v", fields="name")  # type: ignore[arg-type]

    def test_an_empty_tuple_is_refused(self) -> None:
        with pytest.raises(CoreException, match="empty fields"):
            DerivedReadField(source="s", via="v", fields=())

    def test_a_repeated_name_is_refused(self) -> None:
        with pytest.raises(CoreException, match="names a field twice"):
            DerivedReadField(source="s", via="v", fields=("id", "id"))

    def test_fields_without_a_source_is_partly_resolved(self) -> None:
        with pytest.raises(CoreException, match="partly resolved"):
            DerivedReadField(via="v", fields=("id",))

    def test_fields_is_a_resolved_declaration(self) -> None:
        assert REF.resolved is True
        assert REF.field is None

    def test_a_blank_name_is_refused_by_the_spec(self) -> None:
        with pytest.raises(CoreException, match="blank source field"):
            DocumentSpec(
                name="orders",
                read=OrderRead,
                derived_read_fields={
                    "warehouse": DerivedReadField(
                        source="warehouses", via="warehouse_id", fields=("id", " ")
                    )
                },
            )

    def test_a_nullable_key_still_needs_optional(self) -> None:
        with pytest.raises(CoreException, match="which is nullable"):
            DocumentSpec(
                name="orders",
                read=OrderReadOptional,
                derived_read_fields={"warehouse": REF},
            )

    def test_the_reference_is_off_the_query_axes(self) -> None:
        assert "warehouse" not in ORDERS.filterable_fields()
        assert "warehouse_id" in ORDERS.filterable_fields()


# ....................... #


class TestResolution:
    async def test_a_nested_reference_is_built_from_the_joined_row(self) -> None:
        state = MockState()
        wid = _warehouse(state, name="North", code="N1")
        pk = _order(state, warehouse_id=wid)

        row = await _orders(state).get(pk)

        assert row.warehouse == WarehouseRef(id=wid, name="North")

    async def test_only_the_named_fields_are_projected(self) -> None:
        state = MockState()
        wid = _warehouse(state, name="North", code="N1")
        pk = _order(state, warehouse_id=wid)
        row = await _orders(state, spec=ORDERS_MAPPING).get(pk)

        # `code` is on the source row and not named, so it stays there.
        assert row.warehouse == {"id": str(wid), "name": "North"}

    async def test_every_read_path_resolves_it(self) -> None:
        state = MockState()
        wid = _warehouse(state)
        pk = _order(state, warehouse_id=wid)
        orders = _orders(state)

        page = await orders.find_many()
        assert [r.warehouse.name for r in page.hits] == ["North"]

        many = await orders.get_many([pk])
        assert [r.warehouse.id for r in many] == [wid]

        projected = await orders.project_many(["id", "warehouse"])
        assert [r["warehouse"] for r in projected.hits] == [{"id": str(wid), "name": "North"}]

    async def test_the_source_row_is_read_live(self) -> None:
        state = MockState()
        wid = _warehouse(state, name="North")
        pk = _order(state, warehouse_id=wid)
        store = state.documents["warehouses"]
        store[wid] = {**store[wid], "name": "Renamed"}

        assert (await _orders(state).get(pk)).warehouse.name == "Renamed"


# ....................... #


class TestMissingSources:
    async def test_optional_unset_key_is_none_as_a_whole(self) -> None:
        state = MockState()
        pk = _order(state, warehouse_id=None)

        assert (await _orders(state, optional=True).get(pk)).warehouse is None

    async def test_optional_missing_target_is_none_as_a_whole(self) -> None:
        state = MockState()
        state.documents["warehouses"] = {}
        pk = _order(state, warehouse_id=uuid4())

        assert (await _orders(state, optional=True).get(pk)).warehouse is None

    async def test_a_dangling_key_is_refused(self) -> None:
        state = MockState()
        state.documents["warehouses"] = {}
        pk = _order(state, warehouse_id=uuid4())

        with pytest.raises(CoreException, match="holds no such row"):
            await _orders(state).get(pk)

    async def test_an_unset_key_without_optional_is_refused(self) -> None:
        state = MockState()
        pk = _order(state, warehouse_id=None)

        with pytest.raises(CoreException, match="which is unset"):
            await _orders(state).get(pk)


# ....................... #


class TestWiring:
    def test_the_factory_passes_the_tuple_through(self) -> None:
        factory = ConfigurableMockDocument(module=MockDepsModule(state=MockState()))
        ctx = cast("Any", SimpleNamespace(inv_ctx=SimpleNamespace(get_tenant=lambda: None)))

        resolved = factory._derived_for(ctx, ORDERS)  # pyright: ignore[reportPrivateUsage]

        assert resolved["warehouse"].field == ("id", "name")

    async def test_a_seeded_world_round_trips_through_the_deps_module(self) -> None:
        wid = UUID("00000000-0000-4000-8000-0000000000a1")
        ctx = context_from_modules(MockDepsModule())
        plan = SeedPlan(
            specs=(
                SpecSeed(spec=WAREHOUSES, fixtures=({"id": wid, "name": "North", "code": "N1"},)),
                SpecSeed(spec=ORDERS, count=2),
            ),
            links={"orders": {"warehouse_id": "warehouses"}},
        )

        result = await apply_seed(ctx, plan)
        rows = await ctx.doc.query(ORDERS).get_many(list(result["orders"]))

        assert [r.warehouse for r in rows] == [WarehouseRef(id=wid, name="North")] * 2
