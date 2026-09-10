"""Derived read fields against the in-memory document adapter.

The claim under test is the one that motivates the feature: an aggregate whose read
model requires a field no write produces — a view's joined column — round-trips
through the mock. Every case here fails without
:mod:`forze_mock.adapters._derived`.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.conformity import DerivedReadField
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule
from forze_mock.adapters import MockDocumentAdapter, MockState
from forze_mock.execution.configs import MockRouteConfig
from forze_mock.execution.factories import ConfigurableMockDocument
from forze_mock.adapters._derived import ResolvedDerivedRead
from forze_mock.adapters._mvcc import (  # pyright: ignore[reportPrivateUsage]
    MvccTx,
    _mvcc_tx,
)

# ----------------------- #
# The source aggregate — an ordinary one


class Supplier(Document):
    name: str


class SupplierCreate(CreateDocumentCmd):
    name: str


class SupplierRead(ReadDocument):
    name: str


SUPPLIERS = DocumentSpec(
    name="suppliers",
    read=SupplierRead,
    write=DocumentWriteTypes(domain=Supplier, create_cmd=SupplierCreate),
)


# ----------------------- #
# The reading aggregate — its read model is a view


class Order(Document):
    supplier_id: UUID


class OrderCreate(CreateDocumentCmd):
    supplier_id: UUID


class OrderUpdate(BaseDTO):
    supplier_id: UUID | None = None


class OrderRead(ReadDocument):
    supplier_id: UUID
    supplier: str
    """Required, and no write produces it. This is the shape leniency cannot serve."""


class OrderReadOptional(ReadDocument):
    supplier_id: UUID | None = None
    supplier: str | None = None


ORDERS = DocumentSpec(
    name="orders",
    read=OrderRead,
    write=DocumentWriteTypes(domain=Order, create_cmd=OrderCreate, update_cmd=OrderUpdate),
    derived_read_fields={
        "supplier": DerivedReadField(source="suppliers", via="supplier_id", field="name"),
    },
)

ORDERS_OPTIONAL = DocumentSpec(
    name="orders",
    read=OrderReadOptional,
    write=DocumentWriteTypes(domain=Order, create_cmd=OrderCreate, update_cmd=OrderUpdate),
    derived_read_fields={
        "supplier": DerivedReadField(
            source="suppliers", via="supplier_id", field="name", optional=True
        ),
    },
)


# ----------------------- #


def _suppliers(state: MockState) -> MockDocumentAdapter[SupplierRead, Supplier, SupplierCreate, BaseDTO]:
    return MockDocumentAdapter(
        spec=SUPPLIERS,
        state=state,
        namespace="suppliers",
        read_model=SupplierRead,
        domain_model=Supplier,
    )


def _orders(
    state: MockState,
    *,
    spec: DocumentSpec = ORDERS,
    read_model: type = OrderRead,
    optional: bool = False,
    tenant_scoped_source: bool = False,
    tenant_aware: bool = False,
    tenant_provider=lambda: None,
) -> MockDocumentAdapter:
    return MockDocumentAdapter(
        spec=spec,
        state=state,
        namespace="orders",
        read_model=read_model,
        domain_model=Order,
        tenant_aware=tenant_aware,
        tenant_provider=tenant_provider,
        derived={
            "supplier": ResolvedDerivedRead(
                namespace="suppliers",
                via="supplier_id",
                field="name",
                optional=optional,
                tenant_scoped=tenant_scoped_source,
            )
        },
    )


def _row(pk: UUID, *, supplier_id: UUID | None) -> dict[str, object]:
    """A stored order row, seeded by hand so the join key can be left unset."""

    now = datetime.now(UTC)
    return {
        "id": str(pk),
        "rev": 1,
        "created_at": now.isoformat(),
        "last_update_at": now.isoformat(),
        "supplier_id": str(supplier_id) if supplier_id is not None else None,
    }


# ----------------------- #


class TestTheMotivatingCase:
    """A required derived field round-trips. Without hydration, pydantic refuses."""

    async def test_get_resolves_the_join(self) -> None:
        state = MockState()
        supplier = await _suppliers(state).create(SupplierCreate(name="Acme"))
        orders = _orders(state)

        created = await orders.create(OrderCreate(supplier_id=supplier.id))

        assert created.supplier == "Acme"
        assert (await orders.get(created.id)).supplier == "Acme"

    async def test_every_read_path_resolves_it(self) -> None:
        """Nine decode sites; a per-method fix passes `get` and fails these."""

        state = MockState()
        supplier = await _suppliers(state).create(SupplierCreate(name="Acme"))
        orders = _orders(state)
        created = await orders.create(OrderCreate(supplier_id=supplier.id))

        found = await orders.find({"$values": {"supplier_id": {"$eq": supplier.id}}})
        assert found is not None and found.supplier == "Acme"

        page = await orders.find_many(filters=None)
        assert [row.supplier for row in page.hits] == ["Acme"]

        many = await orders.get_many([created.id])
        assert [row.supplier for row in many] == ["Acme"]

        cursor = await orders.find_cursor(cursor={"limit": 10})
        assert [row.supplier for row in cursor.hits] == ["Acme"]

    async def test_projection_resolves_it(self) -> None:
        state = MockState()
        supplier = await _suppliers(state).create(SupplierCreate(name="Acme"))
        orders = _orders(state)
        await orders.create(OrderCreate(supplier_id=supplier.id))

        page = await orders.project_many(["id", "supplier"])
        assert [row["supplier"] for row in page.hits] == ["Acme"]

    async def test_the_source_row_is_read_live(self) -> None:
        """A view reflects the source's current value, so the join must not be cached."""

        state = MockState()
        suppliers = _suppliers(state)
        supplier = await suppliers.create(SupplierCreate(name="Acme"))
        orders = _orders(state)
        created = await orders.create(OrderCreate(supplier_id=supplier.id))

        store = state.documents["suppliers"]
        store[supplier.id] = {**store[supplier.id], "name": "Renamed"}

        assert (await orders.get(created.id)).supplier == "Renamed"


class TestMissingSources:
    async def test_dangling_key_is_refused(self) -> None:
        state = MockState()
        orders = _orders(state)

        # A supplier id nothing was seeded for: in a store that holds every row, this
        # is a seeding bug, and a silent None would surface as a confusing assertion
        # later. `create` reads its own result back, so the refusal lands there.
        with pytest.raises(CoreException, match="holds no such row"):
            await orders.create(OrderCreate(supplier_id=uuid4()))

    async def test_optional_source_yields_none(self) -> None:
        state = MockState()
        orders = _orders(
            state, spec=ORDERS_OPTIONAL, read_model=OrderReadOptional, optional=True
        )

        created = await orders.create(OrderCreate(supplier_id=uuid4()))

        assert (await orders.get(created.id)).supplier is None

    async def test_optional_unset_key_yields_none(self) -> None:
        state = MockState()
        orders = _orders(
            state, spec=ORDERS_OPTIONAL, read_model=OrderReadOptional, optional=True
        )
        # No supplier_id at all — a nullable join key with nothing in it.
        store_ns = "orders"
        pk = uuid4()
        state.documents[store_ns] = {pk: _row(pk, supplier_id=None)}

        assert (await orders.get(pk)).supplier is None

    async def test_unset_key_without_optional_is_refused(self) -> None:
        state = MockState()
        orders = _orders(state)
        pk = uuid4()
        state.documents["orders"] = {pk: _row(pk, supplier_id=None)}

        with pytest.raises(CoreException, match="which is unset"):
            await orders.get(pk)


class TestTenancy:
    async def test_a_tenant_scoped_source_is_read_in_the_bound_partition(self) -> None:
        state = MockState()
        t1, t2 = uuid4(), uuid4()
        bound = {"id": TenantIdentity(tenant_id=t1)}

        suppliers = MockDocumentAdapter(
            spec=SUPPLIERS,
            state=state,
            namespace="suppliers",
            read_model=SupplierRead,
            domain_model=Supplier,
            tenant_aware=True,
            tenant_provider=lambda: bound["id"],
        )
        supplier = await suppliers.create(SupplierCreate(name="TenantOne"))

        orders = _orders(
            state,
            tenant_scoped_source=True,
            tenant_aware=True,
            tenant_provider=lambda: bound["id"],
        )
        created = await orders.create(OrderCreate(supplier_id=supplier.id))

        assert (await orders.get(created.id)).supplier == "TenantOne"

        # The other tenant's partition holds no such supplier, so the same join
        # refuses rather than reaching across.
        bound["id"] = TenantIdentity(tenant_id=t2)

        with pytest.raises(CoreException):
            await orders.get(created.id)


class TestTransactionView:
    """A derived read must observe the same snapshot the reading document does.

    Joining the live store beneath the overlay would let a snapshot transaction see a
    sibling write it is not supposed to — the opposite of what the overlay is for. The
    routing is a one-line choice in :meth:`_hydrate`, so it needs a test that fails when
    the line reads the raw store instead.
    """

    async def test_a_snapshot_transaction_sees_the_source_as_of_begin(self) -> None:
        state = MockState()
        suppliers = _suppliers(state)
        supplier = await suppliers.create(SupplierCreate(name="Before"))
        orders = _orders(state)
        created = await orders.create(OrderCreate(supplier_id=supplier.id))

        tx = MvccTx(begin_version=state.mvcc_version, serializable=False)
        token = _mvcc_tx.set(tx)

        try:
            # Freeze the source's as-of-begin view, then commit a rename outside the
            # transaction, straight into the live store.
            # `view` falls back to a frozen as-of-begin snapshot per namespace, seeded
            # here for both the reading document and its source.
            for ns in ("orders", "suppliers"):
                tx.snapshots[ns] = {
                    key: dict(row) for key, row in state.documents[ns].items()
                }
            live = state.documents["suppliers"]
            live[supplier.id] = {**live[supplier.id], "name": "After"}
            state.mvcc_version += 1

            assert (await orders.get(created.id)).supplier == "Before"
        finally:
            _mvcc_tx.reset(token)

        # Outside the transaction the same read sees the committed value.
        assert (await orders.get(created.id)).supplier == "After"


class TestWiringRefusal:
    """The one pairing that cannot be resolved is refused at wiring, not at read time.

    A tenant-aware source read by a non-tenant-aware reader has no bound tenant to
    partition the source's namespace with, so the join would reach the unpartitioned
    namespace: nothing, or another tenant's row. Both are wrong, and the second is a
    cross-tenant read that presents as missing data — which is why this is a refusal
    rather than a resolution.
    """

    def _factory(self, *, source_tenant_aware: bool, reader_tenant_aware: bool):
        module = MockDepsModule(
            state=MockState(),
            routes={
                "orders": MockRouteConfig(tenant_aware=reader_tenant_aware),
                "suppliers": MockRouteConfig(tenant_aware=source_tenant_aware),
            },
        )
        return ConfigurableMockDocument(module=module)

    def _ctx(self, tenant: TenantIdentity | None):
        inv = SimpleNamespace(get_tenant=lambda: tenant)
        return cast("Any", SimpleNamespace(inv_ctx=inv))

    def test_tenant_aware_source_under_untenanted_reader_is_refused(self) -> None:
        factory = self._factory(source_tenant_aware=True, reader_tenant_aware=False)

        with pytest.raises(CoreException, match="derived_tenant_mismatch"):
            factory._derived_for(self._ctx(None), ORDERS)  # pyright: ignore[reportPrivateUsage]

    def test_both_tenant_aware_resolves(self) -> None:
        factory = self._factory(source_tenant_aware=True, reader_tenant_aware=True)
        tenant = TenantIdentity(tenant_id=uuid4())

        resolved = factory._derived_for(self._ctx(tenant), ORDERS)  # pyright: ignore[reportPrivateUsage]

        assert resolved["supplier"].tenant_scoped is True
        assert resolved["supplier"].via == "supplier_id"

    def test_neither_tenant_aware_resolves_unscoped(self) -> None:
        factory = self._factory(source_tenant_aware=False, reader_tenant_aware=False)

        resolved = factory._derived_for(self._ctx(None), ORDERS)  # pyright: ignore[reportPrivateUsage]

        assert resolved["supplier"].tenant_scoped is False
        assert resolved["supplier"].namespace == "suppliers"

    def test_a_tenanted_reader_over_an_unscoped_source_is_allowed(self) -> None:
        """The safe direction: a shared lookup table read by a per-tenant aggregate."""

        factory = self._factory(source_tenant_aware=False, reader_tenant_aware=True)
        tenant = TenantIdentity(tenant_id=uuid4())

        resolved = factory._derived_for(self._ctx(tenant), ORDERS)  # pyright: ignore[reportPrivateUsage]

        assert resolved["supplier"].tenant_scoped is False


class TestMalformedData:
    async def test_a_join_key_that_is_not_a_primary_key_is_refused(self) -> None:
        """Probed rather than assumed reachable: a stored row can hold anything."""

        state = MockState()
        orders = _orders(state)
        pk = uuid4()
        row = _row(pk, supplier_id=None)
        row["supplier_id"] = "not-a-uuid"
        state.documents["orders"] = {pk: row}

        with pytest.raises(CoreException, match="is not a primary key"):
            await orders.get(pk)
