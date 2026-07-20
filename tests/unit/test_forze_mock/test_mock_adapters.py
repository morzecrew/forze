"""Unit tests for in-memory mock adapters."""

import asyncio
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from pydantic import BaseModel, computed_field

from forze.application.contracts.document import (
    DocumentCodecs,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.contracts.search import SearchSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.serialization import default_model_codec
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_kits.domain.soft_deletion.models import DocWithSoftDeletion
from forze_mock.adapters import (
    MockCounterAdapter,
    MockDocumentAdapter,
    MockSearchAdapter,
    MockState,
)
from tests.unit._gateway_codec_helpers import write_codecs_for

# ----------------------- #

class _ProductDoc(DocWithSoftDeletion):
    title: str
    category: str
    price: float = 0.0
    tags: list[str] = []

class _ProductCreate(CreateDocumentCmd):
    title: str
    category: str
    price: float = 0.0
    tags: list[str] = []

class _ProductUpdate(BaseDTO):
    title: str | None = None
    category: str | None = None
    tags: list[str] | None = None

class _ProductRead(ReadDocument):
    title: str
    category: str
    price: float = 0.0
    tags: list[str] = []
    is_deleted: bool = False

class _ProductSearch(BaseModel):
    id: UUID
    title: str
    category: str
    price: float = 0.0
    tags: list[str] = []

class _CategoryStats(BaseModel):
    category: str
    products: int
    revenue: float
    median_price: float
    expensive_products: int
    expensive_revenue: float | None

def _document_adapter(
    state: MockState,
) -> MockDocumentAdapter[_ProductRead, _ProductDoc, _ProductCreate, _ProductUpdate]:
    spec = DocumentSpec(
        name="products",
        read=_ProductRead,
        write=DocumentWriteTypes(
            domain=_ProductDoc,
            create_cmd=_ProductCreate,
            update_cmd=_ProductUpdate,
        ),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=state,
        namespace="products",
        read_model=_ProductRead,
        domain_model=_ProductDoc,
    )

def _search_adapter(state: MockState) -> MockSearchAdapter[_ProductSearch]:
    spec = SearchSpec(
        name="products",
        model_type=_ProductSearch,
        fields=["title", "category", "tags"],
    )
    return MockSearchAdapter(state=state, spec=spec)

@pytest.mark.asyncio
async def test_document_filter_sort_projection_and_search() -> None:
    state = MockState()
    doc = _document_adapter(state)
    search = _search_adapter(state)

    created = await doc.create(
        _ProductCreate(title="Rust Book", category="books", tags=["rust", "backend"])
    )
    await doc.create(
        _ProductCreate(title="TypeScript Guide", category="books", tags=["frontend"])
    )
    await doc.create(
        _ProductCreate(title="Gaming Mouse", category="hardware", tags=["pc"])
    )

    # Filter by scalar shortcut and sort descending by title.
    page = await doc.project_page(
        ["title", "category"],
        filters={"$values": {"category": "books"}},
        sorts={"title": "desc"},
    )
    assert page.count == 2
    assert [row["title"] for row in page.hits] == ["TypeScript Guide", "Rust Book"]

    # $in shortcut over list fields.
    page = await doc.find_page(
        filters={"$values": {"tags": {"$in": ["rust"]}}},
    )
    assert page.count == 1
    assert page.hits[0].title == "Rust Book"


    # Field-to-field compare ($compare).
    page = await doc.find_page(
        filters={"$fields": {"title": "category"}},
    )
    assert page.count == 0
    await doc.create(
        _ProductCreate(title="books", category="books", price=1.0),
    )
    page = await doc.find_page(
        filters={"$fields": {"title": "category"}},
    )
    assert page.count == 1

    # Search sees the same namespace as document adapter.
    s_page = await search.search_page("rust")
    assert s_page.count == 1
    assert s_page.hits[0].title == "Rust Book"

    any_p = await search.search_page(
        ["rust", "typescript"],
    )
    assert any_p.count == 2
    all_p = await search.search_page(
        ["rust", "typescript"],
        options={"phrase_combine": "all"},
    )
    assert all_p.count == 0

    both_p = await search.search_page(
        ["rust", "book"],
        options={"phrase_combine": "all"},
    )
    assert both_p.count == 1
    assert both_p.hits[0].title == "Rust Book"

    raw_page = await search.project_search_page(
        ["title"],
        "rust",
    )
    assert raw_page.count == 1
    assert raw_page.hits == [{"title": "Rust Book"}]

    # Soft delete + restore.
    deleted = await doc.delete(created.id, created.rev)
    assert deleted.is_deleted is True
    restored = await doc.restore(created.id, deleted.rev)
    assert restored.is_deleted is False


@pytest.mark.asyncio
async def test_find_with_unknown_sort_field_fails_loud() -> None:
    # Runtime sort on a field absent from the read model must fail loud (parity
    # with Postgres/Mongo/Firestore), not silently mis-sort.
    doc = _document_adapter(MockState())
    with pytest.raises(CoreException, match="not on the mock read model") as ei:
        await doc.find_many(sorts={"nonexistent_field": "asc"})
    # A caller-supplied sort on an unknown field is a precondition (HTTP 400).
    assert ei.value.kind is ExceptionKind.PRECONDITION
    assert ei.value.code == "field_not_on_read_model"


@pytest.mark.asyncio
async def test_find_with_unknown_filter_field_fails_loud() -> None:
    # Runtime filter on a field absent from the read model (e.g. a computed
    # field, never stored) must fail loud, not silently match nothing.
    doc = _document_adapter(MockState())
    with pytest.raises(CoreException, match="not on the read model") as ei:
        await doc.find_many(filters={"$values": {"nonexistent_field": "x"}})
    # A caller-supplied filter on an unknown field is a precondition (HTTP 400).
    assert ei.value.kind is ExceptionKind.PRECONDITION
    assert ei.value.code == "field_not_on_read_model"


@pytest.mark.asyncio
async def test_document_aggregates_group_and_validate_return_type() -> None:
    state = MockState()
    doc = _document_adapter(state)

    await doc.create(_ProductCreate(title="Rust Book", category="books", price=10.0))
    await doc.create(
        _ProductCreate(title="TypeScript Guide", category="books", price=30.0),
    )
    await doc.create(
        _ProductCreate(title="Gaming Mouse", category="hardware", price=50.0),
    )

    page = await doc.select_page_aggregated(
        _CategoryStats,
        {
            "$groups": {"category": "category"},
            "$computed": {
                "products": {"$count": None},
                "revenue": {"$sum": "price"},
                "median_price": {"$median": "price"},
                "expensive_products": {
                    "$count": {"filter": {"$values": {"price": {"$gte": 30}}}},
                },
                "expensive_revenue": {
                    "$sum": {
                        "field": "price",
                        "filter": {"$values": {"price": {"$gte": 30}}},
                    },
                },
            },
        },
        filters=None,
        sorts={"revenue": "desc"},
    )

    assert page.count == 2
    assert page.hits == [
        _CategoryStats(
            category="hardware",
            products=1,
            revenue=50.0,
            median_price=50.0,
            expensive_products=1,
            expensive_revenue=50.0,
        ),
        _CategoryStats(
            category="books",
            products=2,
            revenue=40.0,
            median_price=20.0,
            expensive_products=1,
            expensive_revenue=30.0,
        ),
    ]

@pytest.mark.asyncio
async def test_document_create_and_update_use_spec_codecs() -> None:
    """Create/update paths use resolved create and update codecs from DocumentSpec."""
    state = MockState()
    read_codec = default_model_codec(_ProductRead)
    domain_codec, real_create, real_update = write_codecs_for(
        domain_type=_ProductDoc,
        create_type=_ProductCreate,
        update_type=_ProductUpdate,
    )
    create_codec = MagicMock()
    create_codec.transform.side_effect = real_create.transform
    update_codec = MagicMock()
    update_codec.encode_mapping.side_effect = real_update.encode_mapping
    spec = DocumentSpec(
        name="products",
        read=_ProductRead,
        write=DocumentWriteTypes(
            domain=_ProductDoc,
            create_cmd=_ProductCreate,
            update_cmd=_ProductUpdate,
        ),
        codecs=DocumentCodecs(
            read=read_codec,
            domain=domain_codec,
            create=create_codec,
            update=update_codec,
        ),
    )
    doc = MockDocumentAdapter(
        spec=spec,
        state=state,
        namespace="products",
        read_model=_ProductRead,
        domain_model=_ProductDoc,
    )
    dto = _ProductCreate(title="Codec Test", category="books")
    created = await doc.create(dto)
    create_codec.transform.assert_called_once_with(dto)
    assert created.title == "Codec Test"

    await doc.update(created.id, created.rev, _ProductUpdate(title="Renamed"))
    # The patch goes through the codec's non-encrypting ``encode_mapping`` path so
    # the merge stays plaintext (see _document_command.update); the single
    # domain-codec encode on write-back is what encrypts.
    update_codec.encode_mapping.assert_called_once()
    assert update_codec.encode_mapping.call_args.kwargs == {
        "exclude": {"computed_fields": True, "unset": True},
    }


@pytest.mark.asyncio
async def test_document_update_detects_revision_conflict() -> None:
    state = MockState()
    doc = _document_adapter(state)
    created = await doc.create(_ProductCreate(title="A", category="x"))

    with pytest.raises(CoreException):
        await doc.update(created.id, created.rev + 1, _ProductUpdate(title="B"))

@pytest.mark.asyncio
async def test_document_create_duplicate_id_raises_conflict() -> None:
    """Mirror Postgres: creating an existing id maps to ``exc.conflict``."""
    state = MockState()
    doc = _document_adapter(state)
    created = await doc.create(_ProductCreate(title="A", category="x"))

    with pytest.raises(CoreException, match="Unique violation") as excinfo:
        await doc.create(_ProductCreate(title="B", category="y"), id=created.id)

    assert excinfo.value.kind is ExceptionKind.CONFLICT
    # The original document is untouched.
    assert (await doc.get(created.id)).title == "A"


@pytest.mark.asyncio
async def test_document_ensure_and_upsert_tolerate_existing_id() -> None:
    state = MockState()
    doc = _document_adapter(state)
    created = await doc.create(_ProductCreate(title="A", category="x"))

    # ensure() returns the existing document without conflict.
    ensured = await doc.ensure(created.id, _ProductCreate(title="B", category="y"))
    assert ensured.id == created.id
    assert ensured.title == "A"

    # upsert() updates over the existing document without conflict.
    upserted = await doc.upsert(
        created.id,
        _ProductCreate(title="C", category="z"),
        _ProductUpdate(title="Updated"),
    )
    assert upserted.id == created.id
    assert upserted.title == "Updated"


class _OrderDoc(Document):
    qty: int
    unit_price: float

    @computed_field  # type: ignore[prop-decorator]
    @property
    def total(self) -> float:
        return self.qty * self.unit_price


class _OrderRead(ReadDocument):
    qty: int
    unit_price: float

    @computed_field  # type: ignore[prop-decorator]
    @property
    def total(self) -> float:
        return self.qty * self.unit_price


class _OrderCreate(CreateDocumentCmd):
    qty: int
    unit_price: float


class _OrderUpdate(BaseDTO):
    qty: int | None = None
    unit_price: float | None = None


def _order_adapter(
    state: MockState,
) -> MockDocumentAdapter[_OrderRead, _OrderDoc, _OrderCreate, _OrderUpdate]:
    spec = DocumentSpec(
        name="orders",
        read=_OrderRead,
        write=DocumentWriteTypes(
            domain=_OrderDoc,
            create_cmd=_OrderCreate,
            update_cmd=_OrderUpdate,
        ),
        materialized={"total"},
    )
    return MockDocumentAdapter(
        spec=spec,
        state=state,
        namespace="orders",
        read_model=_OrderRead,
        domain_model=_OrderDoc,
    )


@pytest.mark.asyncio
async def test_materialized_field_is_persisted_filterable_and_sortable() -> None:
    doc = _order_adapter(MockState())

    await doc.create(_OrderCreate(qty=2, unit_price=10.0))  # total 20
    await doc.create(_OrderCreate(qty=5, unit_price=10.0))  # total 50
    await doc.create(_OrderCreate(qty=1, unit_price=10.0))  # total 10

    # Filter on the materialized derived field (matches against the stored value).
    page = await doc.find_many(filters={"$values": {"total": {"$gte": 20}}})
    assert sorted(row.total for row in page.hits) == [20.0, 50.0]

    # Sort on the materialized derived field.
    ordered = await doc.find_many(sorts={"total": "desc"})
    assert [row.total for row in ordered.hits] == [50.0, 20.0, 10.0]


@pytest.mark.asyncio
async def test_materialized_field_recomputed_and_persisted_on_update() -> None:
    doc = _order_adapter(MockState())

    created = await doc.create(_OrderCreate(qty=2, unit_price=10.0))  # total 20
    assert created.total == 20.0

    # Change an input; the stored derived value must be recomputed.
    updated = await doc.update(created.id, created.rev, _OrderUpdate(qty=5))
    assert updated.total == 50.0

    # The *stored* total is now 50 — a filter on the stale value finds nothing,
    # and a filter on the new value matches (proves the column was rewritten).
    assert (await doc.find_many(filters={"$values": {"total": 20.0}})).hits == []
    matched = await doc.find_many(filters={"$values": {"total": 50.0}})
    assert [row.id for row in matched.hits] == [created.id]


@pytest.mark.asyncio
async def test_update_matching_rejected_with_materialized() -> None:
    # Dev/prod parity: real backends reject set-based bulk update on a materialized
    # aggregate (cannot recompute per row), so the mock must reject it too.
    doc = _order_adapter(MockState())
    await doc.create(_OrderCreate(qty=2, unit_price=10.0))

    with pytest.raises(CoreException, match="materialized") as ei:
        await doc.update_matching(
            {"$values": {"qty": 2}},
            _OrderUpdate(unit_price=20.0),
        )
    assert ei.value.code == "core.document.materialized_bulk_update_unsupported"


@pytest.mark.asyncio
async def test_counter_is_async_safe_under_concurrent_increments() -> None:
    state = MockState()
    counter = MockCounterAdapter(state=state, namespace="orders")

    results = await asyncio.gather(*[counter.incr() for _ in range(100)])
    assert sorted(results) == list(range(1, 101))
    assert await counter.incr_batch(3) == [101, 102, 103]
