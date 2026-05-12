"""Unit tests for in-memory mock adapters."""

import asyncio
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.search import SearchSpec
from forze.base.errors import ConcurrencyError
from forze.domain.mixins import SoftDeletionMixin
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock.adapters import (
    MockCounterAdapter,
    MockDocumentAdapter,
    MockSearchAdapter,
    MockState,
)

# ----------------------- #


class _ProductDoc(Document, SoftDeletionMixin):
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
        filters={"$fields": {"category": "books"}},
        sorts={"title": "desc"},
    )
    assert page.count == 2
    assert [row["title"] for row in page.hits] == ["TypeScript Guide", "Rust Book"]

    # $in shortcut over list fields.
    page = await doc.find_page(
        filters={"$fields": {"tags": {"$in": ["rust"]}}},
    )
    assert page.count == 1
    assert page.hits[0].title == "Rust Book"

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
            "$fields": {"category": "category"},
            "$computed": {
                "products": {"$count": None},
                "revenue": {"$sum": "price"},
                "median_price": {"$median": "price"},
                "expensive_products": {
                    "$count": {"filter": {"$fields": {"price": {"$gte": 30}}}},
                },
                "expensive_revenue": {
                    "$sum": {
                        "field": "price",
                        "filter": {"$fields": {"price": {"$gte": 30}}},
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
async def test_document_update_detects_revision_conflict() -> None:
    state = MockState()
    doc = _document_adapter(state)
    created = await doc.create(_ProductCreate(title="A", category="x"))

    with pytest.raises(ConcurrencyError):
        await doc.update(created.id, created.rev + 1, _ProductUpdate(title="B"))


@pytest.mark.asyncio
async def test_counter_is_async_safe_under_concurrent_increments() -> None:
    state = MockState()
    counter = MockCounterAdapter(state=state, namespace="orders")

    results = await asyncio.gather(*[counter.incr() for _ in range(100)])
    assert sorted(results) == list(range(1, 101))
    assert await counter.incr_batch(3) == [101, 102, 103]
