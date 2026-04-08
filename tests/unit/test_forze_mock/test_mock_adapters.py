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
    tags: list[str] = []
    meta: str | None = None
    score: int = 0


class _ProductCreate(CreateDocumentCmd):
    title: str
    category: str
    tags: list[str] = []
    meta: str | None = None
    score: int = 0


class _ProductUpdate(BaseDTO):
    title: str | None = None
    category: str | None = None
    tags: list[str] | None = None
    meta: str | None = None
    score: int | None = None


class _ProductRead(ReadDocument):
    title: str
    category: str
    tags: list[str] = []
    is_deleted: bool = False
    meta: str | None = None
    score: int = 0


class _ProductSearch(BaseModel):
    id: UUID
    title: str
    category: str
    tags: list[str] = []


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
    rows, count = await doc.find_many(
        filters={"$fields": {"category": "books"}},
        sorts={"title": "desc"},
        return_fields=["title", "category"],
    )
    assert count == 2
    assert [row["title"] for row in rows] == ["TypeScript Guide", "Rust Book"]

    # $in shortcut over list fields.
    rows, count = await doc.find_many(
        filters={"$fields": {"tags": {"$in": ["rust"]}}},
    )
    assert count == 1
    assert rows[0].title == "Rust Book"

    # Search sees the same namespace as document adapter.
    hits, total = await search.search("rust")
    assert total == 1
    assert hits[0].title == "Rust Book"

    raw_hits, raw_total = await search.search("rust", return_fields=["title"])
    assert raw_total == 1
    assert raw_hits == [{"title": "Rust Book"}]

    # Soft delete + restore.
    deleted = await doc.delete(created.id, created.rev)
    assert deleted.is_deleted is True
    restored = await doc.restore(created.id, deleted.rev)
    assert restored.is_deleted is False


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


@pytest.mark.asyncio
async def test_document_query_operators_and_logic() -> None:
    """Exercise mock query matching beyond scalar / $in shortcuts."""
    state = MockState()
    doc = _document_adapter(state)

    a = await doc.create(
        _ProductCreate(
            title="Alpha",
            category="cat-a",
            tags=["x", "y"],
            meta=None,
            score=10,
        )
    )
    await doc.create(
        _ProductCreate(
            title="Beta",
            category="cat-b",
            tags=[],
            meta="present",
            score=20,
        )
    )
    await doc.create(
        _ProductCreate(
            title="Gamma",
            category="cat-a",
            tags=["z"],
            meta=None,
            score=15,
        )
    )

    rows, n = await doc.find_many(
        filters={"$fields": {"title": {"$neq": "Beta"}}},
        sorts={"score": "asc"},
    )
    assert n == 2
    assert [r.title for r in rows] == ["Alpha", "Gamma"]

    rows, n = await doc.find_many(
        filters={"$fields": {"score": {"$gte": 15, "$lte": 20}}},
    )
    assert n == 2

    rows, n = await doc.find_many(filters={"$fields": {"score": {"$gt": 18}}})
    assert n == 1 and rows[0].title == "Beta"

    row = await doc.find(filters={"$fields": {"meta": {"$null": True}}})
    assert row is not None and row.id == a.id

    rows, n = await doc.find_many(
        filters={"$fields": {"tags": {"$empty": True}}},
    )
    assert n == 1 and rows[0].title == "Beta"

    rows, n = await doc.find_many(
        filters={"$fields": {"tags": {"$superset": ["x", "y"]}}},
    )
    assert n == 1 and rows[0].title == "Alpha"

    rows, n = await doc.find_many(
        filters={
            "$fields": {
                "tags": {"$subset": ["x", "y", "z"]},
                "title": {"$neq": "Beta"},
            }
        },
    )
    assert n == 2
    assert {r.title for r in rows} == {"Alpha", "Gamma"}

    rows, n = await doc.find_many(
        filters={"$fields": {"tags": {"$disjoint": ["z"]}}},
    )
    assert n == 2

    rows, n = await doc.find_many(
        filters={"$fields": {"tags": {"$overlaps": ["z"]}}},
    )
    assert n == 1

    rows, n = await doc.find_many(
        filters={
            "$or": [
                {"$fields": {"title": "Alpha"}},
                {"$fields": {"title": "Gamma"}},
            ]
        },
    )
    assert n == 2
    assert {r.title for r in rows} == {"Alpha", "Gamma"}

    assert (
        await doc.count(
            filters={"$fields": {"category": {"$in": ["cat-a", "cat-b"]}}},
        )
        == 3
    )

    many = await doc.get_many([a.id])
    assert len(many) == 1 and many[0].title == "Alpha"
