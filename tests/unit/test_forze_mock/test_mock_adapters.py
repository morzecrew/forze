"""Unit tests for in-memory mock adapters."""

import asyncio
from uuid import UUID

import pytest
from pydantic import BaseModel

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


class _ProductCreate(CreateDocumentCmd):
    title: str
    category: str
    tags: list[str] = []


class _ProductUpdate(BaseDTO):
    title: str | None = None
    category: str | None = None
    tags: list[str] | None = None


class _ProductRead(ReadDocument):
    title: str
    category: str
    tags: list[str] = []
    is_deleted: bool = False


class _ProductSearch(BaseModel):
    id: UUID
    title: str
    category: str
    tags: list[str] = []


def _document_adapter(
    state: MockState,
) -> MockDocumentAdapter[_ProductRead, _ProductDoc, _ProductCreate, _ProductUpdate]:
    return MockDocumentAdapter(
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
