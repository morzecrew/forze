"""Integration-style exercises of :mod:`forze_mock.adapters` sharing one :class:`~forze_mock.adapters.MockState`."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.search import SearchSpec
from forze.domain.mixins import SoftDeletionMixin
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import (
    MockCacheAdapter,
    MockDocumentAdapter,
    MockQueueAdapter,
    MockSearchAdapter,
    MockState,
    MockStorageAdapter,
)


class _ItemDoc(Document, SoftDeletionMixin):
    title: str


class _ItemCreate(CreateDocumentCmd):
    title: str


class _ItemUpdate(BaseDTO):
    title: str | None = None


class _ItemRead(ReadDocument):
    title: str
    is_deleted: bool = False


class _ItemSearch(BaseModel):
    id: UUID
    title: str

    model_config = {"extra": "ignore"}


class _QMsg(BaseModel):
    body: str


def _doc_spec() -> DocumentSpec[_ItemRead, _ItemDoc, _ItemCreate, _ItemUpdate]:
    return DocumentSpec(
        name="catalog",
        read=_ItemRead,
        write=DocumentWriteTypes(
            domain=_ItemDoc,
            create_cmd=_ItemCreate,
            update_cmd=_ItemUpdate,
        ),
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mock_shared_state_document_search_storage_cache_and_queue() -> None:
    """Single :class:`MockState` backs document, search, storage, cache, and queue adapters."""
    state = MockState()
    spec = _doc_spec()

    docs = MockDocumentAdapter(
        spec=spec,
        state=state,
        namespace="catalog",
        read_model=_ItemRead,
        domain_model=_ItemDoc,
    )
    search = MockSearchAdapter(
        state=state,
        spec=SearchSpec(
            name="catalog",
            model_type=_ItemSearch,
            fields=["title"],
        ),
    )
    storage = MockStorageAdapter(state=state, bucket="files")
    cache = MockCacheAdapter(state=state, namespace="session")
    queue = MockQueueAdapter(state=state, namespace="jobs", model=_QMsg)

    created = await docs.create(_ItemCreate(title="integrate-me"))
    assert created.title == "integrate-me"

    __p = await search.search("integrate", return_count=True)
    hits = __p.hits
    total = __p.count
    assert total >= 1
    assert any("integrate" in getattr(h, "title", "").lower() for h in hits)

    stored = await storage.upload(
        "note.txt",
        b"hello",
        description="mock integration",
        prefix="docs",
    )
    assert stored["size"] == 5

    dl = await storage.download(stored["key"])
    assert dl["data"] == b"hello"

    now = datetime.now(tz=UTC)
    await cache.set_versioned("user-1", "v1", {"seen": True})
    assert await cache.get("user-1") == {"seen": True}

    mid = await queue.enqueue("jobs", _QMsg(body="process"), enqueued_at=now)
    batch = await queue.receive("jobs", limit=5)
    assert batch[0]["id"] == mid
    assert await queue.ack("jobs", [mid]) == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mock_document_create_many_and_touch_shared_state() -> None:
    """Batch writes and touch operations stay consistent in one state partition."""
    state = MockState()
    spec = _doc_spec()
    docs = MockDocumentAdapter(
        spec=spec,
        state=state,
        namespace="catalog",
        read_model=_ItemRead,
        domain_model=_ItemDoc,
    )

    out = await docs.create_many(
        [
            _ItemCreate(title="a"),
            _ItemCreate(title="b"),
        ]
    )
    assert len(out) == 2

    pks = [d.id for d in out]
    touched = await docs.touch_many(pks)
    assert all(t.rev >= 2 for t in touched)

    await docs.kill(pks[0])
    remaining = await docs.count()
    assert remaining == 1
