"""Performance tests for MongoDocumentAdapter."""

from uuid import UUID

import pytest
import pytest_asyncio

pytest.importorskip("pymongo")

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mongo.execution.deps.deps import ConfigurableMongoDocument
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.platform import MongoClient

_MONGO_DOC_CREATE_MANY_LARGE = 500
_MONGO_DOC_GET_MANY_LARGE = 200
_MONGO_DOC_FIND_MANY_SEED = 2_000


class PerfDoc(Document):
    """Domain model for perf tests."""

    name: str


class PerfCreateDoc(CreateDocumentCmd):
    """Create command for perf tests."""

    name: str


class PerfUpdateDoc(BaseDTO):
    """Update DTO for perf tests."""

    name: str | None = None


class PerfReadDoc(ReadDocument):
    """Read model for perf tests."""

    name: str


@pytest.fixture
def execution_context(mongo_client: MongoClient) -> ExecutionContext:
    """Build execution context with Mongo deps."""
    db_name = mongo_client.db().name
    configurable = ConfigurableMongoDocument(
        config={
            "read": (db_name, "perf_docs"),
            "write": (db_name, "perf_docs"),
            "history": (db_name, "perf_docs_history"),
        }
    )
    deps = Deps.plain(
        {
            MongoClientDepKey: mongo_client,
            DocumentQueryDepKey: configurable,
            DocumentCommandDepKey: configurable,
        }
    )
    return ExecutionContext(deps=deps)


@pytest_asyncio.fixture
async def document_adapter(
    mongo_client: MongoClient, execution_context: ExecutionContext
):
    """Create document adapter with collection and spec."""
    spec = DocumentSpec(
        name="perf_docs_ns",
        read=PerfReadDoc,
        write={
            "domain": PerfDoc,
            "create_cmd": PerfCreateDoc,
            "update_cmd": PerfUpdateDoc,
        },
        history_enabled=True,
    )

    return execution_context.doc_command(spec)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_mongo_document_create_benchmark(async_benchmark, document_adapter) -> None:
    """Benchmark document create."""

    async def run() -> None:
        result = await document_adapter.create(PerfCreateDoc(name="bench item"))
        assert isinstance(result.id, UUID)
        await document_adapter.kill(result.id)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_mongo_document_get_benchmark(async_benchmark, document_adapter) -> None:
    """Benchmark document get by id."""
    doc = await document_adapter.create(PerfCreateDoc(name="bench item"))

    async def run() -> None:
        result = await document_adapter.get(doc.id)
        assert result.id == doc.id

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_mongo_document_get_many_benchmark(async_benchmark, document_adapter) -> None:
    """Benchmark document get_many with 10 ids."""
    docs = [
        await document_adapter.create(PerfCreateDoc(name=f"item {i}"))
        for i in range(10)
    ]
    pks = [d.id for d in docs]

    async def run() -> None:
        result = await document_adapter.get_many(pks)
        assert len(result) == 10

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_mongo_document_get_many_large_benchmark(
    async_benchmark, document_adapter
) -> None:
    """Benchmark document get_many with a large id list (200)."""
    docs = [
        await document_adapter.create(PerfCreateDoc(name=f"bulk {i}"))
        for i in range(_MONGO_DOC_GET_MANY_LARGE)
    ]
    pks = [d.id for d in docs]

    async def run() -> None:
        result = await document_adapter.get_many(pks)
        assert len(result) == _MONGO_DOC_GET_MANY_LARGE

    await async_benchmark(run)

    await document_adapter.kill_many(pks)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_mongo_document_create_many_benchmark(
    async_benchmark, document_adapter
) -> None:
    """Benchmark document create_many with 20 items."""

    async def run() -> None:
        dtos = [PerfCreateDoc(name=f"batch {i}") for i in range(20)]
        result = await document_adapter.create_many(dtos)
        await document_adapter.kill_many([doc.id for doc in result])

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_mongo_document_create_many_large_benchmark(
    async_benchmark, document_adapter
) -> None:
    """Benchmark document create_many with a large batch (500 items)."""

    async def run() -> None:
        dtos = [PerfCreateDoc(name=f"batch-lg {i}") for i in range(_MONGO_DOC_CREATE_MANY_LARGE)]
        result = await document_adapter.create_many(dtos)
        await document_adapter.kill_many([doc.id for doc in result])

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_mongo_document_find_many_benchmark(async_benchmark, document_adapter) -> None:
    """Benchmark document find_many with 50 pre-inserted rows."""
    for i in range(50):
        await document_adapter.create(PerfCreateDoc(name=f"find {i}"))

    async def run() -> None:
        rows, cnt = await document_adapter.find_many(pagination={"limit": 50})
        assert cnt >= 50
        assert len(rows) >= 50

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_mongo_document_find_many_large_benchmark(
    async_benchmark, document_adapter
) -> None:
    """Benchmark find_many against a large collection (2k docs, limit 500)."""
    batch_size = 200
    for start in range(0, _MONGO_DOC_FIND_MANY_SEED, batch_size):
        chunk = [
            PerfCreateDoc(name=f"find-lg {i}")
            for i in range(start, min(start + batch_size, _MONGO_DOC_FIND_MANY_SEED))
        ]
        await document_adapter.create_many(chunk)

    async def run() -> None:
        rows, cnt = await document_adapter.find_many(pagination={"limit": 500})
        assert cnt >= 500
        assert len(rows) >= 500

    await async_benchmark(run)
