"""Performance tests for MongoDocumentAdapter."""

from uuid import UUID

import pytest
import pytest_asyncio

pytest.importorskip("pymongo")

from forze.application.contracts.document.specs import DocumentSpec
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mongo.execution.deps.deps import mongo_document_configurable
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.platform import MongoClient


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
    deps = Deps({MongoClientDepKey: mongo_client})
    return ExecutionContext(deps=deps)


@pytest_asyncio.fixture
async def document_adapter(
    mongo_client: MongoClient, execution_context: ExecutionContext
):
    """Create document adapter with collection and spec."""
    collection = "perf_docs"
    history_collection = "perf_docs_history"

    spec = DocumentSpec(
        namespace="perf_docs_ns",
        read={"source": collection, "model": PerfReadDoc},
        write={
            "source": collection,
            "models": {
                "domain": PerfDoc,
                "create_cmd": PerfCreateDoc,
                "update_cmd": PerfUpdateDoc,
            },
        },
        history={"source": history_collection},
    )

    factory = mongo_document_configurable(
        rev_bump_strategy="application",
        history_write_strategy="application",
    )
    return factory(execution_context, spec)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_document_create_benchmark(
    async_benchmark, document_adapter
) -> None:
    """Benchmark document create."""

    async def run() -> None:
        result = await document_adapter.create(PerfCreateDoc(name="bench item"))
        assert isinstance(result.id, UUID)
        await document_adapter.kill(result.id)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_document_get_benchmark(
    async_benchmark, document_adapter
) -> None:
    """Benchmark document get by id."""
    doc = await document_adapter.create(PerfCreateDoc(name="bench item"))

    async def run() -> None:
        result = await document_adapter.get(doc.id)
        assert result.id == doc.id

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_document_get_many_benchmark(
    async_benchmark, document_adapter
) -> None:
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
async def test_document_create_many_benchmark(
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
async def test_document_find_many_benchmark(
    async_benchmark, document_adapter
) -> None:
    """Benchmark document find_many with 50 pre-inserted rows."""
    for i in range(50):
        await document_adapter.create(PerfCreateDoc(name=f"find {i}"))

    async def run() -> None:
        rows, cnt = await document_adapter.find_many(limit=50)
        assert cnt >= 50
        assert len(rows) >= 50

    await async_benchmark(run)
