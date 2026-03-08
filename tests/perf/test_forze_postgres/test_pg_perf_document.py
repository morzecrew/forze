"""Performance tests for PostgresDocumentAdapter."""

from uuid import UUID

import pytest
import pytest_asyncio

pytest.importorskip("psycopg")

from forze.application.contracts.document.specs import DocumentSpec
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.execution.deps.deps import postgres_document_configurable
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient


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
def execution_context(pg_client: PostgresClient):
    """Build execution context with Postgres deps."""
    deps = Deps(
        {
            PostgresClientDepKey: pg_client,
            PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
        }
    )
    return ExecutionContext(deps=deps)


@pytest_asyncio.fixture
async def document_adapter(pg_client: PostgresClient, execution_context):
    """Create document adapter with table and spec."""
    await pg_client.execute(
        """
        CREATE TABLE IF NOT EXISTS perf_docs (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )

    spec = DocumentSpec(
        namespace="perf_docs_ns",
        read={"source": "perf_docs", "model": PerfReadDoc},
        write={
            "source": "perf_docs",
            "models": {
                "domain": PerfDoc,
                "create_cmd": PerfCreateDoc,
                "update_cmd": PerfUpdateDoc,
            },
        },
    )

    factory = postgres_document_configurable(rev_bump_strategy="application")
    return factory(execution_context, spec)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_document_create_benchmark(async_benchmark, document_adapter) -> None:
    """Benchmark document create."""

    async def run() -> None:
        result = await document_adapter.create(PerfCreateDoc(name="bench item"))
        assert isinstance(result.id, UUID)
        await document_adapter.kill(result.id)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_document_get_benchmark(async_benchmark, document_adapter) -> None:
    """Benchmark document get by id."""
    doc = await document_adapter.create(PerfCreateDoc(name="bench item"))

    async def run() -> None:
        result = await document_adapter.get(doc.id)
        assert result.id == doc.id

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_document_get_many_benchmark(async_benchmark, document_adapter) -> None:
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
    async_benchmark, document_adapter, pg_client
) -> None:
    """Benchmark document find_many with 50 pre-inserted rows."""
    await pg_client.execute("TRUNCATE perf_docs")
    for i in range(50):
        await document_adapter.create(PerfCreateDoc(name=f"find {i}"))

    async def run() -> None:
        rows, cnt = await document_adapter.find_many(limit=50)
        assert cnt >= 50
        assert len(rows) >= 50

    await async_benchmark(run)
