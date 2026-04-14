"""Performance tests for PostgresDocumentAdapter."""

from uuid import UUID

import pytest
import pytest_asyncio

pytest.importorskip("psycopg")

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.execution.deps.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient

_PG_DOC_CREATE_MANY_LARGE = 500
_PG_DOC_GET_MANY_LARGE = 200
_PG_DOC_FIND_MANY_SEED = 2_000


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
    configurable = ConfigurablePostgresDocument(
        config={
            "read": ("public", "perf_docs"),
            "write": ("public", "perf_docs"),
            "bookkeeping_strategy": "application",
        }
    )
    deps = Deps.plain(
        {
            PostgresClientDepKey: pg_client,
            PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            DocumentQueryDepKey: configurable,
            DocumentCommandDepKey: configurable,
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
        name="perf_docs_ns",
        read=PerfReadDoc,
        write={
            "domain": PerfDoc,
            "create_cmd": PerfCreateDoc,
            "update_cmd": PerfUpdateDoc,
        },
    )

    return execution_context.doc_command(spec)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_document_create_benchmark(async_benchmark, document_adapter) -> None:
    """Benchmark document create."""

    async def run() -> None:
        result = await document_adapter.create(PerfCreateDoc(name="bench item"))
        assert isinstance(result.id, UUID)
        await document_adapter.kill(result.id)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_document_get_benchmark(async_benchmark, document_adapter) -> None:
    """Benchmark document get by id."""
    doc = await document_adapter.create(PerfCreateDoc(name="bench item"))

    async def run() -> None:
        result = await document_adapter.get(doc.id)
        assert result.id == doc.id

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_document_get_many_benchmark(async_benchmark, document_adapter) -> None:
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
async def test_pg_document_get_many_large_benchmark(
    async_benchmark, document_adapter
) -> None:
    """Benchmark document get_many with a large id list (200)."""
    docs = [
        await document_adapter.create(PerfCreateDoc(name=f"bulk {i}"))
        for i in range(_PG_DOC_GET_MANY_LARGE)
    ]
    pks = [d.id for d in docs]

    async def run() -> None:
        result = await document_adapter.get_many(pks)
        assert len(result) == _PG_DOC_GET_MANY_LARGE

    await async_benchmark(run)

    await document_adapter.kill_many(pks)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_document_create_many_benchmark(
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
async def test_pg_document_create_many_large_benchmark(
    async_benchmark, document_adapter
) -> None:
    """Benchmark document create_many with a large batch (500 items)."""

    async def run() -> None:
        dtos = [PerfCreateDoc(name=f"batch-lg {i}") for i in range(_PG_DOC_CREATE_MANY_LARGE)]
        result = await document_adapter.create_many(dtos)
        await document_adapter.kill_many([doc.id for doc in result])

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_document_find_many_benchmark(
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


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_document_find_many_large_benchmark(
    async_benchmark, document_adapter, pg_client
) -> None:
    """Benchmark find_many against a large table (2k rows, limit 500)."""
    await pg_client.execute("TRUNCATE perf_docs")
    batch_size = 200
    for start in range(0, _PG_DOC_FIND_MANY_SEED, batch_size):
        chunk = [
            PerfCreateDoc(name=f"find-lg {i}")
            for i in range(start, min(start + batch_size, _PG_DOC_FIND_MANY_SEED))
        ]
        await document_adapter.create_many(chunk)

    async def run() -> None:
        rows, cnt = await document_adapter.find_many(limit=500)
        assert cnt >= 500
        assert len(rows) >= 500

    await async_benchmark(run)

    await pg_client.execute("TRUNCATE perf_docs")
