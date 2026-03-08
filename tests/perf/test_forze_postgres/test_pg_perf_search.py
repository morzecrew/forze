"""Performance tests for PostgresSearchAdapter."""

from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from pydantic import BaseModel

pytest.importorskip("psycopg")

from forze.application.contracts.search.specs import SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze_postgres.execution.deps.deps import postgres_search
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient


class SearchableModel(BaseModel):
    """Search result model for perf tests."""

    id: UUID
    title: str
    content: str


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
async def search_adapter(pg_client: PostgresClient, execution_context):
    """Create search adapter with PGroonga index and sample data."""
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    await pg_client.execute(
        """
        CREATE TABLE IF NOT EXISTS perf_search_items (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            content text NOT NULL
        );
        """
    )
    await pg_client.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_perf_search_pgroonga
        ON perf_search_items USING pgroonga ((ARRAY[title, content]));
        """
    )

    await pg_client.execute("TRUNCATE perf_search_items")
    for i in range(100):
        await pg_client.execute(
            """
            INSERT INTO perf_search_items (id, title, content)
            VALUES (%(id)s, %(title)s, %(content)s)
            """,
            {
                "id": uuid4(),
                "title": f"Document {i}",
                "content": f"Content for document {i} with searchable terms",
            },
        )

    spec = SearchSpec(
        namespace="perf_search_ns",
        model=SearchableModel,
        indexes={
            "idx_perf_search_pgroonga": {
                "source": "perf_search_items",
                "mode": "pgroonga",
                "fields": [{"path": "title"}, {"path": "content"}],
            }
        },
        default_index="idx_perf_search_pgroonga",
    )

    return postgres_search(execution_context, spec)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_search_benchmark(async_benchmark, search_adapter) -> None:
    """Benchmark search query."""

    async def run() -> None:
        res, cnt = await search_adapter.search("document")
        assert cnt >= 0
        assert isinstance(res, list)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_search_with_limit_benchmark(async_benchmark, search_adapter) -> None:
    """Benchmark search with limit."""

    async def run() -> None:
        res, cnt = await search_adapter.search("searchable", limit=10)
        assert len(res) <= 10

    await async_benchmark(run)
