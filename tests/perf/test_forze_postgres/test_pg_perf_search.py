"""Performance tests for PostgresSearchAdapter."""

from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from pydantic import BaseModel

pytest.importorskip("psycopg")

from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze_postgres.execution.deps.deps import ConfigurablePostgresSearch
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient

_PG_SEARCH_LARGE_ROWS = 5_000


class SearchableModel(BaseModel):
    """Search result model for perf tests."""

    id: UUID
    title: str
    content: str


@pytest.fixture
def execution_context(pg_client: PostgresClient):
    """Build execution context with Postgres deps."""
    deps = Deps.plain(
        {
            PostgresClientDepKey: pg_client,
            PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            SearchQueryDepKey: ConfigurablePostgresSearch(
                config={
                    "index": ("public", "idx_perf_search_pgroonga"),
                    "read": ("public", "perf_search_items"),
                    "engine": "pgroonga",
                }
            ),
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
        name="perf_search_ns",
        model_type=SearchableModel,
        fields=["title", "content"],
    )

    return execution_context.search_query(spec)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_search_benchmark(async_benchmark, search_adapter) -> None:
    """Benchmark search query."""

    async def run() -> None:
        res, cnt = await search_adapter.search("document")
        assert cnt >= 0
        assert isinstance(res, list)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_search_with_limit_benchmark(async_benchmark, search_adapter) -> None:
    """Benchmark search with limit."""

    async def run() -> None:
        res, cnt = await search_adapter.search("searchable", pagination={"limit": 10})
        assert len(res) <= 10

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_search_large_corpus_benchmark(
    async_benchmark, pg_client: PostgresClient, execution_context: ExecutionContext
) -> None:
    """Benchmark search against a large indexed corpus (5k rows)."""
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
    await pg_client.execute_many(
        """
        INSERT INTO perf_search_items (id, title, content)
        VALUES (%(id)s, %(title)s, %(content)s)
        """,
        [
            {
                "id": uuid4(),
                "title": f"Bulk document {i}",
                "content": f"Bulk content {i} searchable corpus terms",
            }
            for i in range(_PG_SEARCH_LARGE_ROWS)
        ],
    )

    spec = SearchSpec(
        name="perf_search_ns",
        model_type=SearchableModel,
        fields=["title", "content"],
    )
    adapter = execution_context.search_query(spec)

    async def run() -> None:
        res, cnt = await adapter.search(
            "corpus", pagination={"limit": 100, "offset": 0}
        )
        assert cnt >= 0
        assert isinstance(res, list)

    await async_benchmark(run)
