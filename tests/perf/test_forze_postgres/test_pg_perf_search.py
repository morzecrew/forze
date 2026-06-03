"""Performance tests for PostgresSearchAdapter."""

import pytest
import pytest_asyncio

pytest.importorskip("psycopg")

from forze_postgres.kernel.client.client import PostgresClient
from tests.perf.support.postgres_search_corpus import (
    ensure_pgroonga_search_schema,
    perf_search_spec,
    search_execution_context,
    seed_search_corpus,
)

_PG_SEARCH_LARGE_ROWS = 5_000


@pytest_asyncio.fixture
async def search_corpus(pg_client: PostgresClient):
    """PGroonga table with 100 searchable rows."""

    await ensure_pgroonga_search_schema(pg_client)
    await seed_search_corpus(pg_client, 100)


@pytest_asyncio.fixture
async def search_adapter(search_corpus, pg_client: PostgresClient):
    ctx = search_execution_context(pg_client)
    return ctx.search.query(perf_search_spec())


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_search_benchmark(async_benchmark, search_adapter) -> None:
    """Benchmark search query."""

    async def run() -> None:
        page = await search_adapter.search_page("document")
        assert page.count >= 0
        assert isinstance(page.hits, list)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_search_with_limit_benchmark(async_benchmark, search_adapter) -> None:
    """Benchmark search with limit."""

    async def run() -> None:
        page = await search_adapter.search_page(
            "searchable",
            pagination={"limit": 10},
        )
        assert len(page.hits) <= 10

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_search_large_corpus_benchmark(
    async_benchmark,
    pg_client: PostgresClient,
) -> None:
    """Benchmark search against a large indexed corpus (5k rows)."""

    await ensure_pgroonga_search_schema(pg_client)
    await seed_search_corpus(pg_client, _PG_SEARCH_LARGE_ROWS, prefix="Bulk document")
    ctx = search_execution_context(pg_client)
    adapter = ctx.search.query(perf_search_spec())

    async def run() -> None:
        page = await adapter.search_page(
            "corpus",
            pagination={"limit": 100, "offset": 0},
        )
        assert page.count >= 0
        assert isinstance(page.hits, list)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
@pytest.mark.parametrize("read_validation", ["strict", "trusted"])
async def test_pg_search_read_validation_benchmark(
    async_benchmark,
    pg_client: PostgresClient,
    read_validation: str,
) -> None:
    """Compare hit decode cost: strict vs trusted validation."""

    await ensure_pgroonga_search_schema(pg_client)
    await seed_search_corpus(pg_client, _PG_SEARCH_LARGE_ROWS, prefix="validation")
    ctx = search_execution_context(pg_client, read_validation=read_validation)
    adapter = ctx.search.query(perf_search_spec())

    async def run() -> None:
        page = await adapter.search_page(
            "validation",
            pagination={"limit": 100},
        )
        assert len(page.hits) <= 100

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_search_snapshot_benchmark(
    async_benchmark,
    pg_client: PostgresClient,
) -> None:
    """Benchmark snapshot path (full candidate decode + pool reuse)."""

    await ensure_pgroonga_search_schema(pg_client)
    await seed_search_corpus(pg_client, 500, prefix="snapshot")
    ctx = search_execution_context(pg_client)
    adapter = ctx.search.query(perf_search_spec())

    async def run() -> None:
        page = await adapter.search_page(
            "snapshot",
            pagination={"limit": 20, "offset": 0},
            snapshot={"mode": "enabled"},
        )
        assert isinstance(page.hits, list)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
@pytest.mark.parametrize("pgroonga_plan", ["filter_first", "index_first"])
async def test_pg_search_plan_mode_benchmark(
    async_benchmark,
    pg_client: PostgresClient,
    pgroonga_plan: str,
) -> None:
    """Smoke benchmark for PGroonga plan modes."""

    await ensure_pgroonga_search_schema(pg_client)
    await seed_search_corpus(pg_client, 1000, prefix="plan")
    ctx = search_execution_context(pg_client, pgroonga_plan=pgroonga_plan)
    adapter = ctx.search.query(perf_search_spec())

    async def run() -> None:
        page = await adapter.search_page(
            "plan",
            pagination={"limit": 50},
        )
        assert isinstance(page.hits, list)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
@pytest.mark.parametrize("search_count", ["exact", "approximate"])
async def test_pg_search_count_policy_benchmark(
    async_benchmark,
    pg_client: PostgresClient,
    search_count: str,
) -> None:
    """Benchmark ranked search with different count policies."""

    await ensure_pgroonga_search_schema(pg_client)
    await seed_search_corpus(pg_client, 1000, prefix="count")
    ctx = search_execution_context(pg_client)
    adapter = ctx.search.query(perf_search_spec())

    async def run() -> None:
        page = await adapter.search_page(
            "count",
            pagination={"limit": 25},
            options={"search_count": search_count},
        )
        assert page.count is None or page.count >= 0

    await async_benchmark(run)
