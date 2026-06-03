"""Performance tests for PostgresHubSearchAdapter (parallel + cursor)."""

from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from pydantic import BaseModel

pytest.importorskip("psycopg")

from forze.application.contracts.search import HubSearchSpec, SearchSpec
from forze.application.execution import Deps
from forze_postgres.execution.deps import ConfigurablePostgresHubSearch
from forze_postgres.execution.deps.configs import (
    PostgresHubSearchConfig,
    PostgresHubSearchMemberConfig,
)
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps


class _HubLegTxt(BaseModel):
    name: str
    display_name: str


class _HubLink(BaseModel):
    id: UUID
    detail_id: UUID
    spec_id: UUID
    quantity: int


def _hub_config(
    *,
    execution: str = "sql",
    read_validation: str = "strict",
) -> PostgresHubSearchConfig:
    return PostgresHubSearchConfig(
        hub=("public", "perf_hub_links"),
        members={
            "detail_txt": PostgresHubSearchMemberConfig(
                index=("public", "idx_perf_hub_details_pg"),
                read=("public", "perf_hub_details"),
                engine="pgroonga",
                hub_fk="detail_id",
            ),
            "spec_txt": PostgresHubSearchMemberConfig(
                index=("public", "idx_perf_hub_specs_pg"),
                read=("public", "perf_hub_specs"),
                engine="pgroonga",
                hub_fk="spec_id",
            ),
        },
        execution=execution,  # type: ignore[arg-type]
        read_validation=read_validation,  # type: ignore[arg-type]
        per_leg_limit=500,
        combo_limit=200,
    )


def _hub_spec() -> HubSearchSpec[_HubLink]:
    return HubSearchSpec(
        name="perf_hub_search",
        model_type=_HubLink,
        members=(
            SearchSpec(
                name="detail_txt",
                model_type=_HubLegTxt,
                fields=["name", "display_name"],
            ),
            SearchSpec(
                name="spec_txt",
                model_type=_HubLegTxt,
                fields=["name", "display_name"],
            ),
        ),
    )


async def _ensure_hub_schema(pg_client: PostgresClient) -> None:
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    await pg_client.execute(
        """
        CREATE TABLE IF NOT EXISTS perf_hub_details (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE TABLE IF NOT EXISTS perf_hub_specs (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE TABLE IF NOT EXISTS perf_hub_links (
            id uuid PRIMARY KEY,
            detail_id uuid NOT NULL,
            spec_id uuid NOT NULL,
            quantity int NOT NULL
        );
        """
    )
    await pg_client.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_perf_hub_details_pg ON perf_hub_details
        USING pgroonga ((ARRAY[name, display_name]));
        CREATE INDEX IF NOT EXISTS idx_perf_hub_specs_pg ON perf_hub_specs
        USING pgroonga ((ARRAY[name, display_name]));
        """
    )


async def _seed_hub_corpus(pg_client: PostgresClient, n: int) -> None:
    await pg_client.execute("TRUNCATE perf_hub_details, perf_hub_specs, perf_hub_links")

    detail_ids = [uuid4() for _ in range(n)]
    spec_ids = [uuid4() for _ in range(n)]

    await pg_client.execute_many(
        """
        INSERT INTO perf_hub_details (id, name, display_name)
        VALUES (%(id)s, %(name)s, %(display_name)s)
        """,
        [
            {
                "id": did,
                "name": f"detail term {i}",
                "display_name": f"Detail {i}",
            }
            for i, did in enumerate(detail_ids)
        ],
    )
    await pg_client.execute_many(
        """
        INSERT INTO perf_hub_specs (id, name, display_name)
        VALUES (%(id)s, %(name)s, %(display_name)s)
        """,
        [
            {
                "id": sid,
                "name": f"spec term {i}",
                "display_name": f"Spec {i}",
            }
            for i, sid in enumerate(spec_ids)
        ],
    )
    await pg_client.execute_many(
        """
        INSERT INTO perf_hub_links (id, detail_id, spec_id, quantity)
        VALUES (%(id)s, %(detail_id)s, %(spec_id)s, %(quantity)s)
        """,
        [
            {
                "id": uuid4(),
                "detail_id": detail_ids[i % n],
                "spec_id": spec_ids[i % n],
                "quantity": i + 1,
            }
            for i in range(n)
        ],
    )


@pytest_asyncio.fixture
async def hub_corpus(pg_client: PostgresClient):
    await _ensure_hub_schema(pg_client)
    await _seed_hub_corpus(pg_client, 200)


def _hub_adapter(
    pg_client: PostgresClient,
    *,
    execution: str = "sql",
    read_validation: str = "strict",
):
    deps = Deps.plain(
        {
            PostgresClientDepKey: pg_client,
            PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
        }
    )
    ctx = context_from_deps(deps)
    config = _hub_config(execution=execution, read_validation=read_validation)
    return ConfigurablePostgresHubSearch(config=config)(ctx, _hub_spec())


@pytest.mark.perf
@pytest.mark.asyncio
@pytest.mark.parametrize("execution", ["sql", "parallel"])
async def test_pg_hub_search_execution_benchmark(
    async_benchmark,
    hub_corpus,
    pg_client: PostgresClient,
    execution: str,
) -> None:
    adapter = _hub_adapter(pg_client, execution=execution)

    async def run() -> None:
        page = await adapter.search_page("term", pagination={"limit": 25})
        assert isinstance(page.hits, list)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_pg_hub_parallel_cursor_benchmark(
    async_benchmark,
    hub_corpus,
    pg_client: PostgresClient,
) -> None:
    adapter = _hub_adapter(pg_client, execution="parallel")

    async def run() -> None:
        page = await adapter.search_cursor("term", cursor={"limit": 20})
        assert isinstance(page.hits, list)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
@pytest.mark.parametrize("read_validation", ["strict", "trusted"])
async def test_pg_hub_read_validation_benchmark(
    async_benchmark,
    hub_corpus,
    pg_client: PostgresClient,
    read_validation: str,
) -> None:
    adapter = _hub_adapter(
        pg_client,
        execution="parallel",
        read_validation=read_validation,
    )

    async def run() -> None:
        page = await adapter.search_cursor("term", cursor={"limit": 30})
        assert len(page.hits) <= 30

    await async_benchmark(run)
