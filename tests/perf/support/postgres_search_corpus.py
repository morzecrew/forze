"""Shared PGroonga search corpus setup for Postgres perf benchmarks."""

from uuid import UUID, uuid4

from pydantic import BaseModel

from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze_postgres.execution.deps import ConfigurablePostgresSearch
from forze_postgres.execution.deps.configs import PostgresSearchConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

# ----------------------- #

PERF_SEARCH_TABLE = "perf_search_items"
PERF_SEARCH_INDEX = "idx_perf_search_pgroonga"


class SearchableModel(BaseModel):
    """Search result model for perf tests."""

    id: UUID
    title: str
    content: str


# ....................... #


def perf_search_spec() -> SearchSpec[SearchableModel]:
    return SearchSpec(
        name="perf_search_ns",
        model_type=SearchableModel,
        fields=["title", "content"],
    )


# ....................... #


async def ensure_pgroonga_search_schema(pg_client: PostgresClient) -> None:
    """Create PGroonga table and index used by search perf tests."""

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    await pg_client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {PERF_SEARCH_TABLE} (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            content text NOT NULL
        );
        """
    )
    await pg_client.execute(
        f"""
        CREATE INDEX IF NOT EXISTS {PERF_SEARCH_INDEX}
        ON {PERF_SEARCH_TABLE} USING pgroonga ((ARRAY[title, content]));
        """
    )


# ....................... #


async def seed_search_corpus(
    pg_client: PostgresClient,
    n: int,
    *,
    prefix: str = "document",
) -> None:
    """Insert *n* searchable rows via ``execute_many``."""

    await pg_client.execute(f"TRUNCATE {PERF_SEARCH_TABLE}")
    await pg_client.execute_many(
        f"""
        INSERT INTO {PERF_SEARCH_TABLE} (id, title, content)
        VALUES (%(id)s, %(title)s, %(content)s)
        """,
        [
            {
                "id": uuid4(),
                "title": f"{prefix} {i}",
                "content": f"Content for {prefix} {i} with searchable terms",
            }
            for i in range(n)
        ],
    )


# ....................... #


def search_execution_context(
    pg_client: PostgresClient,
    *,
    read_validation: str = "strict",
    pgroonga_plan: str = "filter_first",
) -> ExecutionContext:
    """Build execution context with configurable search dep."""

    deps = Deps.plain(
        {
            PostgresClientDepKey: pg_client,
            PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            SearchQueryDepKey: ConfigurablePostgresSearch(
                config=PostgresSearchConfig(
                    index=("public", PERF_SEARCH_INDEX),
                    read=("public", PERF_SEARCH_TABLE),
                    engine="pgroonga",
                    read_validation=read_validation,  # type: ignore[arg-type]
                    pgroonga_plan=pgroonga_plan,  # type: ignore[arg-type]
                )
            ),
        }
    )
    return context_from_deps(deps)
