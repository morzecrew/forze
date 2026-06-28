"""Integration tests: search returns lenient read fields hydrated from defaults.

A ``SearchSpec`` read model may declare a returned field with no backing column
(``lenient_read_fields``). It is dropped from the result projection and filled from
its model default; without leniency the same search fails because the projection
references a missing column.
"""

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze_postgres.execution.deps import ConfigurablePostgresSearch
from forze_postgres.execution.deps.configs import FtsEngine, PostgresSearchConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps


class LenientArticle(BaseModel):
    id: UUID
    title: str
    content: str
    summary: str = "n/a"  # returned, no column on the relation


def _ctx(pg_client: PostgresClient, *, table: str, index_name: str) -> ExecutionContext:
    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", index_name),
                        read=("public", table),
                        engine=FtsEngine(groups={"A": ("title",), "B": ("content",)}),
                    )
                ),
            }
        )
    )


async def _make_index_with_row(pg_client: PostgresClient) -> tuple[str, str]:
    suffix = uuid4().hex[:12]
    table = f"lenient_search_{suffix}"
    index_name = f"idx_lenient_{suffix}"

    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            content text NOT NULL
        );
        """
    )
    await pg_client.execute(
        f"""
        CREATE INDEX {index_name} ON {table}
        USING gin (to_tsvector('english', coalesce(title, '') || ' ' || coalesce(content, '')));
        """
    )
    await pg_client.execute(
        f"INSERT INTO {table} (id, title, content) VALUES (%(id)s, %(title)s, %(content)s)",
        {"id": uuid4(), "title": "PostgreSQL FTS", "content": "tsvector search"},
    )

    return table, index_name


@pytest.mark.integration
@pytest.mark.asyncio
async def test_search_lenient_field_hydrates_from_default(
    pg_client: PostgresClient,
) -> None:
    table, index_name = await _make_index_with_row(pg_client)
    ctx = _ctx(pg_client, table=table, index_name=index_name)

    spec = SearchSpec(
        name="lenient_search",
        model_type=LenientArticle,
        fields=["title", "content"],
        lenient_read_fields={"summary"},
    )
    adapter = ctx.search.query(spec)

    page = await adapter.search_page("tsvector")

    assert page.count == 1
    hit = page.hits[0]
    assert hit.title == "PostgreSQL FTS"
    # No ``summary`` column — it comes from the model default, not the row.
    assert hit.summary == "n/a"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_strict_search_of_missing_column_fails(
    pg_client: PostgresClient,
) -> None:
    table, index_name = await _make_index_with_row(pg_client)
    ctx = _ctx(pg_client, table=table, index_name=index_name)

    # Without leniency the projection selects ``summary``, which is not a column.
    spec = SearchSpec(
        name="strict_search",
        model_type=LenientArticle,
        fields=["title", "content"],
    )
    adapter = ctx.search.query(spec)

    with pytest.raises(CoreException):
        await adapter.search_page("tsvector")
