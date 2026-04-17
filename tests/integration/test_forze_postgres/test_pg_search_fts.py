"""Integration tests for native Postgres FTS (``ConfigurablePostgresSearch`` → ``PostgresFTSSearchAdapterV2``)."""

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.query import QueryFilterExpression
from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze_postgres.adapters.search import PostgresFTSSearchAdapterV2
from forze_postgres.execution.deps.deps import ConfigurablePostgresSearch
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.gateways import PostgresQualifiedName
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient


class FtsArticle(BaseModel):
    id: UUID
    title: str
    content: str


def _fts_context(
    pg_client: PostgresClient,
    *,
    table: str,
    index_name: str,
) -> ExecutionContext:
    return ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config={
                        "index": ("public", index_name),
                        "read": ("public", table),
                        "engine": "fts",
                        "fts_groups": {
                            "A": ("title",),
                            "B": ("content",),
                        },
                    }
                ),
            }
        )
    )


@pytest.mark.asyncio
async def test_fts_search_counts_and_ranks(pg_client: PostgresClient) -> None:
    """GIN ``tsvector`` index on a table; index name differs from table name."""
    suffix = uuid4().hex[:12]
    table = f"fts_articles_{suffix}"
    index_name = f"idx_fts_gin_{suffix}"

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
        CREATE INDEX {index_name}
        ON {table}
        USING gin (to_tsvector('english', coalesce(title, '') || ' ' || coalesce(content, '')));
        """
    )

    docs = [
        {
            "id": uuid4(),
            "title": "PostgreSQL FTS",
            "content": "Full text search with tsvector",
        },
        {
            "id": uuid4(),
            "title": "Cooking",
            "content": "Recipes without database jargon",
        },
    ]
    for row in docs:
        await pg_client.execute(
            f"""
            INSERT INTO {table} (id, title, content)
            VALUES (%(id)s, %(title)s, %(content)s)
            """,
            row,
        )

    ctx = _fts_context(pg_client, table=table, index_name=index_name)
    spec = SearchSpec(
        name="fts_ns",
        model_type=FtsArticle,
        fields=["title", "content"],
    )
    adapter = ctx.search_query(spec)

    assert isinstance(adapter, PostgresFTSSearchAdapterV2)

    res, total = await adapter.search("postgres OR search")
    assert total == 1
    assert len(res) == 1
    assert res[0].title == "PostgreSQL FTS"

    page, n_total = await adapter.search(
        "text",
        pagination={"limit": 1, "offset": 0},
        sorts={"title": "asc"},
        options={"weights": {"title": 0.6, "content": 0.4}},
    )
    assert n_total >= 1
    assert len(page) == 1

    titles, t2 = await adapter.search("text", return_fields=["title"])
    assert t2 >= 1
    assert set(titles[0].keys()) == {"title"}


@pytest.mark.asyncio
async def test_fts_search_with_filters_and_empty_query(
    pg_client: PostgresClient,
) -> None:
    """Filter-only path (empty search string) and structured filters."""
    suffix = uuid4().hex[:12]
    table = f"fts_filter_{suffix}"
    index_name = f"idx_fts_filter_{suffix}"

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
        CREATE INDEX {index_name}
        ON {table}
        USING gin (to_tsvector('english', title || ' ' || content));
        """
    )

    await pg_client.execute(
        f"""
        INSERT INTO {table} (id, title, content) VALUES
        (%(a)s, 'keep', 'alpha'),
        (%(b)s, 'drop', 'beta')
        """,
        {"a": uuid4(), "b": uuid4()},
    )

    ctx = _fts_context(pg_client, table=table, index_name=index_name)
    spec = SearchSpec(
        name="fts_filter_ns",
        model_type=FtsArticle,
        fields=["title", "content"],
    )
    adapter = ctx.search_query(spec)

    flt: QueryFilterExpression = {"$fields": {"title": "keep"}}
    rows, cnt = await adapter.search("", filters=flt)
    assert cnt == 1
    assert rows[0].title == "keep"


@pytest.mark.asyncio
async def test_fts_v2_projection_view_and_heap_split(pg_client: PostgresClient) -> None:
    """Index and filters use distinct relations: GIN on base table, rows from a view."""

    suffix = uuid4().hex[:12]
    table = f"fts_base_{suffix}"
    view = f"fts_proj_{suffix}"
    index_name = f"idx_fts_v2_{suffix}"

    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            content text NOT NULL
        );
        CREATE VIEW {view} AS SELECT * FROM {table};
        """
    )

    await pg_client.execute(
        f"""
        CREATE INDEX {index_name}
        ON {table}
        USING gin (to_tsvector('english', coalesce(title, '') || ' ' || coalesce(content, '')));
        """
    )

    await pg_client.execute(
        f"""
        INSERT INTO {table} (id, title, content) VALUES (%(id)s, 'view fts', 'heap indexed');
        """,
        {"id": uuid4()},
    )

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config={
                        "index": ("public", index_name),
                        "read": ("public", view),
                        "heap": ("public", table),
                        "engine": "fts",
                        "fts_groups": {
                            "A": ("title",),
                            "B": ("content",),
                        },
                    }
                ),
            }
        )
    )

    spec = SearchSpec(
        name="fts_v2_view",
        model_type=FtsArticle,
        fields=["title", "content"],
    )
    adapter = ctx.search_query(spec)

    assert isinstance(adapter, PostgresFTSSearchAdapterV2)

    res, total = await adapter.search("fts")
    assert total == 1
    assert len(res) == 1
    assert res[0].title == "view fts"


@pytest.mark.asyncio
async def test_fts_adapter_v2_direct_projection_heap_and_index_field_map(
    pg_client: PostgresClient,
) -> None:
    """Instantiate :class:`PostgresFTSSearchAdapterV2` with view, heap, and ``index_field_map``."""

    suffix = uuid4().hex[:12]
    heap = f"fts_heap_fm_{suffix}"
    proj = f"fts_proj_fm_{suffix}"
    idx = f"idx_fts_heap_fm_{suffix}"

    await pg_client.execute(
        f"""
        CREATE TABLE {heap} (
            id uuid PRIMARY KEY,
            c1 text NOT NULL,
            c2 text NOT NULL
        );
        CREATE VIEW {proj} AS
        SELECT id, c1 AS title, c2 AS content FROM {heap};
        CREATE INDEX {idx} ON {heap}
        USING gin (to_tsvector('english', coalesce(c1, '') || ' ' || coalesce(c2, '')));
        """
    )
    await pg_client.execute(
        f"INSERT INTO {heap} (id, c1, c2) VALUES (%(id)s, 'hello fts', 'world')",
        {"id": uuid4()},
    )

    introspector = PostgresIntrospector(client=pg_client)
    spec = SearchSpec(
        name="fts_fm",
        model_type=FtsArticle,
        fields=["title", "content"],
    )
    adapter = PostgresFTSSearchAdapterV2(
        spec=spec,
        index_qname=PostgresQualifiedName("public", idx),
        source_qname=PostgresQualifiedName("public", proj),
        index_heap_qname=PostgresQualifiedName("public", heap),
        fts_groups={"A": ("title",), "B": ("content",)},
        index_field_map={"title": "c1", "content": "c2"},
        client=pg_client,
        model_type=FtsArticle,
        introspector=introspector,
        tenant_provider=None,
        tenant_aware=False,
    )

    rows, n = await adapter.search("hello")
    assert n == 1
    assert rows[0].title == "hello fts"
