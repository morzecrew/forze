"""Integration tests for native Postgres FTS (``ConfigurablePostgresSearch`` → ``PostgresFTSSearchAdapter``)."""

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.base import CursorPage
from forze.application.contracts.query import QueryFilterExpression
from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze_postgres.adapters.search import PostgresFTSSearchAdapter
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

    assert isinstance(adapter, PostgresFTSSearchAdapter)

    __p = await adapter.search("postgres OR search", return_count=True)
    res = __p.hits
    total = __p.count
    assert total == 1
    assert len(res) == 1
    assert res[0].title == "PostgreSQL FTS"

    __p = await adapter.search(
        "text",
        pagination={"limit": 1, "offset": 0},
        sorts={"title": "asc"},
        options={"weights": {"title": 0.6, "content": 0.4}},
        return_count=True,
    )
    page = __p.hits
    n_total = __p.count
    assert n_total >= 1
    assert len(page) == 1

    __p = await adapter.search("text", return_fields=["title"], return_count=True)
    titles = __p.hits
    t2 = __p.count
    assert t2 >= 1
    assert set(titles[0].keys()) == {"title"}

    __p = await adapter.search("zzzabsenttoken", return_count=True)
    no_hits = __p.hits
    n_zero = __p.count
    assert n_zero == 0
    assert no_hits == []

    class TitleSlice(BaseModel):
        title: str

    __p = await adapter.search(
        "postgres OR search", return_type=TitleSlice, return_count=True
    )
    slim = __p.hits
    n_slim = __p.count
    assert n_slim == 1
    assert slim[0].title == "PostgreSQL FTS"

    __p = await adapter.search(["search", "recipe"], return_count=True)
    multi = __p.hits
    n_multi = __p.count
    assert n_multi == 2
    assert {r.title for r in multi} == {"PostgreSQL FTS", "Cooking"}

    __p = await adapter.search("search OR recipe", return_count=True)
    str_or = __p.hits
    n_str = __p.count
    assert n_str == n_multi
    assert {r.title for r in str_or} == {r.title for r in multi}

    __p = await adapter.search(
        ["search", "full"], options={"phrase_combine": "all"}, return_count=True
    )
    and_hits = __p.hits
    n_and = __p.count
    assert n_and == 1
    assert and_hits[0].title == "PostgreSQL FTS"


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
    __p = await adapter.search("", filters=flt, return_count=True)
    rows = __p.hits
    cnt = __p.count
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

    assert isinstance(adapter, PostgresFTSSearchAdapter)

    __p = await adapter.search("fts", return_count=True)
    res = __p.hits
    total = __p.count
    assert total == 1
    assert len(res) == 1
    assert res[0].title == "view fts"


@pytest.mark.asyncio
async def test_fts_adapter_v2_direct_projection_heap_and_index_field_map(
    pg_client: PostgresClient,
) -> None:
    """Instantiate :class:`PostgresFTSSearchAdapter` with view, heap, and ``index_field_map``."""

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
    adapter = PostgresFTSSearchAdapter(
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

    __p = await adapter.search("hello", return_count=True)
    rows = __p.hits
    n = __p.count
    assert n == 1
    assert rows[0].title == "hello fts"


@pytest.mark.asyncio
async def test_fts_search_with_cursor_ranked_and_browse(
    pg_client: PostgresClient,
) -> None:
    suffix = uuid4().hex[:12]
    table = f"fts_cur_{suffix}"
    index_name = f"idx_fts_cur_{suffix}"

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
    for t in ("a", "b", "c"):
        await pg_client.execute(
            f"""
            INSERT INTO {table} (id, title, content)
            VALUES (%(id)s, %(title)s, 'common token')
            """,
            {"id": uuid4(), "title": t},
        )

    ctx = _fts_context(pg_client, table=table, index_name=index_name)
    spec = SearchSpec(
        name="fts_cur",
        model_type=FtsArticle,
        fields=["title", "content"],
    )
    adapter = ctx.search_query(spec)

    p1: CursorPage = await adapter.search_with_cursor(
        "common",
        sorts={"title": "asc"},
        return_fields=["title", "content", "id"],
        cursor={"limit": 1},
    )
    assert len(p1.hits) == 1
    assert set(p1.hits[0].keys()) == {"title", "content", "id"}
    assert p1.has_more is True
    assert p1.next_cursor is not None

    p2 = await adapter.search_with_cursor(
        "common",
        sorts={"title": "asc"},
        return_fields=["title", "content", "id"],
        cursor={"limit": 5, "after": p1.next_cursor},
    )
    assert len(p2.hits) == 2

    b0 = await adapter.search_with_cursor(
        "",
        sorts={"title": "asc"},
        return_fields=["title", "content", "id"],
        cursor={"limit": 2},
    )
    assert len(b0.hits) == 2
    assert b0.has_more is True


@pytest.mark.asyncio
async def test_fts_phrase_combine_any_vs_all_multi_term(
    pg_client: PostgresClient,
) -> None:
    """List query with ``phrase_combine: "any"`` is broader than ``\"all"`` (disjunction vs conjunction)."""
    suffix = uuid4().hex[:12]
    table = f"fts_pc_{suffix}"
    index_name = f"idx_fts_pc_{suffix}"

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
    await pg_client.execute(
        f"""
        INSERT INTO {table} (id, title, content) VALUES
        (%(a)s, 'a', 'giraffe only here'),
        (%(b)s, 'b', 'zebra only there'),
        (%(c)s, 'c', 'giraffe and zebra both');
        """,
        {"a": uuid4(), "b": uuid4(), "c": uuid4()},
    )
    ctx = _fts_context(pg_client, table=table, index_name=index_name)
    spec = SearchSpec(
        name="fts_pc",
        model_type=FtsArticle,
        fields=["title", "content"],
    )
    adapter = ctx.search_query(spec)
    assert isinstance(adapter, PostgresFTSSearchAdapter)
    p_any = await adapter.search(
        ["giraffe", "zebra"],
        options={"phrase_combine": "any"},
        return_count=True,
    )
    p_all = await adapter.search(
        ["giraffe", "zebra"],
        options={"phrase_combine": "all"},
        return_count=True,
    )
    assert p_any.count == 3
    assert p_all.count == 1
    assert p_all.hits[0].title == "c"


@pytest.mark.asyncio
async def test_fts_search_with_cursor_return_type_and_before(
    pg_client: PostgresClient,
) -> None:
    """Ranked FTS cursor: ``return_type`` and backward ``before`` keyset (same query + sorts)."""
    suffix = uuid4().hex[:12]
    table = f"fts_cb_{suffix}"
    index_name = f"idx_fts_cb_{suffix}"

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
    for t in ("a", "b", "c"):
        await pg_client.execute(
            f"""
            INSERT INTO {table} (id, title, content)
            VALUES (%(id)s, %(t)s, 'curtok shared for cursor');
            """,
            {"id": uuid4(), "t": t},
        )
    ctx = _fts_context(pg_client, table=table, index_name=index_name)
    spec = SearchSpec(
        name="fts_cb",
        model_type=FtsArticle,
        fields=["title", "content"],
    )
    adapter = ctx.search_query(spec)
    assert isinstance(adapter, PostgresFTSSearchAdapter)

    class FtsTitleId(BaseModel):
        id: UUID
        title: str

    p0: CursorPage = await adapter.search_with_cursor(
        "curtok",
        sorts={"title": "asc"},
        return_type=FtsTitleId,
        cursor={"limit": 1},
    )
    assert len(p0.hits) == 1
    assert isinstance(p0.hits[0], FtsTitleId)
    assert p0.hits[0].title == "a"
    assert p0.has_more is True
    assert p0.next_cursor is not None

    p1 = await adapter.search_with_cursor(
        "curtok",
        sorts={"title": "asc"},
        return_type=FtsTitleId,
        cursor={"limit": 1, "after": p0.next_cursor},
    )
    assert len(p1.hits) == 1
    assert p1.hits[0].title == "b"
    assert p1.next_cursor is not None

    p_back: CursorPage = await adapter.search_with_cursor(
        "curtok",
        sorts={"title": "asc"},
        return_type=FtsTitleId,
        cursor={"limit": 2, "before": p1.next_cursor},
    )
    assert len(p_back.hits) >= 1
    assert p_back.hits[0].title == "a"


@pytest.mark.asyncio
async def test_fts_v2_ranked_count_zero_short_circuits(
    pg_client: PostgresClient,
) -> None:
    """``return_count`` with a token that never matches: early empty page (no data query)."""
    suffix = uuid4().hex[:12]
    table = f"fts_zc_{suffix}"
    index_name = f"idx_fts_zc_{suffix}"
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
        """,
    )
    await pg_client.execute(
        f"""
        INSERT INTO {table} (id, title, content) VALUES
        (%(id)s, 'x', 'only zzyxxy token');
        """,
        {"id": uuid4()},
    )
    ctx = _fts_context(pg_client, table=table, index_name=index_name)
    spec = SearchSpec(
        name="fts_zc",
        model_type=FtsArticle,
        fields=["title", "content"],
    )
    adapter = ctx.search_query(spec)
    p = await adapter.search("zzznotokenmatchunique123", return_count=True)
    assert p.count == 0
    assert p.hits == []


@pytest.mark.asyncio
async def test_fts_v2_search_with_cursor_ranked_return_type_and_fields(
    pg_client: PostgresClient,
) -> None:
    """Ranked FTS: ``return_type`` and ``return_fields`` on keyset (non-default model / dict)."""
    suffix = uuid4().hex[:12]
    table = f"fts_rtf_{suffix}"
    index_name = f"idx_fts_rtf_{suffix}"
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
        """,
    )
    for t in ("a", "b"):
        await pg_client.execute(
            f"""
            INSERT INTO {table} (id, title, content)
            VALUES (%(id)s, %(t)s, 'sharedtok');
            """,
            {"id": uuid4(), "t": t},
        )
    ctx = _fts_context(pg_client, table=table, index_name=index_name)
    spec = SearchSpec(
        name="fts_rtf",
        model_type=FtsArticle,
        fields=["title", "content"],
    )
    adapter = ctx.search_query(spec)
    assert isinstance(adapter, PostgresFTSSearchAdapter)

    class TitleOnly(BaseModel):
        title: str

    p1: CursorPage = await adapter.search_with_cursor(
        "sharedtok",
        sorts={"title": "asc"},
        return_type=TitleOnly,
        cursor={"limit": 1},
    )
    assert len(p1.hits) == 1
    assert isinstance(p1.hits[0], TitleOnly)

    p2: CursorPage = await adapter.search_with_cursor(
        "sharedtok",
        sorts={"title": "asc"},
        return_fields=["title", "id"],
        cursor={"limit": 2},
    )
    assert len(p2.hits) == 2
    assert set(p2.hits[0].keys()) == {"title", "id"}
