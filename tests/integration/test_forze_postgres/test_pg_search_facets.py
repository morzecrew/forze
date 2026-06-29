"""Integration tests for Postgres search facets (RFC 0006) — PGroonga + FTS."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchQueryDepKey,
    SearchSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze_postgres.execution.deps import ConfigurablePostgresSearch
from forze_postgres.execution.deps.configs import FtsEngine, PostgresSearchConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

# ----------------------- #


class CatRow(BaseModel):
    id: UUID
    title: str
    content: str
    category: str


async def _bootstrap(pg_client: PostgresClient, *, engine: str) -> tuple[str, str]:
    tag = uuid4().hex[:12]
    table = f"facet_{engine}_{tag}"
    index_name = f"idx_facet_{engine}_{tag}"

    if engine == "pgroonga":
        await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
        index_sql = (
            f"CREATE INDEX {index_name} ON {table} "
            f"USING pgroonga ((ARRAY[title, content]));"
        )
    else:
        index_sql = (
            f"CREATE INDEX {index_name} ON {table} USING gin "
            f"(to_tsvector('english', coalesce(title,'') || ' ' || coalesce(content,'')));"
        )

    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            content text NOT NULL,
            category text NOT NULL
        );
        {index_sql}
        """
    )

    for title, content, category in (
        ("Rust Book", "systems programming", "books"),
        ("Python Book", "scripting language", "books"),
        ("Gaming Mouse", "hardware peripheral", "gear"),
    ):
        await pg_client.execute(
            f"INSERT INTO {table} (id, title, content, category) "
            "VALUES (%(id)s, %(title)s, %(content)s, %(category)s)",
            {
                "id": uuid4(),
                "title": title,
                "content": content,
                "category": category,
            },
        )

    return table, index_name


def _ctx(pg_client: PostgresClient, *, table: str, index_name: str, engine: str) -> ExecutionContext:
    engine_cfg: object = (
        "pgroonga"
        if engine == "pgroonga"
        else FtsEngine(groups={"A": ("title",), "B": ("content",)})
    )
    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", index_name),
                        read=("public", table),
                        engine=engine_cfg,  # type: ignore[arg-type]
                    ),
                ),
            }
        )
    )


def _spec(name: str) -> SearchSpec[CatRow]:
    return SearchSpec(
        name=name,
        model_type=CatRow,
        fields=["title", "content"],
        facetable_fields=frozenset({"category"}),
    )


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pgroonga_facets_ranked_query(pg_client: PostgresClient) -> None:
    table, index_name = await _bootstrap(pg_client, engine="pgroonga")
    ctx = _ctx(pg_client, table=table, index_name=index_name, engine="pgroonga")
    spec = _spec("pgr_facet")

    page = await ctx.search.query(spec).search_page(
        "book", options={"facets": ["category"]}
    )

    assert page.facets is not None
    cat = {b.value: b.count for b in page.facets["category"]}
    assert cat == {"books": 2}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pgroonga_facets_empty_query_browse(pg_client: PostgresClient) -> None:
    table, index_name = await _bootstrap(pg_client, engine="pgroonga")
    ctx = _ctx(pg_client, table=table, index_name=index_name, engine="pgroonga")
    spec = _spec("pgr_facet_browse")

    # Empty query -> browse path; facets over the full table.
    page = await ctx.search.query(spec).search_page("", options={"facets": ["category"]})

    assert page.facets is not None
    cat = {b.value: b.count for b in page.facets["category"]}
    assert cat == {"books": 2, "gear": 1}
    # Ordered count-desc.
    assert page.facets["category"][0].value == "books"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fts_facets_ranked_query(pg_client: PostgresClient) -> None:
    table, index_name = await _bootstrap(pg_client, engine="fts")
    ctx = _ctx(pg_client, table=table, index_name=index_name, engine="fts")
    spec = _spec("fts_facet")

    page = await ctx.search.query(spec).search_page(
        "book", options={"facets": ["category"]}
    )

    assert page.facets is not None
    assert {b.value: b.count for b in page.facets["category"]} == {"books": 2}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_facet_size_caps_buckets(pg_client: PostgresClient) -> None:
    table, index_name = await _bootstrap(pg_client, engine="pgroonga")
    ctx = _ctx(pg_client, table=table, index_name=index_name, engine="pgroonga")
    spec = _spec("pgr_facet_cap")

    page = await ctx.search.query(spec).search_page(
        "", options={"facets": ["category"], "facet_size": 1}
    )

    assert page.facets is not None
    assert page.facets["category"] == (
        page.facets["category"][0],
    )
    assert page.facets["category"][0].value == "books"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_facet_on_non_facetable_field_refused(pg_client: PostgresClient) -> None:
    table, index_name = await _bootstrap(pg_client, engine="pgroonga")
    ctx = _ctx(pg_client, table=table, index_name=index_name, engine="pgroonga")
    spec = _spec("pgr_facet_guard")

    with pytest.raises(CoreException) as ei:
        await ctx.search.query(spec).search_page("book", options={"facets": ["title"]})

    assert ei.value.kind is ExceptionKind.PRECONDITION


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pgroonga_highlights(pg_client: PostgresClient) -> None:
    table, index_name = await _bootstrap(pg_client, engine="pgroonga")
    ctx = _ctx(pg_client, table=table, index_name=index_name, engine="pgroonga")
    spec = _spec("pgr_hl")

    page = await ctx.search.query(spec).search_page(
        "book", options={"highlight": {"fields": ["title"]}}
    )

    assert page.highlights is not None
    assert len(page.highlights) == len(page.hits)
    fragments = [hl["title"][0] for hl in page.highlights if "title" in hl]
    assert fragments
    # Default <em> markers wrap the match (rewritten from PGroonga's fixed span).
    assert all("<em>" in frag.lower() and "book" in frag.lower() for frag in fragments)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pgroonga_highlights_custom_tags(pg_client: PostgresClient) -> None:
    table, index_name = await _bootstrap(pg_client, engine="pgroonga")
    ctx = _ctx(pg_client, table=table, index_name=index_name, engine="pgroonga")
    spec = _spec("pgr_hl_tags")

    page = await ctx.search.query(spec).search_page(
        "book",
        options={"highlight": {"fields": ["title"], "pre_tag": "[", "post_tag": "]"}},
    )

    assert page.highlights is not None
    fragments = [hl["title"][0] for hl in page.highlights if "title" in hl]
    assert fragments
    assert all("[" in frag and "]" in frag and "<span" not in frag for frag in fragments)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fts_highlights(pg_client: PostgresClient) -> None:
    table, index_name = await _bootstrap(pg_client, engine="fts")
    ctx = _ctx(pg_client, table=table, index_name=index_name, engine="fts")
    spec = _spec("fts_hl")

    page = await ctx.search.query(spec).search_page(
        "book", options={"highlight": {"fields": ["title"]}}
    )

    assert page.highlights is not None
    assert len(page.highlights) == len(page.hits)
    fragments = [hl["title"][0] for hl in page.highlights if "title" in hl]
    assert fragments
    assert all("<em>book</em>".lower() in frag.lower() for frag in fragments)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_cursor_highlight_request_fails_closed(pg_client: PostgresClient) -> None:
    # Cursor-paginated highlights are not implemented; a request must fail closed.
    table, index_name = await _bootstrap(pg_client, engine="pgroonga")
    ctx = _ctx(pg_client, table=table, index_name=index_name, engine="pgroonga")
    spec = _spec("pgr_hl_cursor_guard")

    with pytest.raises(CoreException) as ei:
        await ctx.search.query(spec).search_cursor(
            "book", cursor={"limit": 5}, options={"highlight": True}
        )

    assert ei.value.kind is ExceptionKind.PRECONDITION
