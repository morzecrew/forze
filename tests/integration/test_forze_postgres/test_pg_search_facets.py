"""Integration tests for Postgres search facets — PGroonga + FTS."""

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
    # Default <em> markers wrap the match (raw field text marked in Python).
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
async def test_pgroonga_highlight_scan_limit_bounds_far_match(
    pg_client: PostgresClient,
) -> None:
    """``highlight_scan_limit`` caps the field text marked: a far match is not highlighted."""
    tag = uuid4().hex[:12]
    table, index_name = f"hl_scan_{tag}", f"idx_hl_scan_{tag}"
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            content text NOT NULL,
            category text NOT NULL
        );
        CREATE INDEX {index_name} ON {table} USING pgroonga ((ARRAY[title, content]));
        """
    )
    # "needle" sits ~240 chars into content — well past a 50-char highlight scan cap.
    far_content = ("x " * 120) + "needle tail"
    await pg_client.execute(
        f"INSERT INTO {table} (id, title, content, category) "
        "VALUES (%(id)s, %(t)s, %(c)s, %(cat)s)",
        {"id": uuid4(), "t": "short title", "c": far_content, "cat": "books"},
    )

    ctx = _ctx(pg_client, table=table, index_name=index_name, engine="pgroonga")

    # No scan limit: the far content match IS highlighted.
    unbounded = SearchSpec(name="hl_unb", model_type=CatRow, fields=["title", "content"])
    page = await ctx.search.query(unbounded).search_page(
        "needle", options={"highlight": {"fields": ["content"]}}
    )
    assert page.highlights is not None
    content_frags = [hl["content"] for hl in page.highlights if "content" in hl]
    assert content_frags and any("<em>needle</em>" in f for f in content_frags[0])

    # A 50-char scan limit: the hit is still found (the index is unbounded), but the
    # far match is past the cap, so no content fragment is produced.
    bounded = SearchSpec(
        name="hl_bnd",
        model_type=CatRow,
        fields=["title", "content"],
        highlight_scan_limit=50,
    )
    page2 = await ctx.search.query(bounded).search_page(
        "needle", options={"highlight": {"fields": ["content"]}}
    )
    assert len(page2.hits) == 1
    assert page2.highlights is not None
    assert all("content" not in hl for hl in page2.highlights)

    # A match within the cap still highlights normally under the same limit.
    page3 = await ctx.search.query(bounded).search_page(
        "short", options={"highlight": {"fields": ["title"]}}
    )
    assert page3.highlights is not None
    title_frags = [hl["title"] for hl in page3.highlights if "title" in hl]
    assert title_frags and any("<em>short</em>" in f for f in title_frags[0])


async def _bootstrap_cyrillic(pg_client: PostgresClient) -> tuple[str, str]:
    tag = uuid4().hex[:12]
    table, index_name = f"cyr_{tag}", f"idx_cyr_{tag}"

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            content text NOT NULL,
            category text NOT NULL
        );
        CREATE INDEX {index_name} ON {table} USING pgroonga ((ARRAY[title, content]));
        """
    )
    for title in ('ООО "БетаМед"', "Гамма Лаб"):
        await pg_client.execute(
            f"INSERT INTO {table} (id, title, content, category) "
            "VALUES (%(id)s, %(t)s, '', 'org')",
            {"id": uuid4(), "t": title},
        )
    return table, index_name


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pgroonga_highlights_cyrillic_preserves_case(
    pg_client: PostgresClient,
) -> None:
    table, index_name = await _bootstrap_cyrillic(pg_client)
    ctx = _ctx(pg_client, table=table, index_name=index_name, engine="pgroonga")
    spec = _spec("pgr_hl_cyr")

    # The query token is lowercased for matching but the fragment is sliced from the original
    # text, so the wrapped match keeps its source casing (no empty highlight on Cyrillic). A
    # lowercase query needs a case-folding index normalizer to *match*, which the test image
    # lacks; that path is covered by the unit tests.
    page = await ctx.search.query(spec).search_page(
        "Бета", options={"highlight": {"fields": ["title"]}}
    )

    assert page.highlights is not None
    fragments = [hl["title"][0] for hl in page.highlights if "title" in hl]
    assert fragments == ['ООО "<em>Бета</em>Мед"']


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
async def test_pgroonga_cursor_highlights(pg_client: PostgresClient) -> None:
    table, index_name = await _bootstrap(pg_client, engine="pgroonga")
    ctx = _ctx(pg_client, table=table, index_name=index_name, engine="pgroonga")
    spec = _spec("pgr_hl_cursor")

    page = await ctx.search.query(spec).search_cursor(
        "book", cursor={"limit": 5}, options={"highlight": {"fields": ["title"]}}
    )

    assert page.highlights is not None
    assert len(page.highlights) == len(page.hits)
    fragments = [hl["title"][0] for hl in page.highlights if "title" in hl]
    assert fragments
    assert all("<em>" in frag.lower() and "book" in frag.lower() for frag in fragments)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fts_cursor_highlights(pg_client: PostgresClient) -> None:
    table, index_name = await _bootstrap(pg_client, engine="fts")
    ctx = _ctx(pg_client, table=table, index_name=index_name, engine="fts")
    spec = _spec("fts_hl_cursor")

    page = await ctx.search.query(spec).search_cursor(
        "book", cursor={"limit": 5}, options={"highlight": {"fields": ["title"]}}
    )

    assert page.highlights is not None
    fragments = [hl["title"][0] for hl in page.highlights if "title" in hl]
    assert fragments
    assert all("<em>book</em>".lower() in frag.lower() for frag in fragments)
