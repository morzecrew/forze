"""Integration tests for Meilisearch facets & highlights."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchCommandDepKey,
    SearchManagementDepKey,
    SearchQueryDepKey,
    SearchSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze_meilisearch.execution.deps import (
    ConfigurableMeilisearchSearch,
    ConfigurableMeilisearchSearchCommand,
    MeilisearchClientDepKey,
    MeilisearchSearchConfig,
)
from forze_meilisearch.execution.deps.factories import (
    ConfigurableMeilisearchSearchManagement,
)
from tests.support.execution_context import context_from_deps

# ----------------------- #


class Article(BaseModel):
    id: str
    title: str
    body: str = ""
    category: str = "general"


def _ctx(meilisearch_client, *, index_uid: str) -> ExecutionContext:
    cfg = MeilisearchSearchConfig(index_uid=index_uid)
    return context_from_deps(
        Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                SearchQueryDepKey: ConfigurableMeilisearchSearch(config=cfg),
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(config=cfg),
                SearchManagementDepKey: ConfigurableMeilisearchSearchManagement(
                    config=cfg
                ),
            }
        )
    )


def _spec() -> SearchSpec[Article]:
    return SearchSpec(
        name="articles",
        model_type=Article,
        fields=["title", "body"],
        facetable_fields=frozenset({"category"}),
    )


async def _seed(ctx: ExecutionContext, spec: SearchSpec[Article]) -> None:
    mgmt = ctx.search.management(spec)
    await mgmt.ensure_index()
    await mgmt.delete_all()
    await ctx.search.command(spec).upsert(
        [
            Article(id="1", title="Rust Book", body="systems", category="books"),
            Article(id="2", title="Python Book", body="scripting", category="books"),
            Article(id="3", title="Gaming Mouse", body="hardware", category="gear"),
        ]
    )


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_meilisearch_facets(meilisearch_client) -> None:
    ctx = _ctx(meilisearch_client, index_uid="articles_facets_it")
    spec = _spec()
    await _seed(ctx, spec)

    page = await ctx.search.query(spec).search_page(
        "book", options={"facets": ["category"]}
    )

    assert page.facets is not None
    cat = {b.value: b.count for b in page.facets["category"]}
    # Both books match the "book" query; the "gear" article does not.
    assert cat == {"books": 2}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_meilisearch_highlights(meilisearch_client) -> None:
    ctx = _ctx(meilisearch_client, index_uid="articles_hl_it")
    spec = _spec()
    await _seed(ctx, spec)

    page = await ctx.search.query(spec).search_page(
        "book", options={"highlight": {"fields": ["title"]}}
    )

    assert page.highlights is not None
    assert len(page.highlights) == len(page.hits)
    fragments = [hl["title"][0] for hl in page.highlights if "title" in hl]
    assert fragments
    assert all("<em>" in frag.lower() and "book" in frag.lower() for frag in fragments)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_meilisearch_facet_on_non_facetable_field_refused(
    meilisearch_client,
) -> None:
    ctx = _ctx(meilisearch_client, index_uid="articles_facet_guard_it")
    spec = _spec()
    await _seed(ctx, spec)

    with pytest.raises(CoreException) as ei:
        await ctx.search.query(spec).search_page("book", options={"facets": ["title"]})

    assert ei.value.kind is ExceptionKind.PRECONDITION
