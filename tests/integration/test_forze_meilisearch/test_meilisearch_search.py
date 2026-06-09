"""Integration tests for Meilisearch simple search."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchCommandDepKey,
    SearchQueryDepKey,
    SearchSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze_meilisearch.adapters.search import MeilisearchSimpleSearchAdapter
from forze_meilisearch.execution.deps import (
    ConfigurableMeilisearchSearch,
    ConfigurableMeilisearchSearchCommand,
    MeilisearchClientDepKey,
    MeilisearchSearchConfig,
)
from tests.support.execution_context import context_from_deps

# ----------------------- #


class Article(BaseModel):
    id: str
    title: str
    body: str = ""


def _ctx(meilisearch_client, *, index_uid: str) -> ExecutionContext:
    return context_from_deps(Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                SearchQueryDepKey: ConfigurableMeilisearchSearch(
                    config=MeilisearchSearchConfig(index_uid=index_uid),
                ),
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(
                    config=MeilisearchSearchConfig(index_uid=index_uid),
                ),
            }
        )
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_meilisearch_search_upsert_and_query(meilisearch_client) -> None:
    index_uid = "articles_it"
    ctx = _ctx(meilisearch_client, index_uid=index_uid)
    spec = SearchSpec(name="articles", model_type=Article, fields=["title", "body"])

    cmd = ctx.search.command(spec)
    await cmd.ensure_index()
    await cmd.delete_all()
    await cmd.upsert(
        [
            Article(id="1", title="Meilisearch integration", body="search engine"),
            Article(id="2", title="Cooking", body="recipes"),
        ]
    )

    adapter = ctx.search.query(spec)
    assert isinstance(adapter, MeilisearchSimpleSearchAdapter)

    page = await adapter.search_page("meilisearch")
    assert page.count == 1
    assert page.hits[0].title == "Meilisearch integration"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_meilisearch_search_with_filters_sorts_and_cursor(
    meilisearch_client,
) -> None:
    index_uid = "articles_adv_it"
    ctx = _ctx(meilisearch_client, index_uid=index_uid)
    spec = SearchSpec(
        name="articles",
        model_type=Article,
        fields=["title", "body"],
    )
    cfg = MeilisearchSearchConfig(
        index_uid=index_uid,
        filterable_attributes=["title"],
        sortable_attributes=["title"],
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                SearchQueryDepKey: ConfigurableMeilisearchSearch(config=cfg),
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(config=cfg),
            },
        ),
    )

    cmd = ctx.search.command(spec)
    await cmd.ensure_index()
    await cmd.delete_all()
    await cmd.upsert(
        [
            Article(id="1", title="alpha-z", body="first"),
            Article(id="2", title="beta-z", body="second"),
            Article(id="3", title="gamma", body="third"),
        ],
    )

    adapter = ctx.search.query(spec)
    filtered = await adapter.search_page(
        "z",
        filters={
            "$and": [
                {"$values": {"title": {"$in": ["alpha-z", "beta-z"]}}},
                {"$not": {"$values": {"title": {"$eq": "beta-z"}}}},
            ],
        },
        sorts={"title": "asc"},
        pagination={"offset": 0, "limit": 10},
    )
    assert filtered.count == 1
    assert filtered.hits[0].title == "alpha-z"

    with pytest.raises(CoreException, match="search_cursor is not implemented"):
        await adapter.search_cursor("a", cursor={"limit": 1})
