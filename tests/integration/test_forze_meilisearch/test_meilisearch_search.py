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
