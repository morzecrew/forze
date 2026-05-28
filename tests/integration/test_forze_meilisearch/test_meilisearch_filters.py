"""Integration tests for Meilisearch filter rendering."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchCommandDepKey,
    SearchQueryDepKey,
    SearchSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze_meilisearch.execution.deps import (
    MeilisearchClientDepKey,
    MeilisearchSearchConfig,
)
from forze_meilisearch.execution.deps.deps import (
    ConfigurableMeilisearchSearch,
    ConfigurableMeilisearchSearchCommand,
)

# ----------------------- #


class Product(BaseModel):
    id: str
    title: str
    category: str


@pytest.mark.integration
@pytest.mark.asyncio
async def test_filter_eq_narrows_hits(meilisearch_client) -> None:
    index_uid = "products_filter_it"
    spec = SearchSpec(
        name="products",
        model_type=Product,
        fields=["title"],
    )
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                SearchQueryDepKey: ConfigurableMeilisearchSearch(
                    config=MeilisearchSearchConfig(
                        index_uid=index_uid,
                        filterable_attributes=["category"],
                    ),
                ),
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(
                    config=MeilisearchSearchConfig(
                        index_uid=index_uid,
                        filterable_attributes=["category"],
                    ),
                ),
            }
        )
    )

    cmd = ctx.search.command(spec)
    await cmd.ensure_index()
    await cmd.delete_all()
    await cmd.upsert(
        [
            Product(id="1", title="Apple pie", category="food"),
            Product(id="2", title="Apple phone", category="tech"),
        ]
    )

    page = await ctx.search.query(spec).search_page(
        "apple",
        filters={"$values": {"category": {"$eq": "food"}}},
        pagination={"offset": 0, "limit": 10},
    )

    assert page.count == 1
    assert page.hits[0].category == "food"
