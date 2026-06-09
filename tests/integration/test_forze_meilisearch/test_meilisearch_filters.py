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
    ConfigurableMeilisearchSearch,
    ConfigurableMeilisearchSearchCommand,
    MeilisearchClientDepKey,
    MeilisearchSearchConfig,
)
from tests.support.execution_context import context_from_deps

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
    ctx = context_from_deps(Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                SearchQueryDepKey: ConfigurableMeilisearchSearch(
                    config=MeilisearchSearchConfig(
                        index_uid=index_uid,
                        filterable_attributes=["category"],),
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_filter_in_and_or_narrows_hits(meilisearch_client) -> None:
    index_uid = "products_filter_combo_it"
    spec = SearchSpec(
        name="products",
        model_type=Product,
        fields=["title"],
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                SearchQueryDepKey: ConfigurableMeilisearchSearch(
                    config=MeilisearchSearchConfig(
                        index_uid=index_uid,
                        filterable_attributes=["category", "title"],
                    ),
                ),
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(
                    config=MeilisearchSearchConfig(
                        index_uid=index_uid,
                        filterable_attributes=["category", "title"],
                    ),
                ),
            },
        ),
    )

    cmd = ctx.search.command(spec)
    await cmd.ensure_index()
    await cmd.delete_all()
    await cmd.upsert(
        [
            Product(id="1", title="Red apple", category="food"),
            Product(id="2", title="Green apple", category="food"),
            Product(id="3", title="Apple phone", category="tech"),
        ],
    )

    page = await ctx.search.query(spec).search_page(
        "apple",
        filters={
            "$and": [
                {"$values": {"category": {"$in": ["food"]}}},
                {
                    "$or": [
                        {"$values": {"title": {"$eq": "Red apple"}}},
                        {"$values": {"title": {"$eq": "Green apple"}}},
                    ],
                },
            ],
        },
        pagination={"offset": 0, "limit": 10},
    )

    assert page.count == 2
    titles = {hit.title for hit in page.hits}
    assert titles == {"Red apple", "Green apple"}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_filter_comparison_and_null(meilisearch_client) -> None:
    index_uid = "products_filter_cmp_it"
    spec = SearchSpec(
        name="products",
        model_type=Product,
        fields=["title"],
    )
    cfg = MeilisearchSearchConfig(
        index_uid=index_uid,
        filterable_attributes=["category", "title"],
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
            Product(id="1", title="Apple", category="food"),
            Product(id="2", title="Phone", category="tech"),
        ],
    )

    page = await ctx.search.query(spec).search_page(
        "a",
        filters={
            "$or": [
                {"$values": {"category": {"$eq": "food"}}},
                {"$values": {"title": {"$neq": "Phone"}}},
            ],
        },
        pagination={"offset": 0, "limit": 10},
    )
    assert page.count >= 1
