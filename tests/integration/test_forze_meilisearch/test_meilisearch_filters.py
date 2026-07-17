"""Integration tests for Meilisearch filter rendering."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchCommandDepKey,
    SearchManagementDepKey,
    SearchQueryDepKey,
    SearchSpec,
)
from forze.application.execution import Deps
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
                SearchManagementDepKey: ConfigurableMeilisearchSearchManagement(
                    config=MeilisearchSearchConfig(
                        index_uid=index_uid,
                        filterable_attributes=["category"],
                    ),
                ),
            }
        )
    )

    cmd = ctx.search.command(spec)
    mgmt = ctx.search.management(spec)
    await mgmt.ensure_index()
    await mgmt.delete_all()
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
                SearchManagementDepKey: ConfigurableMeilisearchSearchManagement(
                    config=MeilisearchSearchConfig(
                        index_uid=index_uid,
                        filterable_attributes=["category", "title"],
                    ),
                ),
            },
        ),
    )

    cmd = ctx.search.command(spec)
    mgmt = ctx.search.management(spec)
    await mgmt.ensure_index()
    await mgmt.delete_all()
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
                SearchManagementDepKey: ConfigurableMeilisearchSearchManagement(
                    config=cfg,
                ),
            },
        ),
    )

    cmd = ctx.search.command(spec)
    mgmt = ctx.search.management(spec)
    await mgmt.ensure_index()
    await mgmt.delete_all()
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


class PricedProduct(BaseModel):
    id: str
    title: str
    price: Decimal = Decimal("0")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_decimal_field_filters_and_sorts_numerically(meilisearch_client) -> None:
    """Decimal fields index as JSON numbers — Decimal filter values range/equal correctly
    and sort numerically (9.5 < 10.5 < 100.25; a string index would sort lexically and
    range-filter to nothing), then round-trip back into the Decimal read field."""

    index_uid = "products_decimal_it"
    spec = SearchSpec(
        name="products",
        model_type=PricedProduct,
        fields=["title"],
    )
    cfg = MeilisearchSearchConfig(
        index_uid=index_uid,
        filterable_attributes=["price"],
        sortable_attributes=["price"],
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                SearchQueryDepKey: ConfigurableMeilisearchSearch(config=cfg),
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(config=cfg),
                SearchManagementDepKey: ConfigurableMeilisearchSearchManagement(
                    config=cfg,
                ),
            },
        ),
    )

    cmd = ctx.search.command(spec)
    mgmt = ctx.search.management(spec)
    await mgmt.ensure_index()
    await mgmt.delete_all()
    await cmd.upsert(
        [
            PricedProduct(id="1", title="Apple cheap", price=Decimal("9.5")),
            PricedProduct(id="2", title="Apple mid", price=Decimal("10.5")),
            PricedProduct(id="3", title="Apple dear", price=Decimal("100.25")),
        ],
    )

    query = ctx.search.query(spec)

    page = await query.search_page(
        "apple",
        filters={"$values": {"price": {"$lt": Decimal("10.5")}}},
        pagination={"offset": 0, "limit": 10},
    )
    assert page.count == 1
    assert page.hits[0].id == "1"
    assert page.hits[0].price == Decimal("9.5")

    page = await query.search_page(
        "apple",
        filters={"$values": {"price": {"$eq": Decimal("10.5")}}},
        pagination={"offset": 0, "limit": 10},
    )
    assert page.count == 1 and page.hits[0].id == "2"

    page = await query.search_page(
        "apple",
        sorts={"price": "asc"},
        pagination={"offset": 0, "limit": 10},
    )
    assert [h.id for h in page.hits] == ["1", "2", "3"]


class StampedNote(BaseModel):
    id: str
    title: str
    created_at: datetime


@pytest.mark.integration
@pytest.mark.asyncio
async def test_datetime_filter_matches_indexed_utc_representation(
    meilisearch_client,
) -> None:
    """A UTC datetime indexes as ``…Z`` (json-mode dump); the filter literal renders the
    same form, so ``$eq`` matches and range boundaries compare consistently — the
    ``isoformat`` ``+00:00`` form was a different string and silently matched nothing."""

    index_uid = "notes_datetime_it"
    spec = SearchSpec(
        name="notes",
        model_type=StampedNote,
        fields=["title"],
    )
    cfg = MeilisearchSearchConfig(
        index_uid=index_uid,
        filterable_attributes=["created_at"],
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                SearchQueryDepKey: ConfigurableMeilisearchSearch(config=cfg),
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(config=cfg),
                SearchManagementDepKey: ConfigurableMeilisearchSearchManagement(
                    config=cfg,
                ),
            },
        ),
    )

    cmd = ctx.search.command(spec)
    mgmt = ctx.search.management(spec)
    await mgmt.ensure_index()
    await mgmt.delete_all()

    from datetime import timedelta, timezone

    early = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
    late = datetime(2024, 6, 2, 3, 4, 5, tzinfo=UTC)
    # Same instant as ``early`` expressed at +03:00 — indexing normalizes it to UTC-Z,
    # so a UTC operand still finds it.
    offset_stamp = datetime(2024, 1, 2, 6, 4, 5, tzinfo=timezone(timedelta(hours=3)))
    await cmd.upsert(
        [
            StampedNote(id="1", title="Apple early", created_at=early),
            StampedNote(id="2", title="Apple late", created_at=late),
            StampedNote(id="3", title="Apple offset", created_at=offset_stamp),
        ],
    )

    query = ctx.search.query(spec)

    page = await query.search_page(
        "apple",
        filters={"$values": {"created_at": {"$eq": early}}},
        pagination={"offset": 0, "limit": 10},
    )
    assert page.count == 2
    assert {h.id for h in page.hits} == {"1", "3"}

    page = await query.search_page(
        "apple",
        filters={"$values": {"created_at": {"$gte": datetime(2024, 3, 1, tzinfo=UTC)}}},
        pagination={"offset": 0, "limit": 10},
    )
    assert page.count == 1 and page.hits[0].id == "2"


class Note(BaseModel):
    id: str
    title: str
    is_deleted: bool = False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_facetable_boolean_supports_soft_delete_exclusion(
    meilisearch_client,
) -> None:
    """The contract the aggregate kit's read exclusion rides on: a `facetable_fields`
    member is provisioned filterable by ``ensure_index`` (no explicit
    ``filterable_attributes`` override), so a boolean equality filter on it — the kit's
    conjoined ``is_deleted == False`` exclusion — really excludes flagged documents."""

    index_uid = "notes_soft_delete_it"
    spec = SearchSpec(
        name="notes",
        model_type=Note,
        fields=["title"],
        facetable_fields=frozenset({"is_deleted"}),
    )
    cfg = MeilisearchSearchConfig(index_uid=index_uid)
    ctx = context_from_deps(
        Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                SearchQueryDepKey: ConfigurableMeilisearchSearch(config=cfg),
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(config=cfg),
                SearchManagementDepKey: ConfigurableMeilisearchSearchManagement(
                    config=cfg,
                ),
            },
        ),
    )

    cmd = ctx.search.command(spec)
    mgmt = ctx.search.management(spec)
    await mgmt.ensure_index()
    await mgmt.delete_all()
    await cmd.upsert(
        [
            Note(id="1", title="Apple pie recipe"),
            Note(id="2", title="Apple tart recipe", is_deleted=True),
        ],
    )

    page = await ctx.search.query(spec).search_page(
        "apple",
        filters={
            "$and": [
                {"$values": {"is_deleted": False}},
                {"$values": {"title": {"$neq": "nonexistent"}}},
            ],
        },
        pagination={"offset": 0, "limit": 10},
    )

    assert page.count == 1
    assert page.hits[0].id == "1"
