"""Postgres hub ``aggregate_search`` over the merged rows of several members.

The hub relation is a view whose measures (``quantity``, ``price_upd_quantity``) are not on
the hub's read model; ``extra_aggregatable_fields`` lets an aggregate measure them. Every leg
is capped at one row, so an aggregate that measured the capped page set would come up short.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.querying import (
    OPERATOR_TYPE_MISMATCH_CODE,
    UNSUPPORTED_QUERY_FEATURE_CODE,
)
from forze.application.contracts.search import HubSearchSpec, SearchSpec
from forze.application.execution import Deps
from forze.base.exceptions import CoreException, ExceptionKind
from forze_postgres.execution.deps import ConfigurablePostgresHubSearch
from forze_postgres.execution.deps.configs import (
    PostgresHubSearchConfig,
    PostgresHubSearchMemberConfig,
)
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #

NORTH: Any = {"$values": {"region": {"$eq": "north"}}}
BY_REGION: Any = {
    "$groups": {"region": "region"},
    "$computed": {
        "n": {"$count": None},
        "quantity": {"$sum": "quantity"},
        "amount": {"$sum": "price_upd_quantity"},
    },
}


class _Leg(BaseModel):
    name: str = ""


class _Link(BaseModel):
    id: UUID
    item_id: UUID
    spec_id: UUID
    region: str


async def _hub(pg_client: PostgresClient, *, extra: frozenset[str]) -> Any:
    """Six links; ``omega`` names items 0-2 and spec 4, so it matches links 0, 1, 2 and 4.

    Link ``k`` is in the north when ``k`` is even, holds ``k + 1`` units at 10.5 each. Its
    ``upd_or_quantity`` (an update's quantity, else the listed one) is 2, 4 and 8 for links 1,
    2 and 4, and null for the rest. Its ``upd_meta`` (``jsonb``) is set for links 1 and 4 only.
    """

    tag = uuid4().hex[:8]
    items, specs, links = f"ha_items_{tag}", f"ha_specs_{tag}", f"ha_links_{tag}"

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    await pg_client.execute(
        f"""
        CREATE TABLE {items} (id uuid PRIMARY KEY, name text NOT NULL);
        CREATE TABLE {specs} (id uuid PRIMARY KEY, name text NOT NULL);
        CREATE TABLE {links} (
            id uuid PRIMARY KEY,
            item_id uuid NOT NULL REFERENCES {items} (id),
            spec_id uuid NOT NULL REFERENCES {specs} (id),
            region text NOT NULL,
            quantity int NOT NULL,
            price numeric NOT NULL,
            upd_quantity int,
            listed_quantity int
        );
        CREATE VIEW {links}_v AS
            SELECT l.*, l.quantity * l.price AS price_upd_quantity,
                COALESCE(l.upd_quantity, l.listed_quantity) AS upd_or_quantity,
                CASE WHEN l.upd_quantity IS NOT NULL
                    THEN jsonb_build_object('qty', l.upd_quantity) END AS upd_meta
            FROM {links} l;
        CREATE INDEX idx_{items} ON {items} USING pgroonga ((ARRAY[name]));
        CREATE INDEX idx_{specs} ON {specs} USING pgroonga ((ARRAY[name]));
        """
    )

    for k in range(6):
        item, spec = uuid4(), uuid4()
        await pg_client.execute(
            f"INSERT INTO {items} (id, name) VALUES (%(id)s, %(n)s)",
            {"id": item, "n": "omega item" if k < 3 else "alpha item"},
        )
        await pg_client.execute(
            f"INSERT INTO {specs} (id, name) VALUES (%(id)s, %(n)s)",
            {"id": spec, "n": "omega spec" if k == 4 else "beta spec"},
        )
        await pg_client.execute(
            f"INSERT INTO {links} (id, item_id, spec_id, region, quantity, price, "
            "upd_quantity, listed_quantity) "
            "VALUES (%(id)s, %(item)s, %(spec)s, %(r)s, %(q)s, 10.5, %(u)s, %(l)s)",
            {
                "id": uuid4(),
                "item": item,
                "spec": spec,
                "r": "north" if k % 2 == 0 else "south",
                "q": k + 1,
                "u": {1: 2, 4: 8}.get(k),
                "l": {1: 99, 2: 4}.get(k),
            },
        )

    def member(heap: str, fk: str) -> PostgresHubSearchMemberConfig:
        return PostgresHubSearchMemberConfig(
            index=("public", f"idx_{heap}"), read=("public", heap), hub_fk=fk, engine="pgroonga"
        )

    config = PostgresHubSearchConfig(
        hub=("public", f"{links}_v"),
        members={"items": member(items, "item_id"), "specs": member(specs, "spec_id")},
        per_leg_limit=1,
    )
    spec = HubSearchSpec(
        name="hub_aggregates",
        model_type=_Link,
        members=(
            SearchSpec(name="items", model_type=_Leg, fields=["name"]),
            SearchSpec(name="specs", model_type=_Leg, fields=["name"]),
        ),
        extra_aggregatable_fields=extra,
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )

    return ConfigurablePostgresHubSearch(config=config)(ctx, spec)


def _groups(page: Any) -> dict[str, tuple[int, int, Decimal]]:
    return {
        row["region"]: (int(row["n"]), int(row["quantity"]), Decimal(str(row["amount"])))
        for row in page.hits
    }


# ....................... #


async def test_a_query_measures_every_merged_match(pg_client: PostgresClient) -> None:
    hub = await _hub(pg_client, extra=frozenset({"quantity", "price_upd_quantity"}))

    page = await hub.aggregate_search_page(BY_REGION, "omega", None, None, {"region": "asc"})

    # North: links 0, 2, 4 (1 + 3 + 5 units); south: link 1 (2 units). Each leg was capped at
    # one row for a page, and neither cap reached the aggregate.
    assert _groups(page) == {
        "north": (3, 9, Decimal("94.5")),
        "south": (1, 2, Decimal("21.0")),
    }
    assert [row["region"] for row in page.hits] == ["north", "south"]
    assert page.count == 2


async def test_a_filter_narrows_what_a_query_measures(pg_client: PostgresClient) -> None:
    hub = await _hub(pg_client, extra=frozenset({"quantity", "price_upd_quantity"}))

    measured = await hub.aggregate_search(BY_REGION, "omega", NORTH)
    blank = await hub.aggregate_search(BY_REGION, "", NORTH)
    counted = await hub.search_page("omega", NORTH, {"limit": 1})

    assert _groups(measured) == {"north": (3, 9, Decimal("94.5"))}
    assert counted.count == 3
    # A blank query measures every north link: 0, 2 and 4, and no others.
    assert _groups(blank) == {"north": (3, 9, Decimal("94.5"))}


async def test_an_extra_field_missing_from_the_hub_relation_is_refused(
    pg_client: PostgresClient,
) -> None:
    hub = await _hub(pg_client, extra=frozenset({"quantity", "price_upd_or_quantity"}))

    with pytest.raises(CoreException) as refused:
        await hub.aggregate_search({"$computed": {"q": {"$sum": "quantity"}}}, "omega")

    assert refused.value.code == "postgres_schema_validation_failed"
    assert "price_upd_or_quantity" in str(refused.value)


async def test_an_extra_field_named_like_a_pipeline_column_is_refused(
    pg_client: PostgresClient,
) -> None:
    hub = await _hub(pg_client, extra=frozenset({"_hub_rank"}))

    with pytest.raises(CoreException) as refused:
        await hub.aggregate_search({"$computed": {"n": {"$count": None}}}, "omega")

    assert refused.value.kind is ExceptionKind.CONFIGURATION
    assert "_hub_rank" in str(refused.value)


# ....................... #

UPD_EXTRA = frozenset({"quantity", "upd_or_quantity", "upd_meta"})


async def test_a_metric_filter_names_an_extra_column(pg_client: PostgresClient) -> None:
    """The issue's query: rows whose computed column is null, counted beside its sum."""

    hub = await _hub(pg_client, extra=UPD_EXTRA)

    page = await hub.aggregate_search(
        {
            "$groups": {"region": "region"},
            "$computed": {
                "total": {"$sum": "upd_or_quantity"},
                "missing": {
                    "$count": {"filter": {"$values": {"upd_or_quantity": {"$null": True}}}}
                },
                "large": {"$count": {"filter": {"$values": {"upd_or_quantity": {"$gte": "4"}}}}},
                "picked": {
                    "$sum": {
                        "field": "quantity",
                        "filter": {"$values": {"upd_or_quantity": {"$in": [2, 8]}}},
                    }
                },
            },
        },
        "omega",
        None,
        None,
        {"region": "asc"},
    )

    # North: links 0 (null), 2 (4) and 4 (8); south: link 1 (2).
    assert [
        (row["region"], int(row["total"]), row["missing"], row["large"], int(row["picked"]))
        for row in page.hits
    ] == [("north", 12, 1, 2, 5), ("south", 2, 0, 0, 2)]


async def test_a_search_filter_still_refuses_an_extra_column(pg_client: PostgresClient) -> None:
    hub = await _hub(pg_client, extra=UPD_EXTRA)

    with pytest.raises(CoreException) as refused:
        await hub.aggregate_search(
            {"$computed": {"n": {"$count": None}}},
            "omega",
            {"$values": {"upd_or_quantity": {"$null": True}}},
        )

    assert refused.value.code == "field_not_on_read_model"


@pytest.mark.parametrize(
    ("predicate", "code"),
    [
        # Typed by the column (int4): a number takes no text, set or emptiness operator.
        ({"upd_or_quantity": {"$like": "4%"}}, OPERATOR_TYPE_MISMATCH_CODE),
        ({"upd_or_quantity": {"$empty": True}}, OPERATOR_TYPE_MISMATCH_CODE),
        ({"upd_or_quantity": {"$superset": [4]}}, OPERATOR_TYPE_MISMATCH_CODE),
        ({"upd_or_quantity.x": {"$eq": 4}}, UNSUPPORTED_QUERY_FEATURE_CODE),
        ({"upd_or_quantity": {"$any": {"$eq": 4}}}, UNSUPPORTED_QUERY_FEATURE_CODE),
    ],
)
async def test_an_operator_an_extra_column_cannot_take_is_refused(
    pg_client: PostgresClient, predicate: dict[str, Any], code: str
) -> None:
    hub = await _hub(pg_client, extra=UPD_EXTRA)

    with pytest.raises(CoreException) as refused:
        await hub.aggregate_search(
            {"$computed": {"n": {"$count": {"filter": {"$values": predicate}}}}}, "omega"
        )

    assert refused.value.code == code
    assert "upd_or_quantity" in str(refused.value)


@pytest.mark.parametrize(
    "aggregates",
    [
        {"$computed": {"q": {"$sum": "upd_or_quantity.x"}}},
        {"$groups": {"g": "upd_or_quantity.x"}, "$computed": {"n": {"$count": None}}},
    ],
)
async def test_a_path_into_an_extra_column_is_refused(
    pg_client: PostgresClient, aggregates: dict[str, Any]
) -> None:
    """Refused before the renderer, which has no model to walk into the column."""

    hub = await _hub(pg_client, extra=UPD_EXTRA)

    with pytest.raises(CoreException) as refused:
        await hub.aggregate_search(aggregates, "omega")

    assert refused.value.code == UNSUPPORTED_QUERY_FEATURE_CODE
    assert "upd_or_quantity" in str(refused.value)


async def test_an_untyped_extra_column_takes_null(pg_client: PostgresClient) -> None:
    hub = await _hub(pg_client, extra=UPD_EXTRA)

    page = await hub.aggregate_search(
        {"$computed": {"bare": {"$count": {"filter": {"$values": {"upd_meta": {"$null": True}}}}}}},
        "omega",
    )

    # Links 0 and 2 carry no metadata.
    assert [row["bare"] for row in page.hits] == [2]


@pytest.mark.parametrize(
    "predicate",
    [
        {"$values": {"upd_meta": {"$eq": 8}}},
        {"$values": {"upd_meta": {"$gt": 5}}},
        {"$values": {"upd_meta": {"$in": [1, 2]}}},
        {"$values": {"upd_meta": {"$empty": True}}},
        {"$fields": {"upd_or_quantity": {"$eq": "upd_meta"}}},
    ],
)
async def test_an_untyped_extra_column_refuses_a_value_operator(
    pg_client: PostgresClient, predicate: dict[str, Any]
) -> None:
    """No rule binds a value to a ``jsonb`` column, so nothing but ``$null`` reaches it."""

    hub = await _hub(pg_client, extra=UPD_EXTRA)

    with pytest.raises(CoreException) as refused:
        await hub.aggregate_search({"$computed": {"n": {"$count": {"filter": predicate}}}}, "omega")

    assert refused.value.code == OPERATOR_TYPE_MISMATCH_CODE
    assert "upd_meta" in str(refused.value)
