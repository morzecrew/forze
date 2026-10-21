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

    Link ``k`` is in the north when ``k`` is even, holds ``k + 1`` units at 10.5 each.
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
            price numeric NOT NULL
        );
        CREATE VIEW {links}_v AS
            SELECT l.*, l.quantity * l.price AS price_upd_quantity FROM {links} l;
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
            f"INSERT INTO {links} (id, item_id, spec_id, region, quantity, price) "
            "VALUES (%(id)s, %(item)s, %(spec)s, %(r)s, %(q)s, 10.5)",
            {
                "id": uuid4(),
                "item": item,
                "spec": spec,
                "r": "north" if k % 2 == 0 else "south",
                "q": k + 1,
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
    hub = await _hub(pg_client, extra=frozenset({"quantity", "upd_or_quantity"}))

    with pytest.raises(CoreException) as refused:
        await hub.aggregate_search({"$computed": {"q": {"$sum": "quantity"}}}, "omega")

    assert refused.value.code == "postgres_schema_validation_failed"
    assert "upd_or_quantity" in str(refused.value)


async def test_an_extra_field_named_like_a_pipeline_column_is_refused(
    pg_client: PostgresClient,
) -> None:
    hub = await _hub(pg_client, extra=frozenset({"_hub_rank"}))

    with pytest.raises(CoreException) as refused:
        await hub.aggregate_search({"$computed": {"n": {"$count": None}}}, "omega")

    assert refused.value.kind is ExceptionKind.CONFIGURATION
    assert "_hub_rank" in str(refused.value)
