"""Postgres ``search_count="approximate"``: hub totals with search text, and filtered ones.

The hub's approximate total for a text query counts the merged rows the page can reach, so
it includes the text predicate: zero for no match, the exact total below the cap, the cap
above it. The planner can't stand in for that count — it estimates a PGroonga predicate at a
fixed selectivity, whatever the query. A blank query keeps the planner's estimate of its
filters, which must name the relation by the alias the filters use.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import HubSearchSpec, SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps
from forze_postgres.execution.deps import (
    ConfigurablePostgresHubSearch,
    ConfigurablePostgresSearch,
)
from forze_postgres.execution.deps.configs import (
    FtsEngine,
    PgroongaEngine,
    PostgresHubSearchConfig,
    PostgresHubSearchMemberConfig,
    PostgresSearchConfig,
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

ROWS = 150
CAP = 60
"""``per_leg_limit``: with a five-row page it is also the merged-rows cap."""

PAGE = {"limit": 5}
APPROXIMATE: Any = {"search_count": "approximate"}
NORTH: Any = {"$values": {"region": {"$eq": "north"}}}


class _Leg(BaseModel):
    name: str = ""


class _Link(BaseModel):
    id: UUID
    item_id: UUID
    spec_id: UUID
    region: str


async def _hub(pg_client: PostgresClient, execution: str) -> Any:
    """Two legs over 150 links: ``alpha`` names 120 items, ``omega`` five items and ten specs."""

    tag = uuid4().hex[:8]
    items, specs, links = f"hc_items_{tag}", f"hc_specs_{tag}", f"hc_links_{tag}"

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    await pg_client.execute(
        f"""
        CREATE TABLE {items} (id uuid PRIMARY KEY, name text NOT NULL);
        CREATE TABLE {specs} (id uuid PRIMARY KEY, name text NOT NULL);
        CREATE TABLE {links} (
            id uuid PRIMARY KEY,
            item_id uuid NOT NULL REFERENCES {items} (id),
            spec_id uuid NOT NULL REFERENCES {specs} (id),
            region text NOT NULL
        );
        CREATE INDEX idx_{items} ON {items} USING pgroonga ((ARRAY[name]));
        CREATE INDEX idx_{specs} ON {specs} USING pgroonga ((ARRAY[name]));
        """
    )

    for i in range(ROWS):
        item, spec = uuid4(), uuid4()
        item_name = ("alpha" if i < 120 else "beta") + (" omega" if i < 5 else "")
        spec_name = "gamma" + (" omega" if i >= 140 else "")
        await pg_client.execute(
            f"INSERT INTO {items} (id, name) VALUES (%(id)s, %(n)s)", {"id": item, "n": item_name}
        )
        await pg_client.execute(
            f"INSERT INTO {specs} (id, name) VALUES (%(id)s, %(n)s)", {"id": spec, "n": spec_name}
        )
        await pg_client.execute(
            f"INSERT INTO {links} (id, item_id, spec_id, region) "
            "VALUES (%(id)s, %(item)s, %(spec)s, %(r)s)",
            {"id": uuid4(), "item": item, "spec": spec, "r": "north" if i % 2 == 0 else "south"},
        )

    # Real statistics on the hub relation, so its own row estimate is the 150 it holds.
    await pg_client.execute(f"ANALYZE {links};")

    def member(heap: str, fk: str) -> PostgresHubSearchMemberConfig:
        return PostgresHubSearchMemberConfig(
            index=("public", f"idx_{heap}"), read=("public", heap), hub_fk=fk, engine="pgroonga"
        )

    config = PostgresHubSearchConfig(
        hub=("public", links),
        members={"items": member(items, "item_id"), "specs": member(specs, "spec_id")},
        per_leg_limit=CAP,
        execution=execution,  # type: ignore[arg-type]
    )
    spec = HubSearchSpec(
        name="hub_count",
        model_type=_Link,
        members=(
            SearchSpec(name="items", model_type=_Leg, fields=["name"]),
            SearchSpec(name="specs", model_type=_Leg, fields=["name"]),
        ),
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


# ....................... #


@pytest.mark.parametrize("execution", ["sql", "parallel"])
async def test_a_query_matching_nothing_counts_zero(
    pg_client: PostgresClient, execution: str
) -> None:
    hub = await _hub(pg_client, execution)

    page = await hub.search_page("zzzнетнет", None, PAGE, options=APPROXIMATE)

    assert (page.count, page.hits) == (0, [])


@pytest.mark.parametrize("execution", ["sql", "parallel"])
async def test_a_match_counts_exactly_up_to_the_cap(
    pg_client: PostgresClient, execution: str
) -> None:
    """Under the cap the approximate total is the exact one; over it, the cap. A first page
    and a later one that leaves the cap where it is count alike (the first folds its count
    into the page's own scan)."""

    hub = await _hub(pg_client, execution)
    cases = [("omega", None, 15, 15), ("omega", NORTH, 8, 8), ("alpha", None, 120, CAP)]

    for query, filters, exact, approximate in cases:
        counted = await hub.search_page(query, filters, PAGE, options={"search_count": "exact"})

        for page in (PAGE, {"limit": 5, "offset": 5}):
            estimated = await hub.search_page(query, filters, page, options=APPROXIMATE)

            assert (counted.count, estimated.count) == (exact, approximate), (query, filters, page)


@pytest.mark.parametrize("execution", ["sql", "parallel"])
async def test_a_page_of_no_rows_still_counts(pg_client: PostgresClient, execution: str) -> None:
    """A ``limit: 0`` page reads no rows, so its total can't ride them: approximate with
    search text, or exact for a blank query."""

    hub = await _hub(pg_client, execution)
    empty = {"limit": 0}

    approximate = await hub.search_page("omega", None, empty, options=APPROXIMATE)
    blank = await hub.search_page("", None, empty, options={"search_count": "exact"})

    assert (approximate.count, blank.count) == (15, ROWS)


async def test_a_filtered_blank_hub_query_is_estimated(pg_client: PostgresClient) -> None:
    hub = await _hub(pg_client, "sql")

    page = await hub.search_page("", NORTH, PAGE, options=APPROXIMATE)

    assert page.count == ROWS // 2


# ....................... #


class _Doc(BaseModel):
    id: UUID
    name: str
    region: str


@pytest.mark.parametrize(
    ("engine", "query"), [("fts", ""), ("pgroonga", ""), ("pgroonga_auto", "alpha")]
)
async def test_a_filtered_single_index_query_is_estimated(
    pg_client: PostgresClient, engine: str, query: str
) -> None:
    """A blank browse estimates its filters; so does PGroonga's ``auto`` plan, to pick one."""

    table = f"ac_docs_{uuid4().hex[:8]}"
    index = (
        "USING gin (to_tsvector('english', name))"
        if engine == "fts"
        else "USING pgroonga ((ARRAY[name]))"
    )

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (id uuid PRIMARY KEY, name text NOT NULL, region text NOT NULL);
        CREATE INDEX idx_{table} ON {table} {index};
        """
    )

    for i in range(ROWS):
        await pg_client.execute(
            f"INSERT INTO {table} (id, name, region) VALUES (%(id)s, 'alpha', %(r)s)",
            {"id": uuid4(), "r": "north" if i % 2 == 0 else "south"},
        )

    config = PostgresSearchConfig(
        index=("public", f"idx_{table}"),
        read=("public", table),
        engine={
            "fts": FtsEngine(groups={"A": ("name",)}),
            "pgroonga": PgroongaEngine(),
            "pgroonga_auto": PgroongaEngine(plan="auto"),
        }[engine],
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(config=config),
            }
        )
    )
    port = ctx.search.query(SearchSpec(name="docs", model_type=_Doc, fields=["name"]))

    page = await port.search_page(query, NORTH, PAGE, options=APPROXIMATE)

    assert len(page.hits) == 5
    assert page.count is not None and page.count > 0
