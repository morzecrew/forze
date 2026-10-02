"""Both PGroonga plans answer a capped page, whatever relation reads it and whatever it sorts by.

Index-first caps the heap before it meets the read projection, so its cap can only order by
what the heap carries; a sort on a projection-only column runs filter-first instead. A capped
offset page must hold the rows the uncapped cursor page holds, on the base table, on a view
over it and on a second table alike.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps
from forze_postgres.execution.deps import ConfigurablePostgresSearch
from forze_postgres.execution.deps.configs import PgroongaEngine, PostgresSearchConfig
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _Row(BaseModel):
    id: UUID
    title: str
    twice: int


_SORTS = {
    "none": None,
    "id": {"id": "desc"},
    "heap column": {"title": "asc"},
    "projection column": {"twice": "desc"},
}


async def _port(
    client: PostgresClient,
    *,
    read: str,
    plan: str,
    index_field_map: dict[str, str] | None = None,
    model: type[BaseModel] = _Row,
) -> Any:
    heap = f"plans_{uuid4().hex[:8]}"
    await client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga")
    await client.execute(
        f"CREATE TABLE {heap} (id uuid PRIMARY KEY, title text NOT NULL, twice int NOT NULL)"
    )
    await client.execute(
        f"INSERT INTO {heap} SELECT gen_random_uuid(), 'python ' || g, g * 2 "
        "FROM generate_series(1, 80) g"
    )
    await client.execute(f"CREATE INDEX {heap}_pgr ON {heap} USING pgroonga ((ARRAY[title]))")
    projection = heap

    # Apart from the heap, ``twice`` is the projection's own: computed, or copied apart.
    if read == "view":
        projection = f"{heap}_v"
        await client.execute(f"CREATE VIEW {projection} AS SELECT id, title, twice FROM {heap}")

    elif read == "second table":
        projection = f"{heap}_t"
        await client.execute(f"CREATE TABLE {projection} AS SELECT * FROM {heap}")

    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", f"{heap}_pgr"),
                        read=("public", projection),
                        heap=("public", heap),
                        engine=PgroongaEngine(plan=plan),  # type: ignore[arg-type]
                        candidate_limit=1,
                        field_map=index_field_map,
                    )
                ),
            }
        )
    )

    return ctx.search.query(SearchSpec(name="rows", model_type=model, fields=["title"]))


@pytest.mark.parametrize("read", ["base table", "view", "second table"])
@pytest.mark.parametrize("plan", ["filter_first", "index_first"])
@pytest.mark.parametrize("sorts", list(_SORTS.values()), ids=list(_SORTS))
async def test_a_capped_page_is_the_cursors(
    pg_client: PostgresClient, read: str, plan: str, sorts: Any
) -> None:
    port = await _port(pg_client, read=read, plan=plan)

    offset = await port.search("python", None, {"limit": 5}, sorts)
    cursor = await port.search_cursor("python", None, {"limit": 5}, sorts)

    assert len(offset.hits) == 5
    assert [hit.id for hit in offset.hits] == [hit.id for hit in cursor.hits]


async def test_a_mapped_index_field_sort_keeps_index_first_on_a_view(
    pg_client: PostgresClient,
) -> None:
    # The heap carries a field the index maps, so index-first orders its cap by the heap's own
    # column, in the page's null placement, instead of giving way to filter-first.
    port = await _port(
        pg_client, read="view", plan="index_first", index_field_map={"title": "title"}
    )

    offset = await port.search("python", None, {"limit": 5}, {"title": "asc"})
    cursor = await port.search_cursor("python", None, {"limit": 5}, {"title": "asc"})

    assert len(offset.hits) == 5
    assert [hit.id for hit in offset.hits] == [hit.id for hit in cursor.hits]


class _NoId(BaseModel):
    title: str


async def test_a_capped_page_of_a_model_without_an_id(pg_client: PostgresClient) -> None:
    # Nothing to order by after the rank but the join keys, which close the cap's order.
    port = await _port(pg_client, read="base table", plan="filter_first", model=_NoId)

    page = await port.search("python", None, {"limit": 5})

    assert len(page.hits) == 5
