"""Postgres hub search facets + highlights (sql execution) and parallel fail-closed."""

from __future__ import annotations

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

# ----------------------- #


class _HubLeg(BaseModel):
    name: str = ""


class _HubRow(BaseModel):
    id: UUID
    name: str
    category: str


def _hub_spec() -> HubSearchSpec[_HubRow]:
    leg = SearchSpec(name="leg", model_type=_HubLeg, fields=["name"])
    return HubSearchSpec(
        name="hub_fh",
        model_type=_HubRow,
        members=(leg,),
        facetable_fields=frozenset({"category"}),
    )


async def _adapter(pg_client: PostgresClient, *, execution: str = "sql"):
    tag = uuid4().hex[:8]
    table, index = f"hub_fh_{tag}", f"idx_hub_fh_{tag}"

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            category text NOT NULL
        );
        CREATE INDEX {index} ON {table} USING pgroonga ((ARRAY[name]));
        """
    )
    for name, category in (
        ("Rust Book", "books"),
        ("Python Book", "books"),
        ("Gaming Book", "gear"),
    ):
        await pg_client.execute(
            f"INSERT INTO {table} (id, name, category) "
            "VALUES (%(id)s, %(n)s, %(c)s)",
            {"id": uuid4(), "n": name, "c": category},
        )

    cfg = PostgresHubSearchConfig(
        hub=("public", table),
        members={
            "leg": PostgresHubSearchMemberConfig(
                index=("public", index),
                read=("public", table),
                hub_fk="id",
                same_heap_as_hub=True,
                engine="pgroonga",
            ),
        },
        execution=execution,  # type: ignore[arg-type]
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    return ConfigurablePostgresHubSearch(config=cfg)(ctx, _hub_spec())


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_hub_offset_facets(pg_client: PostgresClient) -> None:
    adapter = await _adapter(pg_client)

    page = await adapter.search_page("book", options={"facets": ["category"]})

    assert page.facets is not None
    cat = {b.value: b.count for b in page.facets["category"]}
    assert cat == {"books": 2, "gear": 1}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_hub_offset_highlights(pg_client: PostgresClient) -> None:
    adapter = await _adapter(pg_client)

    page = await adapter.search_page(
        "book", options={"highlight": {"fields": ["name"]}}
    )

    assert page.highlights is not None
    assert len(page.highlights) == len(page.hits)
    frags = [hl["name"][0] for hl in page.highlights if "name" in hl]
    assert frags and all("<em>" in f.lower() and "book" in f.lower() for f in frags)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_hub_cursor_facets_and_highlights(pg_client: PostgresClient) -> None:
    adapter = await _adapter(pg_client)

    page = await adapter.search_cursor(
        "book",
        cursor={"limit": 5},
        options={"facets": ["category"], "highlight": {"fields": ["name"]}},
    )

    assert page.facets is not None
    assert {b.value for b in page.facets["category"]} == {"books", "gear"}
    assert page.highlights is not None
    frags = [hl["name"][0] for hl in page.highlights if "name" in hl]
    assert frags and all("<em>" in f.lower() for f in frags)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_hub_projected_search_fails_closed_on_unprojected_highlight_field(
    pg_client: PostgresClient,
) -> None:
    adapter = await _adapter(pg_client)

    # Project only 'category'; the default highlight resolves to the searchable 'name', which
    # was projected away — fail closed rather than return silently partial highlights.
    with pytest.raises(CoreException) as ei:
        await adapter.project_search(
            ["category"], "book", options={"highlight": True}
        )
    assert ei.value.kind is ExceptionKind.PRECONDITION
    assert ei.value.code == "query_feature_unsupported"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_hub_parallel_highlights_work_but_facets_fail_closed(
    pg_client: PostgresClient,
) -> None:
    adapter = await _adapter(pg_client, execution="parallel")

    # Highlights still apply in parallel execution (marked on the returned hits).
    page = await adapter.search_page(
        "book", options={"highlight": {"fields": ["name"]}}
    )
    assert page.highlights is not None
    assert any("name" in hl for hl in page.highlights)

    # Facets fail closed in parallel execution.
    with pytest.raises(CoreException) as ei:
        await adapter.search_page("book", options={"facets": ["category"]})
    assert ei.value.kind is ExceptionKind.PRECONDITION
    assert ei.value.code == "query_feature_unsupported"
