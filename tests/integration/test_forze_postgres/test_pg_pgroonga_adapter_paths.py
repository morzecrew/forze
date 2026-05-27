"""Additional PGroonga adapter integration paths (CountlessPage, phrase_combine, filters)."""

from __future__ import annotations

from uuid import uuid4

import pytest
from dirty_equals import IsStr

from forze.application.contracts.base import CountlessPage, Page
from forze.application.contracts.querying import QueryFilterExpression
from forze_postgres.adapters.search import PostgresPGroongaSearchAdapter
from forze_postgres.kernel.platform.client import PostgresClient
from tests.support import IsPartialDict, IsUUID

from tests.integration.test_forze_postgres._search_fixtures import (
    PgSearchRow,
    bootstrap_pgroonga_search_table,
    pgroonga_search_context,
    pgroonga_spec,
)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pgroonga_select_search_returns_countless_page(
    pg_client: PostgresClient,
) -> None:
    table, index_name, _rows = await bootstrap_pgroonga_search_table(pg_client)
    ctx = pgroonga_search_context(pg_client, table=table, index_name=index_name)
    adapter = ctx.search.query(pgroonga_spec(name=f"cntless_{uuid4().hex[:8]}"))
    assert isinstance(adapter, PostgresPGroongaSearchAdapter)

    page = await adapter.select_search(PgSearchRow, "python")
    assert isinstance(page, CountlessPage)
    assert len(page.hits) >= 2
    assert page.hits[0].model_dump() == IsPartialDict(
        {"id": IsUUID, "title": IsStr},
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pgroonga_phrase_combine_any_vs_all(pg_client: PostgresClient) -> None:
    table, index_name, _rows = await bootstrap_pgroonga_search_table(pg_client)
    ctx = pgroonga_search_context(pg_client, table=table, index_name=index_name)
    adapter = ctx.search.query(pgroonga_spec(name=f"phrase_{uuid4().hex[:8]}"))

    any_page = await adapter.search_page(
        ["python", "framework"],
        options={"phrase_combine": "any"},
    )
    all_page = await adapter.search_page(
        ["python", "framework"],
        options={"phrase_combine": "all"},
    )
    assert isinstance(any_page, Page)
    assert any_page.count >= all_page.count
    assert all_page.count == 1
    assert all_page.hits[0].title == "Forze Framework"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pgroonga_filter_only_empty_query_offset_browse(
    pg_client: PostgresClient,
) -> None:
    table, index_name, rows = await bootstrap_pgroonga_search_table(pg_client)
    ctx = pgroonga_search_context(pg_client, table=table, index_name=index_name)
    adapter = ctx.search.query(pgroonga_spec(name=f"browse_{uuid4().hex[:8]}"))

    filt: QueryFilterExpression = {
        "$values": {"title": rows[0]["title"]},
    }
    page = await adapter.search_page(
        "",
        filters=filt,
        pagination={"limit": 10, "offset": 0},
        sorts={"title": "asc"},
    )
    assert page.count == 1
    assert page.hits[0].title == rows[0]["title"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pgroonga_project_search_page_partial_fields(
    pg_client: PostgresClient,
) -> None:
    table, index_name, _rows = await bootstrap_pgroonga_search_table(pg_client)
    ctx = pgroonga_search_context(pg_client, table=table, index_name=index_name)
    adapter = ctx.search.query(pgroonga_spec(name=f"proj_{uuid4().hex[:8]}"))

    page = await adapter.project_search_page(["title"], "python")
    assert page.count >= 2
    for hit in page.hits:
        assert hit == IsPartialDict({"title": IsStr})
