"""Additional FTS adapter integration paths (CountlessPage, empty-query browse)."""

from __future__ import annotations

from uuid import uuid4

import pytest
from dirty_equals import IsStr

from forze.application.contracts.base import CountlessPage, Page
from forze.application.contracts.querying import QueryFilterExpression
from forze_postgres.adapters.search import PostgresFTSSearchAdapter
from forze_postgres.kernel.client.client import PostgresClient
from tests.integration.test_forze_postgres._search_fixtures import (
    PgSearchRow,
    bootstrap_fts_search_table,
    fts_search_context,
    fts_spec,
)
from tests.support import IsPartialDict, IsUUID


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fts_select_search_returns_countless_page(
    pg_client: PostgresClient,
) -> None:
    table, index_name, _rows = await bootstrap_fts_search_table(pg_client)
    ctx = fts_search_context(pg_client, table=table, index_name=index_name)
    spec = fts_spec(name=f"fts_cnt_{uuid4().hex[:8]}")
    adapter = ctx.search.query(spec)
    assert isinstance(adapter, PostgresFTSSearchAdapter)

    page = await adapter.select_search(PgSearchRow, "search")
    assert isinstance(page, CountlessPage)
    assert len(page.hits) >= 1
    assert page.hits[0].model_dump() == IsPartialDict(
        {"id": IsUUID, "title": IsStr},
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fts_empty_query_with_filter_and_sorts(
    pg_client: PostgresClient,
) -> None:
    table, index_name, rows = await bootstrap_fts_search_table(pg_client)
    ctx = fts_search_context(pg_client, table=table, index_name=index_name)
    adapter = ctx.search.query(fts_spec(name=f"fts_browse_{uuid4().hex[:8]}"))

    filt: QueryFilterExpression = {
        "$values": {"title": rows[0]["title"]},
    }
    page = await adapter.search_page(
        "",
        filters=filt,
        pagination={"limit": 5, "offset": 0},
        sorts={"title": "asc"},
    )
    assert isinstance(page, Page)
    assert page.count == 1
    assert page.hits[0].title == rows[0]["title"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fts_phrase_combine_on_multi_term_query(
    pg_client: PostgresClient,
) -> None:
    table, index_name, _rows = await bootstrap_fts_search_table(pg_client)
    ctx = fts_search_context(pg_client, table=table, index_name=index_name)
    adapter = ctx.search.query(fts_spec(name=f"fts_phrase_{uuid4().hex[:8]}"))

    any_hits = await adapter.search_page(
        ["text", "search"],
        options={"phrase_combine": "any"},
    )
    all_hits = await adapter.search_page(
        ["text", "search"],
        options={"phrase_combine": "all"},
    )
    assert any_hits.count >= all_hits.count
