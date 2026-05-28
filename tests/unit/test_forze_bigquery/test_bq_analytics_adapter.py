"""Tests for BigQueryAnalyticsAdapter with a mocked client."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from forze.application.contracts.base import CountlessPage, Page
from forze_bigquery.adapters import BigQueryAnalyticsAdapter
from forze_bigquery.execution.deps.configs import (
    BigQueryAnalyticsConfig,
    BigQueryQueryConfig,
)
from forze_bigquery.kernel.platform.value_objects import (
    BigQueryInsertResult,
    BigQueryQueryResult,
)


class _Row(BaseModel):
    value: int


class _Params(BaseModel):
    day: str = "2026-01-01"


class _Ingest(BaseModel):
    event: str


def _adapter(mock: Any) -> BigQueryAnalyticsAdapter[_Row, _Ingest]:
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
        ingest=_Ingest,
    )
    config = BigQueryAnalyticsConfig(
        dataset="analytics",
        queries={
            "counts": BigQueryQueryConfig(
                sql="SELECT value FROM t WHERE day = @day",
            ),
        },
        ingest_table="events_raw",
    )
    return BigQueryAnalyticsAdapter(client=mock, spec=spec, config=config)


class _MockClient:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.inserts: list[list[dict[str, Any]]] = []

    async def run_query(
        self,
        sql: str,
        params: BaseModel | None = None,
        *,
        dry_run: bool = False,
        maximum_bytes_billed: int | None = None,
        max_results: int | None = None,
        start_index: int | None = None,
        page_token: str | None = None,
        timeout: int | None = None,
    ) -> BigQueryQueryResult:
        _ = (
            params,
            dry_run,
            maximum_bytes_billed,
            max_results,
            start_index,
            page_token,
            timeout,
        )
        self.queries.append(sql)
        if "COUNT(*)" in sql:
            return BigQueryQueryResult(rows=[{"forze_cnt": 2}], total_rows=1)
        return BigQueryQueryResult(
            rows=[{"value": 10}, {"value": 20}],
            total_rows=2,
            page_token="next-page",
        )

    async def run_query_all_pages(
        self,
        sql: str,
        params: BaseModel | None = None,
        *,
        maximum_bytes_billed: int | None = None,
        max_rows: int | None = None,
        timeout: int | None = None,
        fetch_batch_size: int = 2000,
    ) -> list[dict[str, Any]]:
        _ = maximum_bytes_billed, max_rows, timeout, fetch_batch_size
        result = await self.run_query(sql, params)
        return result.rows

    async def insert_rows(
        self,
        dataset: str,
        table: str,
        rows: list[dict[str, Any]],
        *,
        insert_id_field: str | None = None,
        timeout: int | None = None,
    ) -> BigQueryInsertResult:
        _ = dataset, table, insert_id_field, timeout
        self.inserts.append(rows)
        return BigQueryInsertResult(accepted=len(rows))


@pytest.mark.asyncio
async def test_run_page_uses_count_wrapper() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)
    page = await adapter.run_page("counts", _Params())
    assert page.count == 2
    assert len(page.hits) == 2
    assert any("COUNT(*)" in q for q in mock.queries)


@pytest.mark.asyncio
async def test_run_cursor_exposes_next_token() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)
    page = await adapter.run_cursor("counts", _Params())
    assert page.has_more is True
    assert page.next_cursor is not None


@pytest.mark.asyncio
async def test_run_page_skip_total_skips_count_query() -> None:
    mock = _MockClient()
    config = BigQueryAnalyticsConfig(
        dataset="analytics",
        queries={
            "counts": BigQueryQueryConfig(
                sql="SELECT value FROM t WHERE day = @day",
                skip_total=True,
            ),
        },
        ingest_table="events_raw",
    )
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
        ingest=_Ingest,
    )
    adapter = BigQueryAnalyticsAdapter(client=mock, spec=spec, config=config)
    page = await adapter.run_page("counts", _Params())
    assert isinstance(page, CountlessPage)
    assert not isinstance(page, Page)
    assert not any("COUNT(*)" in q for q in mock.queries)


@pytest.mark.asyncio
async def test_run_with_dry_run_does_not_execute_data_query() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)
    page = await adapter.run("counts", _Params(), options={"dry_run": True})
    assert page.hits == []
    assert mock.queries == []


@pytest.mark.asyncio
async def test_append_ingest() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)
    result = await adapter.append([_Ingest(event="signup")])
    assert result is not None
    assert result.accepted == 1
    assert mock.inserts[0][0]["event"] == "signup"
