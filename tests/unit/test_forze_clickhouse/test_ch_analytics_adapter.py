"""Tests for ClickHouseAnalyticsAdapter with a mocked client."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from forze_clickhouse.adapters import ClickHouseAnalyticsAdapter
from forze_clickhouse.execution.deps.configs import (
    ClickHouseAnalyticsConfig,
    ClickHouseQueryConfig,
)
from forze_clickhouse.kernel.client.value_objects import (
    ClickHouseInsertResult,
    ClickHouseQueryResult,
)


class _Row(BaseModel):
    value: int


class _Params(BaseModel):
    day: str = "2026-01-01"


class _Ingest(BaseModel):
    event: str


def _adapter(mock: Any) -> ClickHouseAnalyticsAdapter[_Row, _Ingest]:
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
        ingest=_Ingest,
    )
    config = ClickHouseAnalyticsConfig(
        database="analytics",
        queries={
            "counts": ClickHouseQueryConfig(
                sql="SELECT value FROM t WHERE day = {day:String}",
            ),
        },
        ingest_table="events_raw",
    )
    return ClickHouseAnalyticsAdapter(client=mock, spec=spec, config=config)


class _MockClient:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.inserts: list[list[dict[str, Any]]] = []

    async def run_query(
        self,
        sql: str,
        params: BaseModel | None = None,
        *,
        database: str | None = None,
        max_rows: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
        timeout: int | None = None,
    ) -> ClickHouseQueryResult:
        _ = params, database, max_rows, limit, offset, timeout
        self.queries.append(sql)
        if "count()" in sql.lower():
            return ClickHouseQueryResult(rows=[{"forze_cnt": 2}], row_count=1)
        return ClickHouseQueryResult(
            rows=[{"value": 10}, {"value": 20}],
            row_count=2,
        )

    async def run_query_all_pages(
        self,
        sql: str,
        params: BaseModel | None = None,
        *,
        database: str | None = None,
        max_rows: int | None = None,
        timeout: int | None = None,
        fetch_batch_size: int = 2000,
    ) -> list[dict[str, Any]]:
        _ = database, max_rows, timeout, fetch_batch_size
        result = await self.run_query(sql, params)
        return result.rows

    async def insert_rows(
        self,
        database: str,
        table: str,
        rows: list[dict[str, Any]],
        *,
        timeout: int | None = None,
    ) -> ClickHouseInsertResult:
        _ = database, table, timeout
        self.inserts.append(rows)
        return ClickHouseInsertResult(accepted=len(rows))


@pytest.mark.asyncio
async def test_run_page_uses_count_wrapper() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)
    page = await adapter.run_page("counts", _Params())
    assert page.count == 2
    assert len(page.hits) == 2
    assert any("count()" in q.lower() for q in mock.queries)


@pytest.mark.asyncio
async def test_run_cursor_exposes_next_token() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)
    page = await adapter.run_cursor("counts", _Params(), cursor={"limit": 1})
    assert page.has_more is True
    assert page.next_cursor is not None


@pytest.mark.asyncio
async def test_append_ingest() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)
    result = await adapter.append([_Ingest(event="signup")])
    assert result is not None
    assert result.accepted == 1
    assert mock.inserts[0][0]["event"] == "signup"
