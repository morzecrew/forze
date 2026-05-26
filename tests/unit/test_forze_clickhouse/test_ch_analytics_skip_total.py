"""Tests for ClickHouse analytics skip_total configuration."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from forze.application.contracts.base import CountlessPage, Page
from forze_clickhouse.adapters import ClickHouseAnalyticsAdapter
from forze_clickhouse.execution.deps.configs import ClickHouseAnalyticsConfig
from forze_clickhouse.kernel.platform.value_objects import (
    ClickHouseInsertResult,
    ClickHouseQueryResult,
)


class _Row(BaseModel):
    value: int


class _Params(BaseModel):
    day: str = "2026-01-01"


class _MockClient:
    def __init__(self) -> None:
        self.queries: list[str] = []

    async def run_query(self, sql: str, *args: Any, **kwargs: Any) -> ClickHouseQueryResult:
        _ = args, kwargs
        self.queries.append(sql)
        if "count()" in sql.lower():
            return ClickHouseQueryResult(rows=[{"forze_cnt": 1}], row_count=1)
        return ClickHouseQueryResult(rows=[{"value": 1}], row_count=1)

    async def insert_rows(self, *args: Any, **kwargs: Any) -> ClickHouseInsertResult:
        _ = args, kwargs
        return ClickHouseInsertResult(accepted=0)


@pytest.mark.asyncio
async def test_run_page_skip_total_skips_count_query() -> None:
    mock = _MockClient()
    config: ClickHouseAnalyticsConfig = {
        "database": "analytics",
        "queries": {
            "counts": {
                "sql": "SELECT value FROM t WHERE day = {day:String}",
                "skip_total": True,
            },
        },
    }
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
    )
    adapter = ClickHouseAnalyticsAdapter(client=mock, spec=spec, config=config)
    page = await adapter.run_page("counts", _Params())
    assert isinstance(page, CountlessPage)
    assert not isinstance(page, Page)
    assert not any("count()" in q.lower() for q in mock.queries)
