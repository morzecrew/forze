"""Tests for Postgres analytics skip_total configuration."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from forze.application.contracts.base import CountlessPage, Page
from forze_postgres.adapters.analytics import PostgresAnalyticsAdapter
from forze_postgres.execution.deps.configs import PostgresAnalyticsConfig, PostgresQueryConfig


class _Row(BaseModel):
    value: int


class _Params(BaseModel):
    day: str = "2026-01-01"


class _MockClient:
    def __init__(self) -> None:
        self.queries: list[str] = []

    async def fetch_all(
        self,
        query: str,
        params: dict[str, object] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, int]]:
        _ = params, kwargs
        self.queries.append(query)
        if "COUNT(*)" in query:
            return [{"forze_cnt": 1}]
        return [{"value": 1}]


@pytest.mark.asyncio
async def test_run_page_skip_total_skips_count_query() -> None:
    mock = _MockClient()
    config = PostgresAnalyticsConfig(
        queries={
            "counts": PostgresQueryConfig(
                sql="SELECT value FROM t",
                skip_total=True,
            ),
        },
    )
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
    )
    adapter = PostgresAnalyticsAdapter(client=mock, spec=spec, config=config)
    page = await adapter.run_page("counts", _Params())
    assert isinstance(page, CountlessPage)
    assert not isinstance(page, Page)
    assert not any("COUNT(*)" in q for q in mock.queries)
