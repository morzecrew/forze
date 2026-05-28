"""Tests for PostgresAnalyticsAdapter with a mocked client."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from forze.base.exceptions import CoreException
from forze_postgres.adapters.analytics import PostgresAnalyticsAdapter
from forze_postgres.execution.deps.configs import PostgresAnalyticsConfig, PostgresQueryConfig


class _Row(BaseModel):
    value: int


class _Params(BaseModel):
    day: str = "2026-01-01"


class _Ingest(BaseModel):
    event: str


def _adapter(mock: Any) -> PostgresAnalyticsAdapter[_Row, _Ingest]:
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
        ingest=_Ingest,
    )
    config = PostgresAnalyticsConfig(
        schema="public",
        queries={
            "counts": PostgresQueryConfig(
                sql="SELECT value FROM t WHERE day = %(day)s",
            ),
        },
        ingest_table="events_raw",
    )
    return PostgresAnalyticsAdapter(client=mock, spec=spec, config=config)


class _MockClient:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.executes: list[tuple[Any, Any]] = []
        self._in_tx = False

    def is_in_transaction(self) -> bool:
        return self._in_tx

    def transaction(self) -> Any:
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _tx() -> Any:
            self._in_tx = True
            try:
                yield self
            finally:
                self._in_tx = False

        return _tx()

    async def fetch_all(
        self,
        query: str,
        params: dict[str, object] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        _ = kwargs
        self.queries.append(query)
        if "COUNT(*)" in query:
            return [{"forze_cnt": 2}]
        return [{"value": 10}, {"value": 20}]

    async def execute(
        self,
        query: Any,
        params: Any = None,
        **kwargs: Any,
    ) -> None:
        _ = kwargs
        self.executes.append((query, params))


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
    assert len(mock.executes) == 1


@pytest.mark.asyncio
async def test_unknown_query_key_raises() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)
    with pytest.raises(CoreException, match="Unknown analytics query key"):
        await adapter.run("missing", _Params())
