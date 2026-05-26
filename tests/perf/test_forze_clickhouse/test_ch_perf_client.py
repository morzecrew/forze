"""Lightweight ClickHouse client perf smoke tests."""

from __future__ import annotations

import time

import pytest
from pydantic import BaseModel

pytestmark = pytest.mark.perf


class _Params(BaseModel):
    pass


@pytest.mark.asyncio
async def test_run_query_latency_smoke(clickhouse_client) -> None:
    start = time.perf_counter()
    result = await clickhouse_client.run_query("SELECT 1 AS value", _Params())
    elapsed = time.perf_counter() - start

    assert result.row_count == 1
    assert elapsed < 5.0


@pytest.mark.asyncio
async def test_health_smoke(clickhouse_client) -> None:
    message, ok = await clickhouse_client.health()
    assert ok is True
    assert message == "ok"
