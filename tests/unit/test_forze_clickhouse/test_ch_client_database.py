"""Tests for per-request ClickHouse database selection without mutating the shared client."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from pydantic import BaseModel

from forze_clickhouse.kernel.platform.client import ClickHouseClient
from forze_clickhouse.kernel.platform.value_objects import (
    ClickHouseConfig,
    ClickHouseQueryResult,
)


class _Params(BaseModel):
    pass


class _RecordingAsyncClient:
    """Records query settings per call (no shared database mutation)."""

    database = "default"

    def __init__(self) -> None:
        self.query_calls: list[dict[str, Any]] = []

    async def query(
        self,
        query: str,
        *,
        parameters: Any = None,
        settings: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        _ = query, parameters, kwargs
        self.query_calls.append(dict(settings or {}))

        class _Result:
            def named_results(self) -> list[dict[str, Any]]:
                return [{"value": 1}]

        return _Result()

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_run_query_uses_settings_database_not_mutation() -> None:
    client = ClickHouseClient()
    recording = _RecordingAsyncClient()
    client._ClickHouseClient__client = recording  # type: ignore[attr-defined]
    client._ClickHouseClient__config = ClickHouseConfig(  # type: ignore[attr-defined]
        database="default"
    )

    await client.run_query("SELECT 1", _Params(), database="analytics")
    await client.run_query("SELECT 1", _Params(), database="other")

    assert recording.database == "default"
    assert recording.query_calls[0]["database"] == "analytics"
    assert recording.query_calls[1]["database"] == "other"


@pytest.mark.asyncio
async def test_concurrent_run_query_database_isolation() -> None:
    client = ClickHouseClient()
    recording = _RecordingAsyncClient()
    client._ClickHouseClient__client = recording  # type: ignore[attr-defined]
    client._ClickHouseClient__config = ClickHouseConfig(  # type: ignore[attr-defined]
        database="default"
    )

    async def _query(db: str) -> ClickHouseQueryResult:
        return await client.run_query("SELECT 1", _Params(), database=db)

    await asyncio.gather(_query("db_a"), _query("db_b"))

    databases = [call["database"] for call in recording.query_calls]
    assert "db_a" in databases
    assert "db_b" in databases
