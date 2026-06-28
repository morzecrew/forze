"""Unit tests for ``ClickHouseClient.insert_rows`` row->column transposition.

The transpose uses ``operator.itemgetter`` for speed; the single-column case is a
distinct branch (``itemgetter`` returns a bare value, not a tuple) and must still
produce a nested ``list[list]`` for the driver.
"""

from __future__ import annotations

from typing import Any

import pytest

from forze_clickhouse.kernel.client.client import ClickHouseClient
from forze_clickhouse.kernel.client.value_objects import ClickHouseConfig

# ----------------------- #


class _RecordingClient:
    """Stub driver recording ``insert`` calls."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def insert(
        self,
        table: str,
        data: list[list[Any]],
        *,
        column_names: list[str],
        database: str,
        settings: dict[str, Any] | None = None,
    ) -> None:
        self.calls.append(
            {
                "table": table,
                "data": data,
                "column_names": column_names,
                "database": database,
            }
        )

    async def close(self) -> None:
        return None


def _client(driver: _RecordingClient) -> ClickHouseClient:
    client = ClickHouseClient()
    client._ClickHouseClient__client = driver  # type: ignore[attr-defined]
    client._ClickHouseClient__config = ClickHouseConfig()  # type: ignore[attr-defined]
    return client


# ....................... #


@pytest.mark.asyncio
async def test_single_column_insert_produces_nested_lists() -> None:
    driver = _RecordingClient()
    client = _client(driver)
    rows = [{"value": 1}, {"value": 2}, {"value": 3}]

    result = await client.insert_rows("analytics", "raw", rows)

    assert result.accepted == 3
    call = driver.calls[0]
    assert call["column_names"] == ["value"]
    # Single-column itemgetter returns a scalar; each row must still be wrapped.
    assert call["data"] == [[1], [2], [3]]


@pytest.mark.asyncio
async def test_multi_column_insert_transposes_in_column_order() -> None:
    driver = _RecordingClient()
    client = _client(driver)
    rows = [
        {"a": 1, "b": "x", "c": True},
        {"a": 2, "b": "y", "c": False},
    ]

    await client.insert_rows("analytics", "raw", rows)

    call = driver.calls[0]
    assert call["column_names"] == ["a", "b", "c"]
    assert call["data"] == [[1, "x", True], [2, "y", False]]


@pytest.mark.asyncio
async def test_empty_rows_is_a_noop() -> None:
    driver = _RecordingClient()
    client = _client(driver)

    result = await client.insert_rows("analytics", "raw", [])

    assert result.accepted == 0
    assert driver.calls == []
