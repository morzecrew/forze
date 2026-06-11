"""Unit tests for the streaming ``run_query_all_pages`` (stubbed driver).

Verifies the single-execution streaming contract: the driver is invoked
exactly once per attempt, blocks are accumulated in order, ``max_rows``
is enforced, and the read retry is no longer nested (executions on
persistent transient failure are bounded by ``attempts + 1``, not
``(attempts + 1)**2``).
"""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from forze.base.exceptions import CoreException
from forze_clickhouse.kernel.client.client import ClickHouseClient
from forze_clickhouse.kernel.client.value_objects import ClickHouseConfig

# ----------------------- #

_COLUMNS = ("event", "value")

# ....................... #


class _FakeStream:
    """Minimal stand-in for clickhouse-connect's ``StreamContext``."""

    def __init__(self, blocks: list[list[tuple[Any, ...]]]) -> None:
        self.source = SimpleNamespace(column_names=_COLUMNS)
        self._blocks = iter(blocks)
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> "_FakeStream":
        self.entered = True
        return self

    async def __aexit__(self, *args: Any) -> bool:
        self.exited = True
        return False

    def __aiter__(self) -> "_FakeStream":
        return self

    async def __anext__(self) -> list[tuple[Any, ...]]:
        try:
            return next(self._blocks)

        except StopIteration:
            raise StopAsyncIteration from None


# ....................... #


class _StreamingAsyncClient:
    """Stub driver client recording ``query_row_block_stream`` calls."""

    def __init__(
        self,
        rows: list[tuple[Any, ...]],
        *,
        fail_first: int = 0,
        fail_always: bool = False,
    ) -> None:
        self._rows = rows
        self._fail_first = fail_first
        self._fail_always = fail_always
        self.calls: list[dict[str, Any]] = []
        self.streams: list[_FakeStream] = []

    async def query_row_block_stream(
        self,
        query: str,
        *,
        parameters: Any = None,
        settings: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> _FakeStream:
        _ = parameters, kwargs
        self.calls.append({"query": query, "settings": dict(settings or {})})

        if self._fail_always or len(self.calls) <= self._fail_first:
            raise ConnectionError("transient")

        block_size = int((settings or {}).get("max_block_size", 2))
        blocks = [
            self._rows[i : i + block_size]
            for i in range(0, len(self._rows), block_size)
        ]
        stream = _FakeStream(blocks)
        self.streams.append(stream)

        return stream

    async def close(self) -> None:
        return None


# ....................... #


def _client(
    driver: _StreamingAsyncClient,
    *,
    retry_attempts: int = 0,
) -> ClickHouseClient:
    client = ClickHouseClient()
    client._ClickHouseClient__client = driver  # type: ignore[attr-defined]
    client._ClickHouseClient__config = ClickHouseConfig(  # type: ignore[attr-defined]
        read_retry_attempts=retry_attempts,
        read_retry_base_delay=timedelta(milliseconds=1),
    )

    return client


# ....................... #


@pytest.mark.asyncio
async def test_single_execution_streams_all_blocks() -> None:
    rows = [(f"e{i}", i) for i in range(7)]
    driver = _StreamingAsyncClient(rows)
    client = _client(driver)

    result = await client.run_query_all_pages("SELECT 1", fetch_batch_size=3)

    # One driver execution, not one per page.
    assert len(driver.calls) == 1
    assert driver.calls[0]["settings"]["max_block_size"] == 3
    # All rows, in stream order, exactly once.
    assert result == [{"event": f"e{i}", "value": i} for i in range(7)]
    # Stream consumed inside its context.
    assert driver.streams[0].entered and driver.streams[0].exited


# ....................... #


@pytest.mark.asyncio
async def test_retry_not_nested_on_persistent_failure() -> None:
    driver = _StreamingAsyncClient([], fail_always=True)
    client = _client(driver, retry_attempts=2)

    with pytest.raises(CoreException):
        await client.run_query_all_pages("SELECT 1", fetch_batch_size=2)

    # Single retry layer: attempts + 1 executions, not (attempts + 1)**2.
    assert len(driver.calls) == 3


# ....................... #


@pytest.mark.asyncio
async def test_retry_recovers_after_transient_failure() -> None:
    rows = [("ok", 1)]
    driver = _StreamingAsyncClient(rows, fail_first=1)
    client = _client(driver, retry_attempts=1)

    result = await client.run_query_all_pages("SELECT 1", fetch_batch_size=2)

    assert len(driver.calls) == 2
    assert result == [{"event": "ok", "value": 1}]


# ....................... #


@pytest.mark.asyncio
async def test_max_rows_pushed_to_sql_and_enforced_client_side() -> None:
    # Stub ignores the SQL LIMIT, so this also proves client-side truncation.
    rows = [(f"e{i}", i) for i in range(10)]
    driver = _StreamingAsyncClient(rows)
    client = _client(driver)

    result = await client.run_query_all_pages(
        "SELECT 1",
        max_rows=4,
        fetch_batch_size=3,
    )

    assert len(driver.calls) == 1
    assert "LIMIT 4" in driver.calls[0]["query"]
    assert result == [{"event": f"e{i}", "value": i} for i in range(4)]


# ....................... #


@pytest.mark.asyncio
async def test_empty_result_returns_empty_list() -> None:
    driver = _StreamingAsyncClient([])
    client = _client(driver)

    result = await client.run_query_all_pages("SELECT 1", fetch_batch_size=5)

    assert result == []
    assert len(driver.calls) == 1


# ....................... #


@pytest.mark.asyncio
async def test_invalid_fetch_batch_size_raises_before_execution() -> None:
    driver = _StreamingAsyncClient([])
    client = _client(driver)

    with pytest.raises(CoreException):
        await client.run_query_all_pages("SELECT 1", fetch_batch_size=0)

    assert driver.calls == []
