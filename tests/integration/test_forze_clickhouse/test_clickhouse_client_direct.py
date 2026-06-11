"""Direct integration tests for :class:`ClickHouseClient` internals.

These exercise the client surface (query/insert/command variants, settings,
health, param binding, read-retry) rather than going through the analytics
adapter, raising coverage of the kernel client module.
"""

from __future__ import annotations

from datetime import timedelta

import attrs
import pytest
from pydantic import BaseModel

from forze.base.exceptions import CoreException
from forze_clickhouse.kernel.client import (
    ClickHouseClient,
    ClickHouseConfig,
)
from forze_clickhouse.kernel.client.value_objects import ClickHouseInsertResult

pytestmark = pytest.mark.integration


class _Bind(BaseModel):
    threshold: int


@pytest.mark.asyncio
async def test_initialize_is_idempotent(
    clickhouse_connection: ClickHouseConfig,
) -> None:
    client = ClickHouseClient()
    await client.initialize(clickhouse_connection)
    # Second call must early-return without replacing the client.
    await client.initialize(clickhouse_connection)
    _, ok = await client.health()
    assert ok is True
    await client.close()


@pytest.mark.asyncio
async def test_uninitialized_client_raises() -> None:
    client = ClickHouseClient()
    with pytest.raises(CoreException):
        await client.run_query("SELECT 1")


@pytest.mark.asyncio
async def test_uninitialized_command_raises() -> None:
    client = ClickHouseClient()
    with pytest.raises(CoreException):
        await client.run_command("SELECT 1")


@pytest.mark.asyncio
async def test_close_when_never_initialized_is_safe() -> None:
    client = ClickHouseClient()
    await client.close()


@pytest.mark.asyncio
async def test_health_reports_failure_after_close(
    clickhouse_connection: ClickHouseConfig,
) -> None:
    client = ClickHouseClient()
    await client.initialize(clickhouse_connection)
    await client.close()
    message, ok = await client.health()
    assert ok is False
    assert message


@pytest.mark.asyncio
async def test_run_query_with_dict_params(
    clickhouse_client: ClickHouseClient,
) -> None:
    result = await clickhouse_client.run_query(
        "SELECT {n:Int32} AS n",
        {"n": 7},
    )
    assert result.row_count == 1
    assert result.rows[0]["n"] == 7


@pytest.mark.asyncio
async def test_run_query_with_model_params(
    clickhouse_client: ClickHouseClient,
    analytics_table: tuple[str, str],
) -> None:
    database_id, table_id = analytics_table
    await clickhouse_client.insert_rows(
        database_id,
        table_id,
        [{"event": "a", "value": 1}, {"event": "b", "value": 9}],
    )
    result = await clickhouse_client.run_query(
        f"SELECT event, value FROM {database_id}.{table_id} "
        "WHERE value >= {threshold:Int32}",
        _Bind(threshold=5),
    )
    assert all(row["value"] >= 5 for row in result.rows)


@pytest.mark.asyncio
async def test_run_query_max_rows_caps_limit(
    clickhouse_client: ClickHouseClient,
    analytics_table: tuple[str, str],
) -> None:
    database_id, table_id = analytics_table
    await clickhouse_client.insert_rows(
        database_id,
        table_id,
        [{"event": f"e{i}", "value": i} for i in range(10)],
    )
    result = await clickhouse_client.run_query(
        f"SELECT event, value FROM {database_id}.{table_id}",
        max_rows=3,
    )
    assert result.row_count <= 3


@pytest.mark.asyncio
async def test_run_query_max_rows_with_explicit_limit(
    clickhouse_client: ClickHouseClient,
    analytics_table: tuple[str, str],
) -> None:
    database_id, table_id = analytics_table
    await clickhouse_client.insert_rows(
        database_id,
        table_id,
        [{"event": f"e{i}", "value": i} for i in range(10)],
    )
    # max_rows lower than limit -> min() branch.
    result = await clickhouse_client.run_query(
        f"SELECT event, value FROM {database_id}.{table_id}",
        limit=5,
        max_rows=2,
    )
    assert result.row_count <= 2


@pytest.mark.asyncio
async def test_run_query_with_timeout_override(
    clickhouse_client: ClickHouseClient,
) -> None:
    result = await clickhouse_client.run_query(
        "SELECT 1 AS one",
        timeout=timedelta(seconds=5),
    )
    assert result.rows[0]["one"] == 1


@pytest.mark.asyncio
async def test_run_query_with_database_override(
    clickhouse_client: ClickHouseClient,
    analytics_table: tuple[str, str],
) -> None:
    database_id, table_id = analytics_table
    await clickhouse_client.insert_rows(
        database_id,
        table_id,
        [{"event": "ov", "value": 1}],
    )
    result = await clickhouse_client.run_query(
        f"SELECT event, value FROM {table_id}",
        database=database_id,
    )
    assert result.row_count >= 1


@pytest.mark.asyncio
async def test_insert_empty_rows_returns_zero(
    clickhouse_client: ClickHouseClient,
    analytics_table: tuple[str, str],
) -> None:
    database_id, table_id = analytics_table
    result = await clickhouse_client.insert_rows(database_id, table_id, [])
    assert result == ClickHouseInsertResult(accepted=0)


@pytest.mark.asyncio
async def test_run_command_with_model_params(
    clickhouse_client: ClickHouseClient,
    analytics_table: tuple[str, str],
) -> None:
    database_id, table_id = analytics_table
    await clickhouse_client.insert_rows(
        database_id,
        table_id,
        [{"event": "cmd", "value": 3}],
    )
    # run_command with a BaseModel exercises parameters_from_model in run_command.
    await clickhouse_client.run_command(
        f"DELETE FROM {database_id}.{table_id} WHERE value < {{threshold:Int32}}",
        _Bind(threshold=2),
    )


@pytest.mark.asyncio
async def test_run_query_all_pages_invalid_batch_size(
    clickhouse_client: ClickHouseClient,
    analytics_table: tuple[str, str],
) -> None:
    database_id, table_id = analytics_table
    with pytest.raises(CoreException):
        await clickhouse_client.run_query_all_pages(
            f"SELECT event, value FROM {database_id}.{table_id}",
            fetch_batch_size=0,
        )


@pytest.mark.asyncio
async def test_run_query_all_pages_paginates(
    clickhouse_client: ClickHouseClient,
    analytics_table: tuple[str, str],
) -> None:
    database_id, table_id = analytics_table
    await clickhouse_client.insert_rows(
        database_id,
        table_id,
        [{"event": f"p{i}", "value": i} for i in range(5)],
    )
    rows = await clickhouse_client.run_query_all_pages(
        f"SELECT event, value FROM {database_id}.{table_id} ORDER BY value",
        fetch_batch_size=2,
    )
    assert len(rows) == 5


@pytest.mark.asyncio
async def test_run_query_all_pages_exactly_once_in_order(
    clickhouse_client: ClickHouseClient,
    analytics_table: tuple[str, str],
) -> None:
    """Streaming fetch returns every row exactly once, in query order."""

    database_id, table_id = analytics_table
    total = 25
    await clickhouse_client.insert_rows(
        database_id,
        table_id,
        [{"event": f"s{i}", "value": i} for i in range(total)],
    )
    rows = await clickhouse_client.run_query_all_pages(
        f"SELECT event, value FROM {database_id}.{table_id} ORDER BY value",
        fetch_batch_size=4,
    )
    assert [row["value"] for row in rows] == list(range(total))
    assert [row["event"] for row in rows] == [f"s{i}" for i in range(total)]


@pytest.mark.asyncio
async def test_run_query_all_pages_max_rows_stops(
    clickhouse_client: ClickHouseClient,
    analytics_table: tuple[str, str],
) -> None:
    database_id, table_id = analytics_table
    await clickhouse_client.insert_rows(
        database_id,
        table_id,
        [{"event": f"m{i}", "value": i} for i in range(6)],
    )
    rows = await clickhouse_client.run_query_all_pages(
        f"SELECT event, value FROM {database_id}.{table_id} ORDER BY value",
        max_rows=3,
        fetch_batch_size=2,
    )
    assert len(rows) == 3


@pytest.mark.asyncio
async def test_read_retry_recovers_after_transient_error(
    clickhouse_connection: ClickHouseConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient read error is retried then succeeds (153-158 path)."""

    config = attrs.evolve(
        clickhouse_connection,
        read_retry_attempts=2,
        read_retry_base_delay=timedelta(seconds=0.01),
    )
    client = ClickHouseClient()
    await client.initialize(config)
    try:
        real_query = client._ClickHouseClient__client.query  # type: ignore[attr-defined]
        calls = {"n": 0}

        async def flaky(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise ConnectionError("transient")
            return await real_query(*args, **kwargs)

        monkeypatch.setattr(
            client._ClickHouseClient__client,  # type: ignore[attr-defined]
            "query",
            flaky,
        )
        result = await client.run_query("SELECT 1 AS one")
        assert result.rows[0]["one"] == 1
        assert calls["n"] == 2
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_read_retry_exhausts_and_reraises(
    clickhouse_connection: ClickHouseConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exhausting retries re-raises the last transient error (155-156 path)."""

    config = attrs.evolve(
        clickhouse_connection,
        read_retry_attempts=1,
        read_retry_base_delay=timedelta(seconds=0.01),
    )
    client = ClickHouseClient()
    await client.initialize(config)
    try:

        async def always_fail(*args, **kwargs):
            raise ConnectionError("down")

        monkeypatch.setattr(
            client._ClickHouseClient__client,  # type: ignore[attr-defined]
            "query",
            always_fail,
        )
        with pytest.raises(CoreException):
            await client.run_query("SELECT 1 AS one")
    finally:
        await client.close()
