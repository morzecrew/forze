"""Integration tests for ClickHouse analytics adapter."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze_clickhouse.adapters import ClickHouseAnalyticsAdapter
from forze_clickhouse.execution import ClickHouseAnalyticsConfig, ClickHouseDepsModule
from forze_clickhouse.execution.deps.configs import ClickHouseQueryConfig

pytestmark = pytest.mark.integration


class _Row(BaseModel):
    value: int
    event: str


class _Params(BaseModel):
    pass


class _Ingest(BaseModel):
    event: str
    value: int = 1


def _spec() -> AnalyticsSpec[_Row, _Ingest]:
    return AnalyticsSpec(
        name="events",
        read=_Row,
        queries={
            "all": AnalyticsQueryDefinition(params=_Params),
        },
        ingest=_Ingest,
    )


def _config(
    database_id: str,
    table_id: str,
    *,
    sql: str | None = None,
) -> ClickHouseAnalyticsConfig:
    query_sql = sql or f"SELECT event, value FROM {database_id}.{table_id}"
    return ClickHouseAnalyticsConfig(
        database=database_id,
        queries={"all": ClickHouseQueryConfig(sql=query_sql)},
        ingest_table=table_id,
    )


@pytest.mark.asyncio
async def test_append_and_query(clickhouse_client, analytics_table) -> None:
    database_id, table_id = analytics_table
    spec = _spec()
    adapter = ClickHouseAnalyticsAdapter(
        client=clickhouse_client,
        spec=spec,
        config=_config(database_id, table_id),
    )

    await adapter.append([_Ingest(event="signup", value=42)])

    page = await adapter.run_page("all", _Params())
    assert page.count >= 1
    assert any(hit.event == "signup" and hit.value == 42 for hit in page.hits)


@pytest.mark.asyncio
async def test_deps_module_wiring(clickhouse_client, analytics_table) -> None:
    database_id, table_id = analytics_table
    spec = _spec()
    module = ClickHouseDepsModule(
        client=clickhouse_client,
        analytics={"events": _config(database_id, table_id)},
    )
    ctx = context_from_deps(module())
    port = ctx.analytics.query(spec)
    page = await port.run("all", _Params())
    assert len(page.hits) >= 0


@pytest.mark.asyncio
async def test_client_health(clickhouse_client) -> None:
    message, ok = await clickhouse_client.health()
    assert ok is True


@pytest.mark.asyncio
async def test_run_chunked_reads_batches(clickhouse_client, analytics_table) -> None:
    database_id, table_id = analytics_table
    spec = _spec()
    adapter = ClickHouseAnalyticsAdapter(
        client=clickhouse_client,
        spec=spec,
        config=_config(database_id, table_id),
    )

    await adapter.append([_Ingest(event=f"evt_{i}", value=i) for i in range(3)])

    batches = [
        batch
        async for batch in adapter.run_chunked(
            "all",
            _Params(),
            fetch_batch_size=2,
        )
    ]
    total = sum(len(batch) for batch in batches)
    assert total >= 3


@pytest.mark.asyncio
async def test_run_cursor_offset_pagination(
    clickhouse_client,
    analytics_table,
) -> None:
    database_id, table_id = analytics_table
    spec = _spec()
    adapter = ClickHouseAnalyticsAdapter(
        client=clickhouse_client,
        spec=spec,
        config=_config(database_id, table_id),
    )

    await adapter.append([_Ingest(event=f"page_{i}", value=i) for i in range(5)])

    seen: list[str] = []
    cursor: dict[str, object] | None = {"limit": 2}
    while cursor is not None:
        page = await adapter.run_cursor("all", _Params(), cursor=cursor)
        seen.extend(hit.event for hit in page.hits)
        cursor = (
            {"limit": 2, "after": page.next_cursor}
            if page.next_cursor is not None
            else None
        )

    assert len(seen) == 5
    assert len(set(seen)) == 5


@pytest.mark.asyncio
async def test_run_cursor_rejects_before_cursor(
    clickhouse_client,
    analytics_table,
) -> None:
    database_id, table_id = analytics_table
    adapter = ClickHouseAnalyticsAdapter(
        client=clickhouse_client,
        spec=_spec(),
        config=_config(database_id, table_id),
    )

    with pytest.raises(CoreException, match="Backward analytics cursors"):
        await adapter.run_cursor(
            "all",
            _Params(),
            cursor={"limit": 2, "before": "opaque"},
        )
