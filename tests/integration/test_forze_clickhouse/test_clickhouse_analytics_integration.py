"""Integration tests for ClickHouse analytics adapter."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
    IngestSpec,
)
from forze.base.exceptions import CoreException
from forze_clickhouse.adapters import ClickHouseAnalyticsAdapter
from forze_clickhouse.execution import ClickHouseAnalyticsConfig, ClickHouseDepsModule
from forze_clickhouse.execution.deps.configs import ClickHouseQueryConfig
from tests.support.execution_context import (
    context_from_deps,
)

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
        ingest=IngestSpec((database_id, table_id)),
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
async def test_run_chunked_exactly_once_in_order(
    clickhouse_client,
    analytics_table,
) -> None:
    """run_chunked over the streaming client yields each row once, in order."""

    database_id, table_id = analytics_table
    spec = _spec()
    adapter = ClickHouseAnalyticsAdapter(
        client=clickhouse_client,
        spec=spec,
        config=_config(
            database_id,
            table_id,
            sql=f"SELECT event, value FROM {database_id}.{table_id} ORDER BY value",
        ),
    )

    total = 9
    await adapter.append([_Ingest(event=f"ord_{i}", value=i) for i in range(total)])

    batches = [
        batch
        async for batch in adapter.run_chunked(
            "all",
            _Params(),
            fetch_batch_size=4,
        )
    ]
    assert [len(batch) for batch in batches] == [4, 4, 1]
    values = [row.value for batch in batches for row in batch]
    assert values == list(range(total))


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
        cursor = {"limit": 2, "after": page.next_cursor} if page.next_cursor is not None else None

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


class _RichRow(BaseModel):
    order_id: UUID
    placed_at: datetime


class _RichIngest(BaseModel):
    order_id: UUID
    placed_at: datetime
    total: Decimal


@pytest.mark.asyncio
async def test_ingest_of_uuid_datetime_and_decimal_binds_natively(clickhouse_client) -> None:
    """A ``UUID`` / ``datetime`` / ``Decimal`` ingest row lands on its typed columns.

    The mirror of the BigQuery test next door, and it exists to pin the *opposite* answer.
    BigQuery's wire is JSON, so its rows must be encoded to JSON before they are sent; ClickHouse
    binds Python values straight onto a ``UUID`` / ``DateTime64`` / ``Decimal`` column, so
    encoding them to strings first would push them back through the column's own parser for
    nothing. The shared ingest encoder therefore defaults to the Python encode and BigQuery opts
    out — and this test is what stops a future "fix" from flipping the default and quietly making
    ClickHouse round-trip its values through text.
    """

    table = f"rich_{uuid4().hex[:10]}"
    await clickhouse_client.run_command(
        f"CREATE TABLE default.{table} "
        "(order_id UUID, placed_at DateTime64(3), total Decimal(18, 2)) ENGINE = Memory"
    )

    spec = AnalyticsSpec(
        name="orders",
        read=_RichRow,
        queries={"all": AnalyticsQueryDefinition(params=_Params)},
        ingest=_RichIngest,
    )
    adapter = ClickHouseAnalyticsAdapter(
        client=clickhouse_client,
        spec=spec,
        config=ClickHouseAnalyticsConfig(
            database="default",
            queries={
                "all": ClickHouseQueryConfig(sql=f"SELECT order_id, placed_at FROM default.{table}")
            },
            ingest=IngestSpec(("default", table)),
        ),
    )

    sent = _RichIngest(
        order_id=uuid4(),
        placed_at=datetime(2026, 7, 14, 12, 30, tzinfo=UTC),
        total=Decimal("19.99"),
    )

    await adapter.append([sent])

    page = await adapter.run_page("all", _Params())

    assert page.count == 1
    assert page.hits[0].order_id == sent.order_id
    assert isinstance(page.hits[0].order_id, UUID)
