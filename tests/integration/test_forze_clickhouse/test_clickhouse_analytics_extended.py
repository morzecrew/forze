"""Extended ClickHouse analytics integration (pagination, projections, keyset, errors)."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from forze.application.contracts.base import CountlessPage, Page
from forze.base.exceptions import CoreException, ExceptionKind
from forze_clickhouse.adapters import ClickHouseAnalyticsAdapter
from forze_clickhouse.execution.deps.configs import (
    ClickHouseAnalyticsConfig,
    ClickHouseQueryConfig,
)
from forze_clickhouse.kernel.client import ClickHouseClient

pytestmark = pytest.mark.integration


class _Row(BaseModel):
    event: str
    value: int


class _Params(BaseModel):
    min_value: int = 0


class _Ingest(BaseModel):
    event: str
    value: int = 1


def _spec() -> AnalyticsSpec[_Row, _Ingest]:
    return AnalyticsSpec(
        name="events",
        read=_Row,
        queries={
            "filtered": AnalyticsQueryDefinition(params=_Params),
            "ordered": AnalyticsQueryDefinition(params=_Params),
        },
        ingest=_Ingest,
    )


def _adapter(
    client: ClickHouseClient,
    database_id: str,
    table_id: str,
    *,
    skip_total: bool = False,
) -> ClickHouseAnalyticsAdapter[_Row, _Ingest]:
    filtered_sql = (
        f"SELECT event, value FROM {database_id}.{table_id} "
        "WHERE value >= {min_value:Int32}"
    )
    ordered_sql = (
        f"SELECT event, value FROM {database_id}.{table_id} ORDER BY event"
    )
    filtered_q = ClickHouseQueryConfig(sql=filtered_sql, skip_total=skip_total)
    ordered_q = ClickHouseQueryConfig(sql=ordered_sql)

    return ClickHouseAnalyticsAdapter(
        client=client,
        spec=_spec(),
        config=ClickHouseAnalyticsConfig(
            database=database_id,
            queries={"filtered": filtered_q, "ordered": ordered_q},
            ingest_table=table_id,
        ),
    )


@pytest.mark.asyncio
async def test_run_page_with_param_filter(
    clickhouse_client: ClickHouseClient,
    analytics_table: tuple[str, str],
) -> None:
    database_id, table_id = analytics_table
    adapter = _adapter(clickhouse_client, database_id, table_id)

    await adapter.append(
        [
            _Ingest(event="low", value=1),
            _Ingest(event="high", value=10),
        ],
    )

    page = await adapter.run_page("filtered", _Params(min_value=5))
    assert isinstance(page, Page)
    assert page.count >= 1
    assert all(hit.value >= 5 for hit in page.hits)


@pytest.mark.asyncio
async def test_skip_total_returns_countless_page(
    clickhouse_client: ClickHouseClient,
    analytics_table: tuple[str, str],
) -> None:
    database_id, table_id = analytics_table
    adapter = _adapter(clickhouse_client, database_id, table_id, skip_total=True)

    await adapter.append([_Ingest(event="only", value=1)])
    page = await adapter.run_page("filtered", _Params())

    assert isinstance(page, CountlessPage)
    assert not isinstance(page, Page)


@pytest.mark.asyncio
async def test_run_pagination_limit_offset(
    clickhouse_client: ClickHouseClient,
    analytics_table: tuple[str, str],
) -> None:
    database_id, table_id = analytics_table
    adapter = _adapter(clickhouse_client, database_id, table_id)

    for i in range(4):
        await adapter.append([_Ingest(event=f"evt_{i}", value=i)])

    page = await adapter.run(
        "filtered",
        _Params(),
        pagination={"limit": 2, "offset": 1},
    )
    assert len(page.hits) <= 2


@pytest.mark.asyncio
async def test_project_run_page_returns_field_subset(
    clickhouse_client: ClickHouseClient,
    analytics_table: tuple[str, str],
) -> None:
    database_id, table_id = analytics_table
    adapter = _adapter(clickhouse_client, database_id, table_id)

    await adapter.append([_Ingest(event="proj", value=7)])

    page = await adapter.project_run_page(
        ("event",),
        "filtered",
        _Params(),
    )
    assert page.count >= 1
    assert page.hits[0] == {"event": "proj"}
    assert "value" not in page.hits[0]


@pytest.mark.asyncio
async def test_dry_run_returns_empty_page(
    clickhouse_client: ClickHouseClient,
    analytics_table: tuple[str, str],
) -> None:
    database_id, table_id = analytics_table
    adapter = _adapter(clickhouse_client, database_id, table_id)

    page = await adapter.run_page(
        "filtered",
        _Params(),
        options={"dry_run": True},
    )
    assert page.hits == []
    assert page.count == 0


@pytest.mark.asyncio
async def test_select_run_chunked(
    clickhouse_client: ClickHouseClient,
    analytics_table: tuple[str, str],
) -> None:
    database_id, table_id = analytics_table
    adapter = _adapter(clickhouse_client, database_id, table_id)

    await adapter.append([_Ingest(event=f"c_{i}", value=i) for i in range(3)])

    class _Proj(BaseModel):
        value: int

    batches = [
        batch
        async for batch in adapter.select_run_chunked(
            _Proj,
            "filtered",
            _Params(),
            fetch_batch_size=2,
        )
    ]
    assert sum(len(b) for b in batches) >= 3


@pytest.mark.asyncio
async def test_ordered_run_cursor_offset_pagination(
    clickhouse_client: ClickHouseClient,
    analytics_table: tuple[str, str],
) -> None:
    database_id, table_id = analytics_table
    adapter = _adapter(clickhouse_client, database_id, table_id)

    for i in range(5):
        await adapter.append([_Ingest(event=f"ord_{i}", value=i)])

    seen: list[str] = []
    cursor: dict[str, object] | None = {"limit": 2}
    while cursor is not None:
        page = await adapter.run_cursor("ordered", _Params(), cursor=cursor)
        seen.extend(hit.event for hit in page.hits)
        cursor = (
            {"limit": 2, "after": page.next_cursor}
            if page.next_cursor is not None
            else None
        )

    assert len(seen) == 5
    assert len(set(seen)) == 5


@pytest.mark.asyncio
async def test_invalid_sql_surfaces_infrastructure_error(
    clickhouse_client: ClickHouseClient,
    analytics_table: tuple[str, str],
) -> None:
    database_id, table_id = analytics_table
    adapter = ClickHouseAnalyticsAdapter(
        client=clickhouse_client,
        spec=_spec(),
        config=ClickHouseAnalyticsConfig(
            database=database_id,
            queries={
                "filtered": ClickHouseQueryConfig(
                    sql=f"SELECT not_a_column FROM {database_id}.{table_id}",
                ),
                "ordered": ClickHouseQueryConfig(
                    sql=f"SELECT event, value FROM {database_id}.{table_id} ORDER BY event",
                ),
            },
            ingest_table=table_id,
        ),
    )

    with pytest.raises(CoreException) as exc_info:
        await adapter.run_page("filtered", _Params())

    err = exc_info.value
    assert err.kind in (ExceptionKind.INFRASTRUCTURE, ExceptionKind.INTERNAL)
