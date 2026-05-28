"""Extended Postgres analytics integration (pagination, projections, filters)."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from forze.application.contracts.base import CountlessPage, Page
from forze_postgres.adapters.analytics import PostgresAnalyticsAdapter
from forze_postgres.execution.deps.configs import PostgresAnalyticsConfig, PostgresQueryConfig
from forze_postgres.kernel.client import PostgresClient

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
    client: PostgresClient,
    table_id: str,
    *,
    skip_total: bool = False,
) -> PostgresAnalyticsAdapter[_Row, _Ingest]:
    filtered_sql = (
        f"SELECT event, value FROM public.{table_id} "
        "WHERE value >= %(min_value)s"
    )
    ordered_sql = f"SELECT event, value FROM public.{table_id} ORDER BY event"

    return PostgresAnalyticsAdapter(
        client=client,
        spec=_spec(),
        config=PostgresAnalyticsConfig(
            schema="public",
            queries={
                "filtered": PostgresQueryConfig(
                    sql=filtered_sql,
                    skip_total=skip_total,
                ),
                "ordered": PostgresQueryConfig(sql=ordered_sql),
            },
            ingest_table=table_id,
        ),
    )


@pytest.mark.asyncio
async def test_run_page_with_param_filter(
    pg_client: PostgresClient,
    pg_analytics_table: str,
) -> None:
    adapter = _adapter(pg_client, pg_analytics_table)

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
    pg_client: PostgresClient,
    pg_analytics_table: str,
) -> None:
    adapter = _adapter(pg_client, pg_analytics_table, skip_total=True)

    await adapter.append([_Ingest(event="only", value=1)])
    page = await adapter.run_page("filtered", _Params())

    assert isinstance(page, CountlessPage)
    assert not isinstance(page, Page)


@pytest.mark.asyncio
async def test_run_pagination_limit_offset(
    pg_client: PostgresClient,
    pg_analytics_table: str,
) -> None:
    adapter = _adapter(pg_client, pg_analytics_table)

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
    pg_client: PostgresClient,
    pg_analytics_table: str,
) -> None:
    adapter = _adapter(pg_client, pg_analytics_table)

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
    pg_client: PostgresClient,
    pg_analytics_table: str,
) -> None:
    adapter = _adapter(pg_client, pg_analytics_table)

    page = await adapter.run_page(
        "filtered",
        _Params(),
        options={"dry_run": True},
    )
    assert page.hits == []
    assert page.count == 0


@pytest.mark.asyncio
async def test_select_run_chunked(
    pg_client: PostgresClient,
    pg_analytics_table: str,
) -> None:
    adapter = _adapter(pg_client, pg_analytics_table)

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
