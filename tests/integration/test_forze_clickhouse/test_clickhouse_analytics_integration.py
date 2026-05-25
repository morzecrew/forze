"""Integration tests for ClickHouse analytics adapter."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from forze.application.execution import ExecutionContext
from forze_clickhouse.adapters import ClickHouseAnalyticsAdapter
from forze_clickhouse.execution import ClickHouseDepsModule

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


@pytest.mark.asyncio
async def test_append_and_query(clickhouse_client, analytics_table) -> None:
    database_id, table_id = analytics_table
    spec = _spec()
    config = {
        "database": database_id,
        "queries": {
            "all": {
                "sql": f"SELECT event, value FROM {database_id}.{table_id}",
            },
        },
        "ingest_table": table_id,
    }
    adapter = ClickHouseAnalyticsAdapter(
        client=clickhouse_client,
        spec=spec,
        config=config,
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
        analytics={
            "events": {
                "database": database_id,
                "queries": {
                    "all": {
                        "sql": f"SELECT event, value FROM {database_id}.{table_id}",
                    },
                },
                "ingest_table": table_id,
            },
        },
    )
    ctx = ExecutionContext(deps=module())
    port = ctx.analytics.query(spec)
    page = await port.run("all", _Params())
    assert len(page.hits) >= 0
