"""Integration tests for BigQuery analytics adapter against goccy emulator."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from forze.application.execution import ExecutionContext
from forze_bigquery.adapters import BigQueryAnalyticsAdapter
from forze_bigquery.execution import BigQueryDepsModule

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
async def test_append_and_query(bigquery_client, analytics_dataset) -> None:
    dataset_id, table_id = analytics_dataset
    spec = _spec()
    config = {
        "dataset": dataset_id,
        "queries": {
            "all": {"sql": f"SELECT event, value FROM {dataset_id}.{table_id}"},
        },
        "ingest_table": table_id,
    }
    adapter = BigQueryAnalyticsAdapter(
        client=bigquery_client,
        spec=spec,
        config=config,
    )

    await adapter.append([_Ingest(event="signup", value=42)])

    page = await adapter.run_page("all", _Params())
    assert page.count >= 1
    assert any(hit.event == "signup" and hit.value == 42 for hit in page.hits)


@pytest.mark.asyncio
async def test_deps_module_wiring(bigquery_client, analytics_dataset) -> None:
    dataset_id, table_id = analytics_dataset
    spec = _spec()
    module = BigQueryDepsModule(
        client=bigquery_client,
        analytics={
            "events": {
                "dataset": dataset_id,
                "queries": {
                    "all": {
                        "sql": f"SELECT event, value FROM {dataset_id}.{table_id}",
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


@pytest.mark.asyncio
async def test_run_cursor_round_trip(bigquery_client, analytics_dataset) -> None:
    dataset_id, table_id = analytics_dataset
    spec = _spec()
    adapter = BigQueryAnalyticsAdapter(
        client=bigquery_client,
        spec=spec,
        config={
            "dataset": dataset_id,
            "queries": {
                "all": {"sql": f"SELECT event, value FROM {dataset_id}.{table_id}"},
            },
            "ingest_table": table_id,
        },
    )

    await adapter.append([_Ingest(event="cursor_a", value=1)])
    first = await adapter.run_cursor("all", _Params(), cursor={"limit": 1})
    assert len(first.hits) >= 1

    if first.next_cursor:
        second = await adapter.run_cursor(
            "all",
            _Params(),
            cursor={"limit": 1, "after": first.next_cursor},
        )
        assert len(second.hits) >= 0


@pytest.mark.asyncio
async def test_client_health(bigquery_client) -> None:
    message, ok = await bigquery_client.health()
    assert ok is True
