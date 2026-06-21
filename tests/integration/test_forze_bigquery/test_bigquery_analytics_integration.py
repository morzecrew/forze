"""Integration tests for BigQuery analytics adapter against goccy emulator."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
    IngestSpec,
)
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze_bigquery.adapters import BigQueryAnalyticsAdapter
from forze_bigquery.execution import BigQueryAnalyticsConfig, BigQueryDepsModule
from forze_bigquery.execution.deps.configs import BigQueryQueryConfig

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


def _config(dataset_id: str, table_id: str, *, sql: str | None = None) -> BigQueryAnalyticsConfig:
    query_sql = sql or f"SELECT event, value FROM {dataset_id}.{table_id}"
    return BigQueryAnalyticsConfig(
        dataset=dataset_id,
        queries={
            "all": BigQueryQueryConfig(sql=query_sql),
        },
        ingest=IngestSpec((dataset_id, table_id)),
    )


@pytest.mark.asyncio
async def test_append_and_query(bigquery_client, analytics_dataset) -> None:
    dataset_id, table_id = analytics_dataset
    spec = _spec()
    adapter = BigQueryAnalyticsAdapter(
        client=bigquery_client,
        spec=spec,
        config=_config(dataset_id, table_id),
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
        analytics={"events": _config(dataset_id, table_id)},
    )
    ctx = context_from_deps(module())
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
        config=_config(dataset_id, table_id),
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
async def test_run_chunked_reads_batches(bigquery_client, analytics_dataset) -> None:
    dataset_id, table_id = analytics_dataset
    adapter = BigQueryAnalyticsAdapter(
        client=bigquery_client,
        spec=_spec(),
        config=_config(dataset_id, table_id),
    )

    await adapter.append([_Ingest(event=f"chunk_{i}", value=i) for i in range(3)])

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
async def test_invalid_query_surfaces_infrastructure_error(
    bigquery_client,
    analytics_dataset,
) -> None:
    dataset_id, table_id = analytics_dataset
    adapter = BigQueryAnalyticsAdapter(
        client=bigquery_client,
        spec=_spec(),
        config=_config(
            dataset_id,
            table_id,
            sql=f"SELECT definitely_not_a_column FROM {dataset_id}.{table_id}",
        ),
    )

    with pytest.raises(CoreException) as exc_info:
        await adapter.run_page("all", _Params())

    err = exc_info.value
    assert err.details is not None
    assert err.details.get("site") == "bigquery.run_query"
    assert err.kind in (ExceptionKind.INFRASTRUCTURE, ExceptionKind.INTERNAL)


@pytest.mark.asyncio
async def test_client_health(bigquery_client) -> None:
    message, ok = await bigquery_client.health()
    assert ok is True


@pytest.mark.asyncio
async def test_project_run_and_select_run(bigquery_client, analytics_dataset) -> None:
    dataset_id, table_id = analytics_dataset
    spec = _spec()
    sql = f"SELECT event, value FROM `{dataset_id}.{table_id}`"
    adapter = BigQueryAnalyticsAdapter(
        client=bigquery_client,
        spec=spec,
        config=_config(dataset_id, table_id, sql=sql),
    )

    await adapter.append([_Ingest(event="proj", value=11)])

    projected = await adapter.project_run(
        ("event", "value"),
        "all",
        _Params(),
        pagination={"limit": 5, "offset": 0},
    )
    assert projected.hits[0]["event"] == "proj"

    class _RowLite(BaseModel):
        event: str

    selected = await adapter.select_run(
        _RowLite,
        "all",
        _Params(),
        pagination={"limit": 5, "offset": 0},
    )
    assert selected.hits[0].event == "proj"


@pytest.mark.asyncio
async def test_run_cursor_rejects_before_token(bigquery_client, analytics_dataset) -> None:
    dataset_id, table_id = analytics_dataset
    adapter = BigQueryAnalyticsAdapter(
        client=bigquery_client,
        spec=_spec(),
        config=_config(dataset_id, table_id),
    )

    with pytest.raises(CoreException, match="Backward analytics cursors"):
        await adapter.run_cursor("all", _Params(), cursor={"before": "tok"})
