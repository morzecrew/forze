"""Integration tests for BigQuery analytics adapter against goccy emulator."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from gcloud.aio.bigquery import Table
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
    IngestSpec,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze_bigquery.adapters import BigQueryAnalyticsAdapter
from forze_bigquery.execution import BigQueryAnalyticsConfig, BigQueryDepsModule
from forze_bigquery.execution.deps.configs import BigQueryQueryConfig
from tests.integration.test_forze_bigquery.conftest import TEST_PROJECT_ID
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


class _RichRow(BaseModel):
    order_id: str
    placed_at: datetime


class _RichIngest(BaseModel):
    order_id: UUID
    placed_at: datetime
    total: Decimal


@pytest_asyncio.fixture(scope="function")
async def rich_table(bigquery_client, analytics_dataset):
    """A table with the column types a ``str``/``int`` ingest model never exercises."""

    dataset_id = analytics_dataset[0]
    table_id = f"rich_{uuid4().hex[:12]}"
    bq_table = Table(
        dataset_name=dataset_id,
        table_name=table_id,
        project=TEST_PROJECT_ID,
        session=bigquery_client.session,
        api_root=bigquery_client.api_root,
    )
    await bq_table.create(
        {
            "tableReference": {
                "projectId": TEST_PROJECT_ID,
                "datasetId": dataset_id,
                "tableId": table_id,
            },
            "schema": {
                "fields": [
                    {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "placed_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
                    {"name": "total", "type": "NUMERIC", "mode": "NULLABLE"},
                ]
            },
        },
        timeout=30,
    )
    return dataset_id, table_id


@pytest.mark.asyncio
async def test_ingest_of_uuid_datetime_and_decimal(bigquery_client, rich_table) -> None:
    """An ingest row carrying a ``UUID``, a ``datetime`` and a ``Decimal`` reaches the table.

    It did not. A BigQuery streaming insert is an HTTP ``insertAll``: the client hands the row
    map straight to a JSON serializer, and the shared ingest encoder was producing Python
    objects — right for Postgres and ClickHouse, which bind those values natively onto typed
    columns, and impossible for a wire that is literally JSON. ``append`` raised ``TypeError:
    Object of type UUID is not JSON serializable`` before the request was even sent.

    Every other ingest model in this suite is ``event: str`` / ``value: int``, which is exactly
    why nothing here ever asked the serializer to encode something it could not.
    """

    dataset_id, table_id = rich_table
    spec = AnalyticsSpec(
        name="orders",
        read=_RichRow,
        queries={"all": AnalyticsQueryDefinition(params=_Params)},
        ingest=_RichIngest,
    )
    adapter = BigQueryAnalyticsAdapter(
        client=bigquery_client,
        spec=spec,
        config=BigQueryAnalyticsConfig(
            dataset=dataset_id,
            queries={
                "all": BigQueryQueryConfig(
                    sql=f"SELECT order_id, placed_at FROM {dataset_id}.{table_id}"
                )
            },
            ingest=IngestSpec((dataset_id, table_id)),
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
    assert page.hits[0].order_id == str(sent.order_id)


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
