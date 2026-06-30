"""Tests for BigQueryAnalyticsAdapter with a mocked client."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
    IngestSpec,
)
from forze.application.contracts.base import CountlessPage, Page
from forze_bigquery.adapters import BigQueryAnalyticsAdapter
from forze_bigquery.execution.deps.configs import (
    BigQueryAnalyticsConfig,
    BigQueryQueryConfig,
)
from forze_bigquery.kernel.client.value_objects import (
    BigQueryInsertResult,
    BigQueryQueryResult,
)


class _Row(BaseModel):
    value: int


class _Params(BaseModel):
    day: str = "2026-01-01"


class _Ingest(BaseModel):
    event: str


def _adapter(mock: Any) -> BigQueryAnalyticsAdapter[_Row, _Ingest]:
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
        ingest=_Ingest,
    )
    config = BigQueryAnalyticsConfig(
        dataset="analytics",
        queries={
            "counts": BigQueryQueryConfig(
                sql="SELECT value FROM t WHERE day = @day",
            ),
        },
        ingest=IngestSpec(("analytics", "events_raw")),
    )
    return BigQueryAnalyticsAdapter(client=mock, spec=spec, config=config)


class _MockClient:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.params_seen: list[Any] = []
        self.datasets_seen: list[str | None] = []
        self.inserts: list[list[dict[str, Any]]] = []

    async def run_query(
        self,
        sql: str,
        params: BaseModel | dict[str, Any] | None = None,
        *,
        dry_run: bool = False,
        maximum_bytes_billed: int | None = None,
        max_results: int | None = None,
        start_index: int | None = None,
        page_token: str | None = None,
        timeout: int | None = None,
        default_dataset: str | None = None,
    ) -> BigQueryQueryResult:
        _ = (
            dry_run,
            maximum_bytes_billed,
            max_results,
            start_index,
            page_token,
            timeout,
        )
        self.datasets_seen.append(default_dataset)
        self.queries.append(sql)
        self.params_seen.append(params)
        if "COUNT(*)" in sql:
            return BigQueryQueryResult(rows=[{"forze_cnt": 2}], total_rows=1)
        return BigQueryQueryResult(
            rows=[{"value": 10}, {"value": 20}],
            total_rows=2,
            page_token="next-page",
        )

    async def run_query_all_pages(
        self,
        sql: str,
        params: BaseModel | None = None,
        *,
        maximum_bytes_billed: int | None = None,
        max_rows: int | None = None,
        timeout: int | None = None,
        fetch_batch_size: int = 2000,
        default_dataset: str | None = None,
    ) -> list[dict[str, Any]]:
        _ = maximum_bytes_billed, max_rows, timeout, fetch_batch_size
        result = await self.run_query(sql, params, default_dataset=default_dataset)
        return result.rows

    async def run_query_streamed(
        self,
        sql: str,
        params: BaseModel | None = None,
        *,
        maximum_bytes_billed: int | None = None,
        max_rows: int | None = None,
        timeout: int | None = None,
        fetch_batch_size: int = 2000,
        default_dataset: str | None = None,
    ) -> Any:
        _ = maximum_bytes_billed, max_rows, timeout
        result = await self.run_query(sql, params, default_dataset=default_dataset)
        rows = result.rows
        for start in range(0, len(rows), fetch_batch_size):
            yield rows[start : start + fetch_batch_size]

    async def insert_rows(
        self,
        dataset: str,
        table: str,
        rows: list[dict[str, Any]],
        *,
        insert_id_field: str | None = None,
        timeout: int | None = None,
    ) -> BigQueryInsertResult:
        _ = dataset, table, insert_id_field, timeout
        self.inserts.append(rows)
        return BigQueryInsertResult(accepted=len(rows))


@pytest.mark.asyncio
async def test_run_page_uses_count_wrapper() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)
    page = await adapter.run_page("counts", _Params())
    assert page.count == 2
    assert len(page.hits) == 2
    assert any("COUNT(*)" in q for q in mock.queries)


@pytest.mark.asyncio
async def test_run_chunked_streams_typed_rows() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)

    chunks = [
        [row.value for row in chunk]
        async for chunk in adapter.run_chunked(
            "counts", _Params(), fetch_batch_size=1
        )
    ]

    assert chunks == [[10], [20]]
    assert all("COUNT(*)" not in q for q in mock.queries)


@pytest.mark.asyncio
async def test_run_cursor_exposes_next_token() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)
    page = await adapter.run_cursor("counts", _Params())
    assert page.has_more is True
    assert page.next_cursor is not None


@pytest.mark.asyncio
async def test_run_page_skip_total_skips_count_query() -> None:
    mock = _MockClient()
    config = BigQueryAnalyticsConfig(
        dataset="analytics",
        queries={
            "counts": BigQueryQueryConfig(
                sql="SELECT value FROM t WHERE day = @day",
                skip_total=True,
            ),
        },
        ingest=IngestSpec(("analytics", "events_raw")),
    )
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
        ingest=_Ingest,
    )
    adapter = BigQueryAnalyticsAdapter(client=mock, spec=spec, config=config)
    page = await adapter.run_page("counts", _Params())
    assert isinstance(page, CountlessPage)
    assert not isinstance(page, Page)
    assert not any("COUNT(*)" in q for q in mock.queries)


@pytest.mark.asyncio
async def test_run_with_dry_run_does_not_execute_data_query() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)
    page = await adapter.run("counts", _Params(), options={"dry_run": True})
    assert page.hits == []
    assert mock.queries == []


@pytest.mark.asyncio
async def test_append_ingest() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)
    result = await adapter.append([_Ingest(event="signup")])
    assert result is not None
    assert result.accepted == 1
    assert mock.inserts[0][0]["event"] == "signup"


# ----------------------- #
# tenant advisory floor


def _tenant_adapter(
    mock: Any,
    tenant_provider: Any,
) -> BigQueryAnalyticsAdapter[_Row, _Ingest]:
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
    )
    config = BigQueryAnalyticsConfig(
        dataset="analytics",
        tenant_aware=True,
        queries={
            "counts": BigQueryQueryConfig(
                sql="SELECT value FROM t WHERE day = @day AND tenant_id = @tenant",
            ),
        },
    )
    return BigQueryAnalyticsAdapter(
        client=mock, spec=spec, config=config, tenant_provider=tenant_provider
    )


@pytest.mark.asyncio
async def test_tenant_aware_binds_tenant_param() -> None:
    from uuid import uuid4

    from forze.application.contracts.tenancy import TenantIdentity

    tid = uuid4()
    mock = _MockClient()
    adapter = _tenant_adapter(mock, lambda: TenantIdentity(tenant_id=tid))

    await adapter.run("counts", _Params())

    bound = mock.params_seen[-1]
    assert isinstance(bound, dict)
    assert bound["tenant"] == str(tid)


@pytest.mark.asyncio
async def test_tenant_aware_fails_closed_without_tenant() -> None:
    from forze.base.exceptions import CoreException

    mock = _MockClient()
    adapter = _tenant_adapter(mock, lambda: None)

    with pytest.raises(CoreException, match="tenant_required"):
        await adapter.run("counts", _Params())

    assert mock.queries == []


def test_tenant_aware_config_rejects_unscoped_sql() -> None:
    from forze.base.exceptions import CoreException

    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
    )
    config = BigQueryAnalyticsConfig(
        dataset="analytics",
        tenant_aware=True,
        queries={"counts": BigQueryQueryConfig(sql="SELECT value FROM t")},
    )

    with pytest.raises(CoreException, match="analytics_tenant_param_unreferenced"):
        config.validate_against_spec(spec)


# ----------------------- #
# per-tenant default dataset (namespace routing)


@pytest.mark.asyncio
async def test_query_dataset_resolver_sets_default_dataset() -> None:
    from uuid import uuid4

    from forze.application.contracts.tenancy import TenantIdentity

    tid = uuid4()
    mock = _MockClient()
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
    )
    config = BigQueryAnalyticsConfig(
        dataset="ingest_ds",
        query_dataset=lambda t: f"ds_{t}",
        queries={"counts": BigQueryQueryConfig(sql="SELECT value FROM t")},
    )
    adapter = BigQueryAnalyticsAdapter(
        client=mock,
        spec=spec,
        config=config,
        tenant_provider=lambda: TenantIdentity(tenant_id=tid),
    )

    await adapter.run("counts", _Params())

    assert mock.datasets_seen[-1] == f"ds_{tid}"


@pytest.mark.asyncio
async def test_no_query_dataset_leaves_default_unset() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)  # no query_dataset

    await adapter.run("counts", _Params())

    assert mock.datasets_seen[-1] is None
