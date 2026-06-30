"""Tests for ClickHouseAnalyticsAdapter with a mocked client."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
    IngestSpec,
)
from forze_clickhouse.adapters import ClickHouseAnalyticsAdapter
from forze_clickhouse.execution.deps.configs import (
    ClickHouseAnalyticsConfig,
    ClickHouseQueryConfig,
)
from forze_clickhouse.kernel.client.value_objects import (
    ClickHouseInsertResult,
    ClickHouseQueryResult,
)


class _Row(BaseModel):
    value: int


class _Params(BaseModel):
    day: str = "2026-01-01"


class _Ingest(BaseModel):
    event: str


def _adapter(mock: Any) -> ClickHouseAnalyticsAdapter[_Row, _Ingest]:
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
        ingest=_Ingest,
    )
    config = ClickHouseAnalyticsConfig(
        database="analytics",
        queries={
            "counts": ClickHouseQueryConfig(
                sql="SELECT value FROM t WHERE day = {day:String}",
            ),
        },
        ingest=IngestSpec(("analytics", "events_raw")),
    )
    return ClickHouseAnalyticsAdapter(client=mock, spec=spec, config=config)


class _MockClient:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.params_seen: list[Any] = []
        self.databases_seen: list[str | None] = []
        self.inserts: list[list[dict[str, Any]]] = []

    async def run_query(
        self,
        sql: str,
        params: BaseModel | dict[str, Any] | None = None,
        *,
        database: str | None = None,
        max_rows: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
        timeout: int | None = None,
    ) -> ClickHouseQueryResult:
        _ = max_rows, limit, offset, timeout
        self.databases_seen.append(database)
        self.queries.append(sql)
        self.params_seen.append(params)
        if "count()" in sql.lower():
            return ClickHouseQueryResult(rows=[{"forze_cnt": 2}], row_count=1)
        return ClickHouseQueryResult(
            rows=[{"value": 10}, {"value": 20}],
            row_count=2,
        )

    async def run_query_all_pages(
        self,
        sql: str,
        params: BaseModel | None = None,
        *,
        database: str | None = None,
        max_rows: int | None = None,
        timeout: int | None = None,
        fetch_batch_size: int = 2000,
    ) -> list[dict[str, Any]]:
        _ = database, max_rows, timeout, fetch_batch_size
        result = await self.run_query(sql, params)
        return result.rows

    async def run_query_streamed(
        self,
        sql: str,
        params: BaseModel | None = None,
        *,
        database: str | None = None,
        max_rows: int | None = None,
        timeout: int | None = None,
        fetch_batch_size: int = 2000,
    ) -> Any:
        _ = database, max_rows, timeout
        result = await self.run_query(sql, params)
        rows = result.rows
        for start in range(0, len(rows), fetch_batch_size):
            yield rows[start : start + fetch_batch_size]

    async def insert_rows(
        self,
        database: str,
        table: str,
        rows: list[dict[str, Any]],
        *,
        timeout: int | None = None,
    ) -> ClickHouseInsertResult:
        _ = database, table, timeout
        self.inserts.append(rows)
        return ClickHouseInsertResult(accepted=len(rows))


@pytest.mark.asyncio
async def test_run_page_uses_count_wrapper() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)
    page = await adapter.run_page("counts", _Params())
    assert page.count == 2
    assert len(page.hits) == 2
    assert any("count()" in q.lower() for q in mock.queries)


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
    assert all("count()" not in q.lower() for q in mock.queries)


@pytest.mark.asyncio
async def test_run_cursor_exposes_next_token() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)
    page = await adapter.run_cursor("counts", _Params(), cursor={"limit": 1})
    assert page.has_more is True
    assert page.next_cursor is not None


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
) -> ClickHouseAnalyticsAdapter[_Row, _Ingest]:
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
    )
    config = ClickHouseAnalyticsConfig(
        database="analytics",
        tenant_aware=True,
        queries={
            "counts": ClickHouseQueryConfig(
                sql="SELECT value FROM t WHERE day = {day:String} AND tenant_id = {tenant:UUID}",
            ),
        },
    )
    return ClickHouseAnalyticsAdapter(
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
    assert bound["day"] == "2026-01-01"


@pytest.mark.asyncio
async def test_tenant_aware_fails_closed_without_tenant() -> None:
    from forze.base.exceptions import CoreException

    mock = _MockClient()
    adapter = _tenant_adapter(mock, lambda: None)

    with pytest.raises(CoreException, match="tenant_required"):
        await adapter.run("counts", _Params())

    assert mock.queries == []  # never reached the client


@pytest.mark.asyncio
async def test_not_tenant_aware_binds_no_tenant_param() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)  # default config: tenant_aware=False

    await adapter.run("counts", _Params())

    bound = mock.params_seen[-1]
    # params pass through as the model (or a dict without a tenant key)
    seen = bound.model_dump() if isinstance(bound, BaseModel) else bound
    assert "tenant" not in seen


def test_tenant_aware_config_rejects_unscoped_sql() -> None:
    from forze.base.exceptions import CoreException

    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
    )
    config = ClickHouseAnalyticsConfig(
        database="analytics",
        tenant_aware=True,
        queries={
            # SQL never references {tenant:...} — must be rejected at wiring.
            "counts": ClickHouseQueryConfig(sql="SELECT value FROM t"),
        },
    )

    with pytest.raises(CoreException, match="analytics_tenant_param_unreferenced"):
        config.validate_against_spec(spec)


# ----------------------- #
# per-tenant query database (namespace routing)


@pytest.mark.asyncio
async def test_query_database_static_overrides_default() -> None:
    mock = _MockClient()
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
    )
    config = ClickHouseAnalyticsConfig(
        database="default_db",
        query_database="tenant_db",
        queries={"counts": ClickHouseQueryConfig(sql="SELECT value FROM t")},
    )
    adapter = ClickHouseAnalyticsAdapter(client=mock, spec=spec, config=config)

    await adapter.run("counts", _Params())

    assert mock.databases_seen[-1] == "tenant_db"


@pytest.mark.asyncio
async def test_query_database_resolver_uses_bound_tenant() -> None:
    from uuid import uuid4

    from forze.application.contracts.tenancy import TenantIdentity

    tid = uuid4()
    mock = _MockClient()
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
    )
    config = ClickHouseAnalyticsConfig(
        database="default_db",
        query_database=lambda t: f"db_{t}",
        queries={"counts": ClickHouseQueryConfig(sql="SELECT value FROM t")},
    )
    adapter = ClickHouseAnalyticsAdapter(
        client=mock,
        spec=spec,
        config=config,
        tenant_provider=lambda: TenantIdentity(tenant_id=tid),
    )

    await adapter.run("counts", _Params())

    assert mock.databases_seen[-1] == f"db_{tid}"


@pytest.mark.asyncio
async def test_no_query_database_uses_static_default() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)  # config has database="analytics", no query_database

    await adapter.run("counts", _Params())

    assert mock.databases_seen[-1] == "analytics"
