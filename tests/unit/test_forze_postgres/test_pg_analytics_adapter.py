"""Tests for PostgresAnalyticsAdapter with a mocked client."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
    IngestSpec,
)
from forze.base.exceptions import CoreException
from forze_postgres.adapters.analytics import PostgresAnalyticsAdapter
from forze_postgres.execution.deps.configs import PostgresAnalyticsConfig, PostgresQueryConfig


class _Row(BaseModel):
    value: int


class _Params(BaseModel):
    day: str = "2026-01-01"


class _Ingest(BaseModel):
    event: str


def _adapter(mock: Any) -> PostgresAnalyticsAdapter[_Row, _Ingest]:
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
        ingest=_Ingest,
    )
    config = PostgresAnalyticsConfig(
        queries={
            "counts": PostgresQueryConfig(
                sql="SELECT value FROM t WHERE day = %(day)s",
            ),
        },
        ingest=IngestSpec(("public", "events_raw")),
    )
    return PostgresAnalyticsAdapter(client=mock, spec=spec, config=config)


class _MockClient:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.params_seen: list[Any] = []
        self.executes: list[tuple[Any, Any]] = []
        self._in_tx = False

    def is_in_transaction(self) -> bool:
        return self._in_tx

    def transaction(self) -> Any:
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _tx() -> Any:
            self._in_tx = True
            try:
                yield self
            finally:
                self._in_tx = False

        return _tx()

    async def fetch_all(
        self,
        query: str,
        params: dict[str, object] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        _ = kwargs
        self.queries.append(query)
        self.params_seen.append(params)
        if "COUNT(*)" in query:
            return [{"forze_cnt": 2}]
        return [{"value": 10}, {"value": 20}]

    async def execute(
        self,
        query: Any,
        params: Any = None,
        **kwargs: Any,
    ) -> None:
        _ = kwargs
        self.executes.append((query, params))


@pytest.mark.asyncio
async def test_run_page_uses_count_wrapper() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)
    page = await adapter.run_page("counts", _Params())
    assert page.count == 2
    assert len(page.hits) == 2
    assert any("COUNT(*)" in q for q in mock.queries)


@pytest.mark.asyncio
async def test_run_cursor_exposes_next_token() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)
    page = await adapter.run_cursor("counts", _Params(), cursor={"limit": 1})
    assert page.has_more is True
    assert page.next_cursor is not None


@pytest.mark.asyncio
async def test_run_cursor_dry_run_returns_empty() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)
    page = await adapter.run_cursor(
        "counts",
        _Params(),
        cursor={"limit": 5},
        options={"dry_run": True},
    )
    assert page.hits == []
    assert page.next_cursor is None
    assert page.has_more is False
    assert mock.queries == []


@pytest.mark.asyncio
async def test_run_cursor_keyset_uses_cursor_column() -> None:
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"by_value": AnalyticsQueryDefinition(params=_Params)},
        ingest=_Ingest,
    )
    config = PostgresAnalyticsConfig(
        queries={
            "by_value": PostgresQueryConfig(
                sql="SELECT value FROM t WHERE value > %(forze_after)s",
                cursor_column="value",
            ),
        },
        ingest=IngestSpec(("public", "events_raw")),
    )
    mock = _MockClient()
    adapter = PostgresAnalyticsAdapter(client=mock, spec=spec, config=config)
    page = await adapter.run_cursor(
        "by_value",
        _Params(),
        cursor={"limit": 1},
    )
    assert len(page.hits) >= 1
    assert page.next_cursor is not None


@pytest.mark.asyncio
async def test_append_ingest() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)
    result = await adapter.append([_Ingest(event="signup")])
    assert result is not None
    assert result.accepted == 1
    assert len(mock.executes) == 1


@pytest.mark.asyncio
async def test_unknown_query_key_raises() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)
    with pytest.raises(CoreException, match="Unknown analytics query key"):
        await adapter.run("missing", _Params())


# ----------------------- #
# tenant advisory floor


def _tenant_adapter(
    mock: Any,
    tenant_provider: Any,
) -> PostgresAnalyticsAdapter[_Row, _Ingest]:
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
    )
    config = PostgresAnalyticsConfig(
        tenant_aware=True,
        queries={
            "counts": PostgresQueryConfig(
                sql="SELECT value FROM t WHERE day = %(day)s AND tenant_id = %(tenant)s",
            ),
        },
    )
    return PostgresAnalyticsAdapter(
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
    mock = _MockClient()
    adapter = _tenant_adapter(mock, lambda: None)

    with pytest.raises(CoreException, match="tenant_required"):
        await adapter.run("counts", _Params())

    assert mock.queries == []


def test_tenant_aware_config_rejects_unscoped_sql() -> None:
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
    )
    config = PostgresAnalyticsConfig(
        tenant_aware=True,
        queries={"counts": PostgresQueryConfig(sql="SELECT value FROM t")},
    )

    with pytest.raises(CoreException, match="analytics_tenant_param_unreferenced"):
        config.validate_against_spec(spec)


# ----------------------- #
# per-tenant query schema (search_path namespace routing)


def _rendered(query: Any) -> str:
    try:
        return query.as_string(None)
    except Exception:  # noqa: BLE001 - best-effort for assertions
        return str(query)


@pytest.mark.asyncio
async def test_query_schema_sets_search_path_per_tenant() -> None:
    from uuid import uuid4

    from forze.application.contracts.tenancy import TenantIdentity

    tid = uuid4()
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
    )
    config = PostgresAnalyticsConfig(
        query_schema=lambda t: f"tenant_{str(t).replace('-', '')}",
        queries={"counts": PostgresQueryConfig(sql="SELECT value FROM t")},
    )
    mock = _MockClient()
    adapter = PostgresAnalyticsAdapter(
        client=mock,
        spec=spec,
        config=config,
        tenant_provider=lambda: TenantIdentity(tenant_id=tid),
    )

    page = await adapter.run("counts", _Params())

    assert len(page.hits) == 2
    set_calls = [_rendered(q) for q, _ in mock.executes]
    expected = f"tenant_{str(tid).replace('-', '')}"
    # search_path must scope to the tenant schema FIRST, then keep `public` reachable so
    # unqualified extension objects / shared lookups don't break at query time.
    search_path = next(s for s in set_calls if "search_path" in s)
    assert expected in search_path
    assert "public" in search_path
    assert search_path.index(expected) < search_path.index("public")


@pytest.mark.asyncio
async def test_tenant_aware_query_schema_fails_closed_without_tenant() -> None:
    # A tenant-aware route with a dynamic query_schema and no bound tenant must fail closed
    # with `tenant_required` — the resolver is never invoked with None (it would crash here).
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
    )
    config = PostgresAnalyticsConfig(
        tenant_aware=True,
        query_schema=lambda t: f"tenant_{t.hex}",  # AttributeError if called with None
        queries={
            "counts": PostgresQueryConfig(sql="SELECT value FROM t WHERE x = %(tenant)s"),
        },
    )
    adapter = PostgresAnalyticsAdapter(
        client=_MockClient(),
        spec=spec,
        config=config,
        tenant_provider=lambda: None,
    )
    with pytest.raises(CoreException, match="tenant_required"):
        await adapter.run("counts", _Params())


@pytest.mark.asyncio
async def test_no_query_schema_does_not_open_transaction() -> None:
    mock = _MockClient()
    adapter = _adapter(mock)  # no query_schema, no timeout

    await adapter.run("counts", _Params())

    assert mock.executes == []  # no SET LOCAL search_path
