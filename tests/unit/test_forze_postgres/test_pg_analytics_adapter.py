"""Tests for PostgresAnalyticsAdapter with a mocked client."""

from __future__ import annotations

from datetime import timedelta
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
from tests.support.analytics_options import (
    assert_every_path_refuses_an_unknown_option,
    assert_every_read_is_bounded_by_the_route_timeout,
)


class _Row(BaseModel):
    value: int


class _Params(BaseModel):
    day: str = "2026-01-01"


class _Ingest(BaseModel):
    event: str


def _adapter(mock: Any, **query: Any) -> PostgresAnalyticsAdapter[_Row, _Ingest]:
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
                **query,
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
        self.copies: list[tuple[Any, list[Any], list[Any]]] = []
        self.fetch_one_calls: list[tuple[Any, Any]] = []
        self.timeouts: list[Any] = []
        self.search_paths: list[str | None] = []
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

    async def fetch_all_scoped(
        self,
        query: str,
        params: dict[str, object] | None = None,
        *,
        statement_timeout: Any = None,
        search_path: str | None = None,
    ) -> list[dict[str, Any]]:
        self.timeouts.append(statement_timeout)
        self.search_paths.append(search_path)
        return await self.fetch_all(query, params)

    async def fetch_one(
        self,
        query: Any,
        params: Any = None,
        **kwargs: Any,
    ) -> tuple[str, ...]:
        _ = kwargs
        self.fetch_one_calls.append((query, params))
        # Capture of current settings: one prior value per requested name.
        return tuple(f"prior_{name}" for name in (params or ()))

    async def execute(
        self,
        query: Any,
        params: Any = None,
        **kwargs: Any,
    ) -> None:
        _ = kwargs
        self.executes.append((query, params))

    async def copy_rows(
        self,
        target: Any,
        columns: Any,
        rows: Any,
        **kwargs: Any,
    ) -> int:
        _ = kwargs
        materialized = list(rows)
        self.copies.append((target, list(columns), materialized))

        return len(materialized)


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

    # One load into the configured relation. Asserting the target and the row rather than
    # "one statement ran" — the previous form counted `execute` calls, which described the
    # multi-VALUES INSERT this now replaces instead of what `append` promises.
    assert len(mock.copies) == 1

    target, columns, rows = mock.copies[0]

    assert target == ("public", "events_raw")
    assert list(rows) == [tuple(row[column] for column in columns) for row in [{"event": "signup"}]]


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
    # The client puts the schema first and keeps `public` after it (session_settings tests).
    assert mock.search_paths == [f"tenant_{str(tid).replace('-', '')}"]


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
async def test_every_read_is_bounded_by_the_route_timeout() -> None:
    from datetime import timedelta

    mock = _MockClient()

    await assert_every_read_is_bounded_by_the_route_timeout(
        _adapter(mock, timeout=timedelta(seconds=7)),
        "counts",
        _Params(),
        route=timedelta(seconds=7),
        seen=mock.timeouts,
    )


@pytest.mark.asyncio
async def test_a_read_with_no_option_gets_the_60_second_default() -> None:
    from datetime import timedelta

    mock = _MockClient()

    await _adapter(mock).run("counts", _Params())

    assert mock.timeouts == [timedelta(seconds=60)]
    assert mock.search_paths == [None]


@pytest.mark.asyncio
async def test_a_per_call_timeout_below_a_millisecond_is_refused() -> None:
    from datetime import timedelta

    with pytest.raises(CoreException) as caught:
        await _adapter(_MockClient()).run(
            "counts", _Params(), options={"timeout": timedelta(microseconds=500)}
        )

    assert caught.value.code == "analytics_option_invalid"


async def test_every_path_refuses_an_unknown_option() -> None:
    await assert_every_path_refuses_an_unknown_option(_adapter(_MockClient()), "counts", _Params())


@pytest.mark.parametrize(
    "bad",
    [timedelta(milliseconds=2**31), timedelta(days=30), timedelta(microseconds=500)],
)
def test_a_timeout_postgres_cannot_hold_is_refused(bad: timedelta) -> None:
    # statement_timeout is an int of milliseconds: above INT_MAX every read would fail at
    # set_config, and below 1 ms it would round up.
    with pytest.raises(CoreException) as caught:
        PostgresQueryConfig(sql="SELECT 1", timeout=bad)

    assert caught.value.code == "analytics_timeout_invalid"


def test_the_longest_postgres_timeout_is_accepted() -> None:
    longest = timedelta(milliseconds=2**31 - 1)

    assert PostgresQueryConfig(sql="SELECT 1", timeout=longest).timeout == longest
