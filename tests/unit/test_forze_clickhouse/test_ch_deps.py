"""Tests for ClickHouse deps module and config validation."""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
    IngestSpec,
)
from forze.base.exceptions import CoreException
from forze_clickhouse.execution.deps import (
    ClickHouseAnalyticsConfig,
    ClickHouseDepsModule,
    ClickHouseQueryConfig,
    ConfigurableClickHouseAnalytics,
)
from forze_clickhouse.kernel.client import (
    ClickHouseClient,
    ClickHouseConfig,
    RoutedClickHouseClient,
)
from tests.support.execution_context import (
    context_from_deps,
)


def test_rejects_mapping_config() -> None:
    with pytest.raises(TypeError, match="ClickHouseAnalyticsConfig"):
        ConfigurableClickHouseAnalytics(config={"database": "db", "queries": {}})


class _Row(BaseModel):
    value: int


class _Params(BaseModel):
    pass


def _spec() -> AnalyticsSpec[_Row, _Row]:
    return AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
        ingest=_Row,
    )


def test_validate_missing_query_key() -> None:
    spec = _spec()
    config = ClickHouseAnalyticsConfig(
        database="analytics",
        queries={},
        ingest=IngestSpec(("analytics", "t")),
    )
    with pytest.raises(CoreException, match="missing query keys"):
        config.validate_against_spec(spec)


def test_deps_module_registers_analytics_keys() -> None:
    client = ClickHouseClient()
    module = ClickHouseDepsModule(
        client=client,
        analytics={
            "events": ClickHouseAnalyticsConfig(
                database="analytics",
                queries={"counts": ClickHouseQueryConfig(sql="SELECT 1 AS value")},
                ingest=IngestSpec(("analytics", "events")),
            ),
        },
    )
    deps = module()
    ctx = context_from_deps(deps)
    spec = _spec()
    assert ctx.analytics.query(spec) is not None
    assert ctx.analytics.ingest(spec) is not None


def test_required_dedicated_isolation_rejects_shared_client() -> None:
    # A shared (non-routed) client cannot satisfy a declared "dedicated" isolation floor.
    with pytest.raises(CoreException, match="clickhouse_analytics_tenancy_validation_failed"):
        ClickHouseDepsModule(
            client=ClickHouseClient(),
            analytics={
                "events": ClickHouseAnalyticsConfig(
                    database="analytics",
                    tenant_aware=True,
                    queries={
                        "counts": ClickHouseQueryConfig(
                            sql="SELECT 1 AS value WHERE tenant_id = {tenant:UUID}",
                        ),
                    },
                ),
            },
            required_tenant_isolation="dedicated",
        )


def test_no_isolation_floor_allows_shared_client() -> None:
    # Default (no declared floor) — shared client is fine.
    ClickHouseDepsModule(
        client=ClickHouseClient(),
        analytics={
            "events": ClickHouseAnalyticsConfig(
                database="analytics",
                queries={"counts": ClickHouseQueryConfig(sql="SELECT 1 AS value")},
            ),
        },
    )


def test_namespace_floor_satisfied_by_per_tenant_query_database() -> None:
    # A dynamic (per-tenant) query_database resolver derives the "namespace" tier on a shared
    # client, satisfying a "namespace" floor.
    ClickHouseDepsModule(
        client=ClickHouseClient(),
        required_tenant_isolation="namespace",
        analytics={
            "events": ClickHouseAnalyticsConfig(
                database="analytics",
                query_database=lambda t: f"tenant_{t}",
                queries={"counts": ClickHouseQueryConfig(sql="SELECT 1 AS value")},
            ),
        },
    )


def test_namespace_floor_rejects_static_query_database() -> None:
    with pytest.raises(CoreException, match="clickhouse_analytics_tenancy_validation_failed"):
        ClickHouseDepsModule(
            client=ClickHouseClient(),
            required_tenant_isolation="namespace",
            analytics={
                "events": ClickHouseAnalyticsConfig(
                    database="analytics",
                    tenant_aware=True,
                    queries={
                        "counts": ClickHouseQueryConfig(
                            sql="SELECT 1 AS value WHERE tenant_id = {tenant:UUID}",
                        ),
                    },
                ),
            },
        )


@pytest.mark.parametrize("static", [None, "query_database", "ingest"])
def test_namespace_floor_needs_every_database_per_tenant(static: str | None) -> None:
    # Unset, the queries fall back to the static ``database``: one place for every tenant.
    def module(*, floor: str | None) -> ClickHouseDepsModule:
        return ClickHouseDepsModule(
            client=ClickHouseClient(),
            required_tenant_isolation=floor,  # type: ignore[arg-type]
            analytics={
                "events": ClickHouseAnalyticsConfig(
                    database="analytics",
                    queries={"counts": ClickHouseQueryConfig(sql="SELECT 1 AS value")},
                    query_database=None if static == "query_database" else lambda t: f"t_{t}",
                    ingest=(
                        ("analytics", "events")
                        if static == "ingest"
                        else lambda t: (f"t_{t}", "events")
                    ),
                ),
            },
        )

    if static is None:
        assert module(floor="namespace") is not None
        return

    with pytest.raises(CoreException, match="clickhouse_analytics_tenancy_validation_failed"):
        module(floor="namespace")

    # The control: with no floor declared the same wiring stands, so the refusal is the floor's.
    assert module(floor=None) is not None


# ....................... #


def _route(timeout: timedelta) -> ClickHouseAnalyticsConfig:
    return ClickHouseAnalyticsConfig(
        database="analytics",
        queries={"counts": ClickHouseQueryConfig(sql="SELECT 1 AS value", timeout=timeout)},
    )


class _FakeAsyncClient:
    async def close(self) -> None:
        return None


async def _started_client(timeout: timedelta) -> tuple[ClickHouseClient, dict[str, Any]]:
    seen: dict[str, Any] = {}

    async def _create(**kwargs: Any) -> _FakeAsyncClient:
        seen.update(kwargs)
        return _FakeAsyncClient()

    client = ClickHouseClient()

    with patch("forze_clickhouse.kernel.client.client.create_async_client", _create):
        await client.initialize(ClickHouseConfig(timeout=timeout))

    return client, seen


@pytest.mark.asyncio
async def test_the_transport_outwaits_the_client_timeout() -> None:
    # The server's TIMEOUT_EXCEEDED must arrive before the socket gives up.
    client, seen = await _started_client(timedelta(seconds=60))

    assert seen["send_receive_timeout"] == 65
    assert seen["connect_timeout"] == 60
    await client.close()


@pytest.mark.asyncio
async def test_a_route_longer_than_the_client_waits_is_refused_at_wiring() -> None:
    client, _ = await _started_client(timedelta(seconds=60))

    ClickHouseDepsModule(client=client, analytics={"ok": _route(timedelta(seconds=60))})

    with pytest.raises(CoreException) as caught:
        ClickHouseDepsModule(client=client, analytics={"long": _route(timedelta(minutes=10))})

    assert caught.value.code == "clickhouse_read_timeout_too_short"
    assert "'long'" in str(caught.value)
    assert "Raise ClickHouseConfig.timeout" in str(caught.value)
    await client.close()


@pytest.mark.asyncio
async def test_a_client_configured_at_startup_is_checked_when_the_route_resolves() -> None:
    # Unstarted, the client has no config to check against; resolving the route does it.
    client = ClickHouseClient()
    module = ClickHouseDepsModule(
        client=client, analytics={"events": _route(timedelta(minutes=10))}
    )

    with patch(
        "forze_clickhouse.kernel.client.client.create_async_client",
        AsyncMock(return_value=_FakeAsyncClient()),
    ):
        await client.initialize(ClickHouseConfig(timeout=timedelta(seconds=60)))

    with pytest.raises(CoreException) as caught:
        context_from_deps(module()).analytics.query(
            AnalyticsSpec(
                name="events",
                read=_Row,
                queries={"counts": AnalyticsQueryDefinition(params=_Params)},
            )
        )

    assert caught.value.code == "clickhouse_read_timeout_too_short"
    await client.close()


@pytest.mark.parametrize(
    ("defaults", "refused"),
    [(None, True), (ClickHouseConfig(timeout=timedelta(minutes=10)), False)],
)
def test_a_routed_client_is_checked_against_the_config_its_tenant_clients_use(
    defaults: ClickHouseConfig | None, refused: bool
) -> None:
    routed = RoutedClickHouseClient(
        secrets=MagicMock(),
        secret_ref_for_tenant=lambda _tid: MagicMock(),
        tenant_provider=lambda: None,
        connection_defaults=defaults,
    )

    def wire() -> None:
        ClickHouseDepsModule(client=routed, analytics={"long": _route(timedelta(minutes=10))})

    if refused:
        with pytest.raises(CoreException, match="clickhouse_read_timeout_too_short"):
            wire()

    else:
        wire()


@pytest.mark.parametrize("bad", [timedelta(milliseconds=500), timedelta(seconds=1.5), timedelta(0)])
def test_a_timeout_that_is_not_whole_seconds_is_refused(bad: timedelta) -> None:
    # max_execution_time counts whole seconds; 500 ms would be sent as a looser 1 s.
    with pytest.raises(CoreException) as caught:
        ClickHouseQueryConfig(sql="SELECT 1", timeout=bad)

    assert caught.value.code == "analytics_timeout_invalid"
