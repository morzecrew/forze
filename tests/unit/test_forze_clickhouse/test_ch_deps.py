"""Tests for ClickHouse deps module and config validation."""

from __future__ import annotations

import pytest

from forze.base.exceptions import CoreException
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
    IngestSpec,
)
from forze.application.execution import ExecutionContext
from forze_clickhouse.execution.deps import (
    ClickHouseAnalyticsConfig,
    ClickHouseDepsModule,
    ClickHouseQueryConfig,
    ConfigurableClickHouseAnalytics,
)
from forze_clickhouse.kernel.client import ClickHouseClient


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
