"""Tests for Postgres deps module and analytics config validation."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze_postgres import PostgresClient, PostgresDepsModule
from forze_postgres.execution.deps.configs import (
    PostgresAnalyticsConfig,
    PostgresQueryConfig,
)


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
    config = PostgresAnalyticsConfig(
        queries={},
        ingest_table="t",
    )
    with pytest.raises(CoreException, match="missing query keys"):
        config.validate_against_spec(spec)


def test_deps_module_registers_analytics_keys() -> None:
    client = PostgresClient()
    module = PostgresDepsModule(
        client=client,
        analytics={
            "events": PostgresAnalyticsConfig(
                queries={"counts": PostgresQueryConfig(sql="SELECT 1 AS value")},
                ingest_table="events",
            ),
        },
    )
    deps = module()
    ctx = context_from_deps(deps)
    spec = _spec()
    assert ctx.analytics.query(spec) is not None
    assert ctx.analytics.ingest(spec) is not None


def test_required_database_isolation_rejects_shared_client_with_analytics() -> None:
    # A tenant-aware analytics route on a shared client derives "row"; a declared
    # "database" floor rejects it at wiring.
    with pytest.raises(CoreException, match="postgres_tenancy_validation_failed"):
        PostgresDepsModule(
            client=PostgresClient(),
            required_tenant_isolation="database",
            analytics={
                "events": PostgresAnalyticsConfig(
                    tenant_aware=True,
                    queries={
                        "counts": PostgresQueryConfig(
                            sql="SELECT 1 AS value WHERE tenant_id = %(tenant)s",
                        ),
                    },
                ),
            },
        )


def test_outbox_route_is_validated_against_isolation_floor() -> None:
    # A tenant-aware outbox route is now included in tenancy validation (was excluded):
    # a "database" floor on a shared client rejects it.
    from forze_postgres.execution.deps.configs import PostgresOutboxConfig

    with pytest.raises(CoreException, match="postgres_tenancy_validation_failed"):
        PostgresDepsModule(
            client=PostgresClient(),
            required_tenant_isolation="database",
            outboxes={
                "events": PostgresOutboxConfig(
                    relation=("public", "outbox"),
                    tenant_aware=True,
                ),
            },
        )
