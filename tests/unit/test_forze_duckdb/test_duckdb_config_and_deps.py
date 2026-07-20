"""Tests for DuckDbAnalyticsConfig validation and the DuckDbDepsModule wiring."""

from __future__ import annotations

from typing import Any

import pytest

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from forze.base.exceptions import CoreException
from forze_duckdb import (
    DuckDbAnalyticsConfig,
    DuckDbConfig,
    DuckDbDepsModule,
    DuckDbQueryConfig,
)
from forze_duckdb.execution.deps import ConfigurableDuckDbAnalytics
from forze_duckdb.kernel.client import DuckDbClient
from tests.support.execution_context import context_from_deps
from tests.unit.test_forze_duckdb.conftest import Params, Row

# ----------------------- #


def _spec(*, ingest: type | None = None) -> AnalyticsSpec[Row, Any]:
    return AnalyticsSpec(
        name="events",
        read=Row,
        queries={"by_day": AnalyticsQueryDefinition(params=Params)},
        ingest=ingest,
    )


# ....................... #


def test_validate_against_spec_passes_on_match() -> None:
    cfg = DuckDbAnalyticsConfig(queries={"by_day": DuckDbQueryConfig(sql="SELECT 1")})

    cfg.validate_against_spec(_spec())  # no raise


# ....................... #


def test_validate_against_spec_missing_key() -> None:
    cfg = DuckDbAnalyticsConfig(queries={"other": DuckDbQueryConfig(sql="SELECT 1")})

    with pytest.raises(CoreException, match="missing query keys"):
        cfg.validate_against_spec(_spec())


# ....................... #


def test_validate_against_spec_extra_key() -> None:
    cfg = DuckDbAnalyticsConfig(
        queries={
            "by_day": DuckDbQueryConfig(sql="SELECT 1"),
            "extra": DuckDbQueryConfig(sql="SELECT 2"),
        }
    )

    with pytest.raises(CoreException, match="unknown query keys"):
        cfg.validate_against_spec(_spec())


# ....................... #


def test_ingest_spec_is_rejected_query_only() -> None:
    cfg = DuckDbAnalyticsConfig(queries={"by_day": DuckDbQueryConfig(sql="SELECT 1")})

    with pytest.raises(CoreException, match="query-only"):
        cfg.validate_against_spec(_spec(ingest=Row))


# ....................... #


def test_empty_query_sql_rejected() -> None:
    with pytest.raises(CoreException, match="non-empty"):
        DuckDbQueryConfig(sql="   ")


# ....................... #


def test_config_rejects_bad_concurrency() -> None:
    with pytest.raises(CoreException, match="max_concurrent_queries"):
        DuckDbConfig(max_concurrent_queries=0)


# ....................... #


def test_factory_rejects_mapping_config() -> None:
    with pytest.raises(TypeError, match="DuckDbAnalyticsConfig"):
        ConfigurableDuckDbAnalytics(config={"queries": {}})  # type: ignore[arg-type]


# ....................... #


def test_deps_module_resolves_analytics_query_adapter() -> None:
    client = DuckDbClient()
    module = DuckDbDepsModule(
        client=client,
        analytics={
            "events": DuckDbAnalyticsConfig(
                queries={"by_day": DuckDbQueryConfig(sql="SELECT 1")},
            )
        },
    )

    ctx = context_from_deps(module())

    assert ctx.analytics.query(_spec()) is not None


def test_dedicated_isolation_floor_always_fails_for_duckdb() -> None:
    # DuckDB is in-process with no per-tenant routing, so it can never satisfy a
    # "dedicated" isolation floor — wiring fails closed by design.
    with pytest.raises(CoreException, match="duckdb_analytics_tenancy_validation_failed"):
        DuckDbDepsModule(
            client=DuckDbClient(),
            required_tenant_isolation="dedicated",
            analytics={
                "events": DuckDbAnalyticsConfig(
                    queries={"by_day": DuckDbQueryConfig(sql="SELECT 1")},
                )
            },
        )
