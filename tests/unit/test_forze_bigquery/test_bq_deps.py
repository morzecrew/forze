"""Tests for BigQuery deps module and config validation."""

from __future__ import annotations

import pytest

from forze.base.exceptions import CoreException
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from forze.application.execution import ExecutionContext
from forze_bigquery.execution.deps import (
    BigQueryAnalyticsConfig,
    BigQueryDepsModule,
    BigQueryQueryConfig,
)
from forze_bigquery.kernel.platform import BigQueryClient


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
    config = BigQueryAnalyticsConfig(
        dataset="ds",
        queries={},
        ingest_table="t",
    )
    with pytest.raises(CoreException, match="missing query keys"):
        config.validate_against_spec(spec)


def test_deps_module_registers_analytics_keys() -> None:
    client = BigQueryClient()
    module = BigQueryDepsModule(
        client=client,
        analytics={
            "events": BigQueryAnalyticsConfig(
                dataset="analytics",
                queries={"counts": BigQueryQueryConfig(sql="SELECT 1 AS value")},
                ingest_table="events",
            ),
        },
    )
    deps = module()
    ctx = context_from_deps(deps)
    spec = _spec()
    assert ctx.analytics.query(spec) is not None
    assert ctx.analytics.ingest(spec) is not None
