"""Tests for Postgres deps module and analytics config validation."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze_postgres import PostgresClient, PostgresDepsModule
from forze_postgres.execution.deps import validate_postgres_analytics_config


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
    config = {
        "queries": {},
        "ingest_table": "t",
    }
    with pytest.raises(CoreException, match="missing query keys"):
        validate_postgres_analytics_config(spec, config)


def test_deps_module_registers_analytics_keys() -> None:
    client = PostgresClient()
    module = PostgresDepsModule(
        client=client,
        analytics={
            "events": {
                "queries": {"counts": {"sql": "SELECT 1 AS value"}},
                "ingest_table": "events",
            },
        },
    )
    deps = module()
    ctx = ExecutionContext(deps=deps)
    spec = _spec()
    assert ctx.analytics.query(spec) is not None
    assert ctx.analytics.ingest(spec) is not None
