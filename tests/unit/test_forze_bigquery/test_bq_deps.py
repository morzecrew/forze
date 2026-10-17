"""Tests for BigQuery deps module and config validation."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
    IngestSpec,
)
from forze.base.exceptions import CoreException
from forze_bigquery.execution.deps import (
    BigQueryAnalyticsConfig,
    BigQueryDepsModule,
    BigQueryQueryConfig,
    ConfigurableBigQueryAnalytics,
)
from forze_bigquery.kernel.client import BigQueryClient
from tests.support.execution_context import (
    context_from_deps,
)


def test_rejects_mapping_config() -> None:
    with pytest.raises(TypeError, match="BigQueryAnalyticsConfig"):
        ConfigurableBigQueryAnalytics(config={"dataset": "d", "queries": {}})


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
        ingest=IngestSpec(("ds", "t")),
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
                ingest=IngestSpec(("analytics", "events")),
            ),
        },
    )
    deps = module()
    ctx = context_from_deps(deps)
    spec = _spec()
    assert ctx.analytics.query(spec) is not None
    assert ctx.analytics.ingest(spec) is not None


@pytest.mark.parametrize("static", [None, "query_dataset", "ingest"])
def test_namespace_floor_needs_every_dataset_per_tenant(static: str | None) -> None:
    # Unset, the queries read whatever their SQL qualifies: one place for every tenant.
    def module(*, floor: str | None) -> BigQueryDepsModule:
        return BigQueryDepsModule(
            client=BigQueryClient(),
            required_tenant_isolation=floor,  # type: ignore[arg-type]
            analytics={
                "events": BigQueryAnalyticsConfig(
                    dataset="analytics",
                    queries={"counts": BigQueryQueryConfig(sql="SELECT 1 AS value")},
                    query_dataset=None if static == "query_dataset" else lambda t: f"t_{t}",
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

    with pytest.raises(CoreException, match="bigquery_analytics_tenancy_validation_failed"):
        module(floor="namespace")

    # The control: with no floor declared the same wiring stands, so the refusal is the floor's.
    assert module(floor=None) is not None
