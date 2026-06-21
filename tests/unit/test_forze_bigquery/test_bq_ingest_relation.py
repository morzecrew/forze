"""Unit tests for BigQuery analytics ingest relation resolvers."""

from __future__ import annotations

from uuid import uuid4

import pytest


@pytest.mark.asyncio
async def test_analytics_adapter_resolves_dynamic_ingest_relation() -> None:
    from unittest.mock import MagicMock

    from pydantic import BaseModel

    from forze.application.contracts.analytics import (
        AnalyticsQueryDefinition,
        AnalyticsSpec,
        IngestSpec,
    )
    from forze_bigquery.adapters import BigQueryAnalyticsAdapter
    from forze_bigquery.execution.deps.configs import (
        BigQueryAnalyticsConfig,
        BigQueryQueryConfig,
    )

    class _Row(BaseModel):
        value: int

    class _Params(BaseModel):
        day: str = "2026-01-01"

    tid = uuid4()
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
    )
    config = BigQueryAnalyticsConfig(
        dataset="analytics",
        queries={
            "counts": BigQueryQueryConfig(sql="SELECT 1"),
        },
        ingest=IngestSpec(lambda t: (f"t_{t}", "raw") if t else ("analytics", "raw")),
    )
    adapter = BigQueryAnalyticsAdapter(
        client=MagicMock(),
        spec=spec,
        config=config,
        tenant_provider=lambda: type("T", (), {"tenant_id": tid})(),
    )

    dataset, table = await adapter._resolved_ingest_target()

    assert dataset == f"t_{tid}"
    assert table == "raw"
