"""Unit tests for ClickHouse analytics ingest relation resolvers."""

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
    from forze_clickhouse.adapters import ClickHouseAnalyticsAdapter
    from forze_clickhouse.execution.deps.configs import (
        ClickHouseAnalyticsConfig,
        ClickHouseQueryConfig,
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
    config = ClickHouseAnalyticsConfig(
        database="analytics",
        queries={
            "counts": ClickHouseQueryConfig(sql="SELECT 1"),
        },
        ingest=IngestSpec(lambda t: (f"t_{t}", "raw") if t else ("analytics", "raw")),
    )
    adapter = ClickHouseAnalyticsAdapter(
        client=MagicMock(),
        spec=spec,
        config=config,
        tenant_provider=lambda: type("T", (), {"tenant_id": tid})(),
    )

    database, table = await adapter._resolved_ingest_target()

    assert database == f"t_{tid}"
    assert table == "raw"
