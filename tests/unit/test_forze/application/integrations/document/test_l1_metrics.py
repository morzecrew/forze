"""Document L1 metrics exporter: live-store registry → OTel gauges/counters."""

from __future__ import annotations

import gc
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from pydantic import BaseModel

from forze.application.contracts.cache import CacheSpec, L1Spec
from forze.application.integrations.document import (
    DocumentCache,
    instrument_document_l1,
)
from forze.application.integrations.document.observability import (
    DOCUMENT_L1_CAPACITY_GAUGE,
    DOCUMENT_L1_EVICTIONS_COUNTER,
    DOCUMENT_L1_HITS_COUNTER,
    DOCUMENT_L1_MISSES_COUNTER,
    DOCUMENT_L1_SIZE_GAUGE,
)
from tests.unit._gateway_codec_helpers import codec_for

# ----------------------- #

_PK = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


class DocModel(BaseModel):
    id: UUID
    rev: int


_CODEC = codec_for(DocModel)
_DOC = DocModel(id=_PK, rev=1)


def _coord(name: str, capacity: int = 16) -> tuple[DocumentCache[DocModel], AsyncMock]:
    cache = AsyncMock()
    cache.get.return_value = None

    coord: DocumentCache[DocModel] = DocumentCache(
        read_model_type=DocModel,
        read_codec=_CODEC,
        document_name=name,
        cache=cache,
        after_commit=None,
        cache_spec=CacheSpec(
            name="c",
            ttl=timedelta(seconds=300),
            l1=L1Spec(ttl=timedelta(seconds=60), capacity=capacity),
        ),
        tenant_key=lambda: None,
    )

    return coord, cache


async def _read(coord: DocumentCache[DocModel]) -> DocModel:
    async def fetch() -> DocModel:
        return _DOC

    return await coord.get_read_through(
        _PK,
        fetch_on_cache_fault=fetch,
        fetch_on_miss_without_lock=fetch,
    )


def _points(reader: InMemoryMetricReader, name: str) -> dict[str, Any]:
    data = reader.get_metrics_data()
    out: dict[str, Any] = {}

    if data is None:
        return out

    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == name:
                    for dp in metric.data.data_points:
                        out[dict(dp.attributes)["forze.document"]] = dp.value

    return out


# ----------------------- #


class TestL1Metrics:
    async def test_stats_exported_per_document_label(self) -> None:
        reader = InMemoryMetricReader()
        meter = MeterProvider(metric_readers=[reader]).get_meter("test")

        coord, _ = _coord(name="metrics-widgets")
        await _read(coord)  # L1 miss → warm
        await _read(coord)  # L1 hit

        instrument_document_l1(meter=meter)

        assert _points(reader, DOCUMENT_L1_SIZE_GAUGE)["metrics-widgets"] == 1
        assert _points(reader, DOCUMENT_L1_CAPACITY_GAUGE)["metrics-widgets"] == 16
        assert _points(reader, DOCUMENT_L1_HITS_COUNTER)["metrics-widgets"] == 1
        assert _points(reader, DOCUMENT_L1_MISSES_COUNTER)["metrics-widgets"] == 1
        assert _points(reader, DOCUMENT_L1_EVICTIONS_COUNTER)["metrics-widgets"] == 0

    async def test_same_document_name_aggregates(self) -> None:
        reader = InMemoryMetricReader()
        meter = MeterProvider(metric_readers=[reader]).get_meter("test")

        # E.g. the read-only and read-write factories of one spec.
        first, _ = _coord(name="metrics-orders", capacity=8)
        second, _ = _coord(name="metrics-orders", capacity=8)
        await _read(first)
        await _read(second)

        instrument_document_l1(meter=meter)

        assert _points(reader, DOCUMENT_L1_SIZE_GAUGE)["metrics-orders"] == 2
        assert _points(reader, DOCUMENT_L1_CAPACITY_GAUGE)["metrics-orders"] == 16

    async def test_dead_stores_pruned(self) -> None:
        reader = InMemoryMetricReader()
        meter = MeterProvider(metric_readers=[reader]).get_meter("test")

        coord, _ = _coord(name="metrics-ephemeral")
        await _read(coord)

        del coord
        gc.collect()

        instrument_document_l1(meter=meter)

        assert "metrics-ephemeral" not in _points(reader, DOCUMENT_L1_SIZE_GAUGE)
