"""Tenant pool churn counters: created/disposed/evicted stats + OTel export."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from forze.application.contracts.tenancy import TenantClientRegistry, TenantPoolStats
from forze.application.execution.observability import (
    TENANT_POOL_CAPACITY_GAUGE,
    TENANT_POOL_CREATED_COUNTER,
    TENANT_POOL_DISPOSED_COUNTER,
    TENANT_POOL_EVICTED_COUNTER,
    TENANT_POOL_SIZE_GAUGE,
    instrument_tenant_pools,
)

# ----------------------- #


async def _async_return[T](value: T) -> T:
    return value


def _registry(max_entries: int = 2, *, guarded: bool = False) -> TenantClientRegistry[str, str]:
    return TenantClientRegistry(
        max_entries=max_entries,
        create=lambda tid: _async_return(f"client-{tid}"),
        dispose=lambda _c: _async_return(None),
        guarded=guarded,
    )


async def _warm(registry: TenantClientRegistry[str, str], *tids: UUID) -> None:
    for tid in tids:
        registry.set_fingerprint(tid, f"fp-{tid}")
        await registry.get(tid)


_TIDS = [UUID(int=i) for i in range(5)]


# ----------------------- #


class TestPoolStats:
    @pytest.mark.asyncio
    async def test_creations_and_size_counted(self) -> None:
        registry = _registry(max_entries=3)
        await registry.startup()

        await _warm(registry, *_TIDS[:2])
        await registry.get(_TIDS[0])  # cache hit: no new creation

        stats = registry.stats()
        assert stats.created == 2
        assert stats.size == 2
        assert stats.capacity == 3
        assert stats.disposed == 0

    @pytest.mark.asyncio
    async def test_capacity_eviction_shows_as_churn(self) -> None:
        registry = _registry(max_entries=2)
        await registry.startup()

        await _warm(registry, *_TIDS)  # 5 tenants through a 2-slot pool

        stats = registry.stats()
        assert stats.created == 5
        assert stats.disposed == 3  # LRU overflow closed three pools
        assert stats.size == 2  # pinned at capacity: the thrash signature
        assert stats.evicted_explicit == 0

    @pytest.mark.asyncio
    async def test_explicit_eviction_counted_separately(self) -> None:
        registry = _registry()
        await registry.startup()

        await _warm(registry, _TIDS[0])
        await registry.evict(_TIDS[0])  # rotation signal

        stats = registry.stats()
        assert stats.evicted_explicit == 1
        assert stats.disposed == 1
        assert stats.size == 0

    @pytest.mark.asyncio
    async def test_guarded_registry_counts_too(self) -> None:
        registry = _registry(max_entries=1, guarded=True)
        await registry.startup()

        for tid in _TIDS[:3]:
            registry.set_fingerprint(tid, f"fp-{tid}")

            async with registry.use(tid):
                pass

        stats = registry.stats()
        assert stats.created == 3
        assert stats.disposed == 2  # idle overflow pools closed immediately
        assert stats.size == 1

    @pytest.mark.asyncio
    async def test_failed_create_not_counted(self) -> None:
        attempts = 0

        async def flaky(_tid: UUID) -> str:
            nonlocal attempts
            attempts += 1

            if attempts == 1:
                raise RuntimeError("boom")

            return "client"

        registry: TenantClientRegistry[str, str] = TenantClientRegistry(
            max_entries=2,
            create=flaky,
            dispose=lambda _c: _async_return(None),
        )
        await registry.startup()
        registry.set_fingerprint(_TIDS[0], "fp")

        with pytest.raises(RuntimeError):
            await registry.get(_TIDS[0])

        assert registry.stats().created == 0

        await registry.get(_TIDS[0])
        assert registry.stats().created == 1


class TestInstrumentTenantPools:
    @pytest.mark.asyncio
    async def test_metrics_exported_per_client_label(self) -> None:
        reader = InMemoryMetricReader()
        meter = MeterProvider(metric_readers=[reader]).get_meter("test")

        registry = _registry(max_entries=2)
        await registry.startup()
        await _warm(registry, *_TIDS[:3])  # 3 creations, 1 disposal

        class _Client:
            def pool_stats(self) -> TenantPoolStats:
                return registry.stats()

        instrument_tenant_pools({"postgres": _Client()}, meter=meter)

        points: dict[str, tuple[dict[str, Any], Any]] = {}
        data = reader.get_metrics_data()
        assert data is not None

        for rm in data.resource_metrics:
            for sm in rm.scope_metrics:
                for metric in sm.metrics:
                    for dp in metric.data.data_points:
                        points[metric.name] = (dict(dp.attributes), dp.value)

        assert points[TENANT_POOL_SIZE_GAUGE][1] == 2
        assert points[TENANT_POOL_CAPACITY_GAUGE][1] == 2
        assert points[TENANT_POOL_CREATED_COUNTER][1] == 3
        assert points[TENANT_POOL_DISPOSED_COUNTER][1] == 1
        assert points[TENANT_POOL_EVICTED_COUNTER][1] == 0
        assert points[TENANT_POOL_SIZE_GAUGE][0] == {"forze.client": "postgres"}
