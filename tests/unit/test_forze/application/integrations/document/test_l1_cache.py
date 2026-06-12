"""In-process L1 for document read-through: store, coordinator flows, isolation."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.cache import CacheSpec, L1Spec
from forze.application.integrations.document import DocumentCache, LruTtlStore
from forze.base.exceptions import CoreException
from tests.unit._gateway_codec_helpers import codec_for

# ----------------------- #

_PK = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_PK2 = UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")


class DocModel(BaseModel):
    id: UUID
    rev: int
    payload: str = ""


_CODEC = codec_for(DocModel)
_DOC = DocModel(id=_PK, rev=1, payload="x")
_DOC2 = DocModel(id=_PK2, rev=1, payload="y")


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def _spec(l1_ttl: float = 1.0, capacity: int = 8) -> CacheSpec:
    return CacheSpec(
        name="c",
        ttl=timedelta(seconds=300),
        l1=L1Spec(ttl=timedelta(seconds=l1_ttl), capacity=capacity),
    )


def _coord(
    *,
    cache: AsyncMock,
    spec: CacheSpec | None = None,
    tenant: str | None = None,
    l1_store: LruTtlStore | None = None,
    tenant_key=None,
) -> DocumentCache[DocModel]:
    return DocumentCache(
        read_model_type=DocModel,
        read_codec=_CODEC,
        document_name="widgets",
        cache=cache,
        after_commit=None,
        cache_spec=spec if spec is not None else _spec(),
        tenant_key=tenant_key if tenant_key is not None else (lambda: tenant),
        l1_store=l1_store,
    )


async def _fetch() -> DocModel:
    return _DOC


async def _no_fetch() -> DocModel:  # pragma: no cover — must not run
    raise AssertionError("fetched despite expected L1 hit")


async def _read(coord: DocumentCache[DocModel], pk: UUID = _PK, fetch=_fetch) -> DocModel:
    return await coord.get_read_through(
        pk,
        fetch_on_cache_fault=fetch,
        fetch_on_miss_without_lock=fetch,
    )


# ----------------------- #


class TestSpecValidation:
    def test_l1_spec_rejects_invalid(self) -> None:
        for kw in (
            {"ttl": timedelta(0)},
            {"ttl": timedelta(seconds=1), "capacity": 0},
        ):
            with pytest.raises(CoreException):
                L1Spec(**kw)  # type: ignore[arg-type]

    def test_l1_ttl_must_be_below_cache_ttl(self) -> None:
        with pytest.raises(CoreException):
            CacheSpec(
                name="c",
                ttl=timedelta(seconds=10),
                l1=L1Spec(ttl=timedelta(seconds=10)),
            )

    def test_coordinator_requires_tenant_key(self) -> None:
        with pytest.raises(CoreException) as ei:
            DocumentCache(
                read_model_type=DocModel,
                read_codec=_CODEC,
                document_name="widgets",
                cache=AsyncMock(),
                after_commit=None,
                cache_spec=_spec(),
            )

        assert "tenant_key" in str(ei.value)


class TestLruTtlStore:
    def test_ttl_expiry(self) -> None:
        clock = _Clock()
        store = LruTtlStore(capacity=4, ttl=1.0, clock=clock)

        store.set("k", "v")
        assert store.get("k") == "v"

        clock.now += 1.5
        assert store.get("k") is None

    def test_lru_eviction_at_capacity(self) -> None:
        store = LruTtlStore(capacity=2, ttl=60.0)

        store.set("a", 1)
        store.set("b", 2)
        assert store.get("a") == 1  # touch: a is now most-recent

        store.set("c", 3)  # evicts b, the least-recently-used

        assert store.get("b") is None
        assert store.get("a") == 1
        assert store.get("c") == 3

    def test_stats_counters(self) -> None:
        store = LruTtlStore(capacity=1, ttl=60.0)

        store.set("a", 1)
        store.get("a")
        store.get("missing")
        store.set("b", 2)  # evicts a

        stats = store.stats()
        assert (stats.hits, stats.misses, stats.evictions, stats.size) == (1, 1, 1, 1)


class TestReadThrough:
    async def test_second_read_served_from_l1(self) -> None:
        cache = AsyncMock()
        cache.get.return_value = None
        coord = _coord(cache=cache)

        first = await _read(coord)
        second = await _read(coord, fetch=_no_fetch)

        assert first == second == _DOC
        cache.get.assert_awaited_once()  # the second read never reached L2

    async def test_l2_hit_populates_l1(self) -> None:
        cache = AsyncMock()
        cache.get.return_value = _CODEC.encode_json_bytes(_DOC)
        coord = _coord(cache=cache)

        await _read(coord, fetch=_no_fetch)  # L2 hit warms L1
        await _read(coord, fetch=_no_fetch)

        cache.get.assert_awaited_once()

    async def test_mutating_result_does_not_poison_l1(self) -> None:
        cache = AsyncMock()
        cache.get.return_value = None
        coord = _coord(cache=cache)

        first = await _read(coord)
        first.payload = "mutated"

        second = await _read(coord, fetch=_no_fetch)
        assert second.payload == "x"

    async def test_tenants_partitioned(self) -> None:
        cache = AsyncMock()
        cache.get.return_value = None
        tenant = {"current": "t1"}
        coord = _coord(cache=cache, tenant_key=lambda: tenant["current"])

        await _read(coord)  # warms t1's entry
        tenant["current"] = "t2"
        await _read(coord)  # t2 must miss L1 and hit L2 again

        assert cache.get.await_count == 2

    async def test_l1_expiry_falls_back_to_l2(self) -> None:
        clock = _Clock()
        store = LruTtlStore(capacity=8, ttl=1.0, clock=clock)
        cache = AsyncMock()
        cache.get.return_value = _CODEC.encode_json_bytes(_DOC)
        coord = _coord(cache=cache, l1_store=store)

        await _read(coord, fetch=_no_fetch)
        clock.now += 2.0  # past the staleness budget
        await _read(coord, fetch=_no_fetch)

        assert cache.get.await_count == 2


class TestWritePaths:
    async def test_set_one_refreshes_l1_read_your_writes(self) -> None:
        cache = AsyncMock()
        coord = _coord(cache=cache)

        await coord.set_one(_DOC)  # write-path warm

        got = await _read(coord, fetch=_no_fetch)
        assert got == _DOC

    async def test_clear_invalidates_l1(self) -> None:
        cache = AsyncMock()
        cache.get.return_value = None
        coord = _coord(cache=cache)

        await _read(coord)  # warm
        await coord.clear(_PK)

        await _read(coord)  # must go through L2 again
        assert cache.get.await_count == 2


class TestGetMany:
    async def test_full_l1_coverage_skips_l2(self) -> None:
        cache = AsyncMock()
        coord = _coord(cache=cache)

        await coord.set_one(_DOC)
        await coord.set_one(_DOC2)

        async def _fault() -> list[DocModel]:  # pragma: no cover
            raise AssertionError("L2 reached despite full L1 coverage")

        res = await coord.get_many_read_through(
            [_PK, _PK2],
            fetch_many_on_cache_fault=_fault,
            fetch_misses_many=lambda _m: _fault(),
        )

        assert [d.id for d in res] == [_PK, _PK2]
        cache.get_many.assert_not_awaited()

    async def test_partial_l1_fetches_only_remaining(self) -> None:
        cache = AsyncMock()
        cache.get_many.return_value = ({}, [str(_PK2)])
        coord = _coord(cache=cache)

        await coord.set_one(_DOC)  # only _PK in L1

        async def _misses(keys: list[str]) -> list[DocModel]:
            assert keys == [str(_PK2)]
            return [_DOC2]

        res = await coord.get_many_read_through(
            [_PK, _PK2],
            fetch_many_on_cache_fault=AsyncMock(),
            fetch_misses_many=_misses,
        )

        assert [d.id for d in res] == [_PK, _PK2]
        ((requested,), _) = cache.get_many.await_args
        assert requested == [str(_PK2)]  # L1 hit excluded from the L2 batch


class TestInactiveWithoutOptIn:
    async def test_no_l1_without_spec(self) -> None:
        cache = AsyncMock()
        cache.get.return_value = None
        coord = DocumentCache(
            read_model_type=DocModel,
            read_codec=_CODEC,
            document_name="widgets",
            cache=cache,
            after_commit=None,
            cache_spec=CacheSpec(name="c", ttl=timedelta(seconds=300)),
        )

        await _read(coord)
        await _read(coord)

        assert cache.get.await_count == 2  # every read reaches L2, as before
