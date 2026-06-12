"""W-TinyLFU L1 store: admission duel, scan resistance, aging, seam wiring."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.cache import CacheSpec, L1Spec
from forze.application.integrations.document import (
    DocumentCache,
    TinyLfuStore,
    tiny_lfu_l1_store,
)
from forze.base.exceptions import CoreException
from tests.unit._gateway_codec_helpers import codec_for

# ----------------------- #


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def _store(capacity: int = 100, ttl: float = 3600.0, **kw: object) -> TinyLfuStore:
    return TinyLfuStore(capacity=capacity, ttl=ttl, **kw)  # type: ignore[arg-type]


# ----------------------- #


class TestContract:
    def test_basic_roundtrip(self) -> None:
        store = _store()

        store.set("a", 1)
        assert store.get("a") == 1

        store.invalidate("a")
        assert store.get("a") is None

        store.set("b", 2)
        store.clear()
        assert store.get("b") is None

    def test_update_in_place(self) -> None:
        store = _store()

        store.set("a", 1)
        store.set("a", 2)

        assert store.get("a") == 2
        assert store.stats().size == 1

    def test_ttl_lazy_expiry(self) -> None:
        clock = _Clock()
        store = _store(ttl=1.0, clock=clock)

        store.set("a", 1)
        assert store.get("a") == 1

        clock.now += 2.0
        assert store.get("a") is None

    def test_capacity_validation(self) -> None:
        with pytest.raises(CoreException):
            _store(capacity=1)

    def test_size_never_exceeds_capacity(self) -> None:
        store = _store(capacity=10)

        for i in range(100):
            store.set(f"k{i}", i)

        assert store.stats().size <= 10


class TestAdmission:
    def test_one_hit_wonders_cannot_displace_the_hot_set(self) -> None:
        store = _store(capacity=10)

        # Build a hot set with real frequency.
        hot = [f"hot{i}" for i in range(8)]

        for _ in range(5):
            for key in hot:
                store.set(key, key)
                store.get(key)

        # A one-pass scan of cold keys floods the window.
        for i in range(1000):
            store.set(f"scan{i}", i)

        survivors = sum(1 for key in hot if store.get(key) is not None)
        assert survivors >= 7  # the hot set held; LRU would be wiped to ~0

    def test_genuinely_hotter_newcomer_is_admitted(self) -> None:
        store = _store(capacity=10)

        # A cold-ish incumbent set (one access each).
        for i in range(9):
            store.set(f"old{i}", i)

        # A newcomer that proves frequency before and during admission.
        for _ in range(6):
            store.get("new")  # misses, but the sketch records the demand

        store.set("new", "v")
        # Push it through the window so it faces the admission duel.
        store.set("filler", 0)
        store.set("filler2", 0)

        assert store.get("new") == "v"


class TestAging:
    def test_regime_change_displaces_last_seasons_hot_set(self) -> None:
        store = _store(capacity=10)

        season_a = [f"a{i}" for i in range(8)]
        season_b = [f"b{i}" for i in range(8)]

        for _ in range(5):
            for key in season_a:
                store.set(key, key)
                store.get(key)

        # Season changes: B is accessed heavily, long enough that the sketch
        # ages A's ancient popularity away (sample_size = 10 * capacity).
        for _ in range(40):
            for key in season_b:
                store.set(key, key)
                store.get(key)

        survivors_b = sum(1 for key in season_b if store.get(key) is not None)
        assert survivors_b >= 6  # the new season won the cache


class TestSegments:
    def test_probation_hit_promotes_to_protected(self) -> None:
        store = _store(capacity=100)

        store.set("a", 1)
        # Push "a" out of the 1-slot window into probation.
        store.set("b", 2)

        assert "a" in store._probation  # noqa: SLF001

        store.get("a")  # reuse on probation proves worth

        assert "a" in store._protected  # noqa: SLF001

    def test_invalidate_keeps_sketch_for_instant_readmission(self) -> None:
        store = _store(capacity=10)

        for _ in range(5):
            store.set("hot", "v")
            store.get("hot")

        store.invalidate("hot")  # e.g. a push invalidation
        assert store.get("hot") is None

        # Re-warm: its retained frequency wins the duel immediately.
        for i in range(9):
            store.set(f"cold{i}", i)

        store.set("hot", "v2")
        store.set("filler", 0)
        store.set("filler2", 0)

        assert store.get("hot") == "v2"


class TestSeamWiring:
    async def test_store_factory_builds_tinylfu_in_coordinator(self) -> None:
        class DocModel(BaseModel):
            id: UUID
            rev: int

        pk = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        doc = DocModel(id=pk, rev=1)

        cache = AsyncMock()
        cache.get.return_value = None

        coord: DocumentCache[DocModel] = DocumentCache(
            read_model_type=DocModel,
            read_codec=codec_for(DocModel),
            document_name="widgets",
            cache=cache,
            after_commit=None,
            cache_spec=CacheSpec(
                name="c",
                ttl=timedelta(seconds=300),
                l1=L1Spec(
                    ttl=timedelta(seconds=60),
                    capacity=64,
                    store_factory=tiny_lfu_l1_store,
                ),
            ),
            tenant_key=lambda: None,
        )

        assert isinstance(coord._l1, TinyLfuStore)  # noqa: SLF001

        async def fetch() -> DocModel:
            return doc

        first = await coord.get_read_through(
            pk, fetch_on_cache_fault=fetch, fetch_on_miss_without_lock=fetch
        )

        async def no_fetch() -> DocModel:  # pragma: no cover — must not run
            raise AssertionError("L1 should have served this")

        second = await coord.get_read_through(
            pk, fetch_on_cache_fault=no_fetch, fetch_on_miss_without_lock=no_fetch
        )

        assert first == second == doc
        cache.get.assert_awaited_once()
