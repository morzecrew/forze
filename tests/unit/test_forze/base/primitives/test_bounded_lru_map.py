"""Tests for the synchronous bounded LRU map."""

from __future__ import annotations

import pytest

from forze.base.exceptions import CoreException
from forze.base.primitives import BoundedLruMap

# ----------------------- #


class TestBoundedLruMap:
    def test_evicts_least_recently_used_past_cap(self) -> None:
        m = BoundedLruMap[str, int](max_entries=2)
        m["a"] = 1
        m["b"] = 2

        assert m.get("a") == 1  # touching "a" makes it most-recently-used

        m["c"] = 3  # over cap -> evict the LRU entry, which is now "b"

        assert "b" not in m
        assert set(m) == {"a", "c"}

    def test_getitem_marks_recently_used(self) -> None:
        m = BoundedLruMap[str, int](max_entries=2)
        m["a"] = 1
        m["b"] = 2

        assert m["a"] == 1  # __getitem__ also refreshes recency

        m["c"] = 3

        assert "b" not in m
        assert set(m) == {"a", "c"}

    def test_update_moves_to_end(self) -> None:
        m = BoundedLruMap[str, int](max_entries=2)
        m["a"] = 1
        m["b"] = 2
        m["a"] = 10  # re-assigning "a" refreshes its recency

        m["c"] = 3

        assert "b" not in m
        assert m["a"] == 10

    def test_get_missing_returns_default(self) -> None:
        m = BoundedLruMap[str, int](max_entries=2)

        assert m.get("x") is None
        assert m.get("x", 99) == 99

    def test_stays_within_cap_under_churn(self) -> None:
        m = BoundedLruMap[str, int](max_entries=8)

        for i in range(1000):
            m[f"route-{i}"] = i

        assert len(m) == 8  # never grows past the cap

    def test_rejects_non_positive_cap(self) -> None:
        with pytest.raises(CoreException):
            BoundedLruMap[str, int](max_entries=0)


class _Cell:
    """A value carrying a mutable ``idle`` flag, for the evictable-predicate tests."""

    def __init__(self, *, idle: bool) -> None:
        self.idle = idle


class TestEvictablePredicate:
    def _map(self, cap: int) -> BoundedLruMap[str, _Cell]:
        return BoundedLruMap[str, _Cell](max_entries=cap, evictable=lambda c: c.idle)

    def test_skips_live_entries_and_evicts_oldest_idle(self) -> None:
        m = self._map(2)
        m["a"] = _Cell(idle=False)  # live: must be skipped
        m["b"] = _Cell(idle=True)  # oldest idle
        m["c"] = _Cell(idle=True)  # over cap -> evict "b", keep the live "a"

        assert set(m) == {"a", "c"}

    def test_does_not_evict_the_just_inserted_key(self) -> None:
        # All older entries are live, so the newest key is the only evictable one. It must
        # NOT be evicted (that would discard the entry we were asked to store) — overshoot.
        m = self._map(2)
        m["a"] = _Cell(idle=False)
        m["b"] = _Cell(idle=False)
        m["c"] = _Cell(idle=True)

        assert set(m) == {"a", "b", "c"}
        assert len(m) == 3  # bounded overshoot, current insertion preserved

    def test_prune_reclaims_idle_overshoot_down_to_cap(self) -> None:
        m = self._map(2)
        live_a, live_b, idle_c = _Cell(idle=False), _Cell(idle=False), _Cell(idle=True)
        m["a"], m["b"], m["c"] = live_a, live_b, idle_c
        assert len(m) == 3  # all older entries were live at insert -> overshoot

        live_b.idle = True  # a live entry goes idle
        m.prune()

        assert set(m) == {"a", "c"}  # oldest idle ("b") reclaimed; live "a" + newest "c" stay
        assert len(m) == 2

    def test_prune_keeps_all_when_nothing_evictable(self) -> None:
        m = self._map(2)
        m["a"] = _Cell(idle=False)
        m["b"] = _Cell(idle=False)
        m["c"] = _Cell(idle=False)
        assert len(m) == 3

        m.prune()  # nothing idle -> never reset live state

        assert len(m) == 3

    def test_prune_is_noop_at_or_under_cap(self) -> None:
        m = self._map(4)
        m["a"] = _Cell(idle=True)
        m["b"] = _Cell(idle=True)

        m.prune()

        assert set(m) == {"a", "b"}
