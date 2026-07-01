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
