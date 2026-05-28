"""Unit tests for :class:`~forze.base.primitives.cache.CacheLane`."""

from forze.base.primitives.cache import CacheLane

# ----------------------- #


class TestCacheLane:
    def test_lookup_miss(self) -> None:
        lane = CacheLane[str, int]()
        assert lane.lookup("a") is None

    def test_store_and_lookup(self) -> None:
        lane = CacheLane[str, int]()
        lane.store("a", 1)
        assert lane.lookup("a") == 1
        assert "a" in lane
        assert len(lane) == 1

    def test_invalidate(self) -> None:
        lane = CacheLane[str, int]()
        lane.store("a", 1)
        lane.invalidate("a")
        assert lane.lookup("a") is None
        assert len(lane) == 0

    def test_clear(self) -> None:
        lane = CacheLane[str, int]()
        lane.store("a", 1)
        lane.store("b", 2)
        lane.clear()
        assert len(lane) == 0

    def test_fifo_trim(self) -> None:
        lane = CacheLane[str, int](max_entries=2)
        lane.store("a", 1)
        lane.store("b", 2)
        lane.store("c", 3)
        assert lane.lookup("a") is None
        assert lane.lookup("b") == 2
        assert lane.lookup("c") == 3

    def test_ttl_expiry(self) -> None:
        t = 0.0

        def clock() -> float:
            return t

        lane = CacheLane[str, int](ttl_seconds=10.0, clock=clock)
        lane.store("a", 1)
        assert lane.lookup("a") == 1

        t = 10.0
        assert lane.lookup("a") is None

    def test_no_ttl_without_ttl_seconds(self) -> None:
        t = 0.0

        def clock() -> float:
            return t

        lane = CacheLane[str, int](clock=clock)
        lane.store("a", 1)
        t = 1_000_000.0
        assert lane.lookup("a") == 1
