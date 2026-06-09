"""Unit tests for :class:`~forze.base.primitives.lanes.InflightLane`."""

import asyncio

import pytest

from forze.base.exceptions import CoreException
from forze.base.primitives.lanes import InflightLane

# ----------------------- #


class TestInflightLane:
    @pytest.mark.asyncio
    async def test_concurrent_run_single_factory(self) -> None:
        lane = InflightLane[int]()
        gate = asyncio.Event()
        calls = 0

        async def factory() -> int:
            nonlocal calls
            calls += 1
            await gate.wait()
            return 42

        t1 = asyncio.create_task(lane.run(("k",), factory))
        t2 = asyncio.create_task(lane.run(("k",), factory))
        await asyncio.sleep(0.05)
        assert calls == 1

        gate.set()
        assert await t1 == 42
        assert await t2 == 42

    @pytest.mark.asyncio
    async def test_distinct_keys_run_independently(self) -> None:
        lane = InflightLane[int]()

        async def factory_a() -> int:
            return 1

        async def factory_b() -> int:
            return 2

        assert await lane.run(("a",), factory_a) == 1
        assert await lane.run(("b",), factory_b) == 2

    @pytest.mark.asyncio
    async def test_clear_allows_new_run(self) -> None:
        lane = InflightLane[int]()

        assert await lane.run(("k",), lambda: _return(1)) == 1
        lane.clear()
        assert await lane.run(("k",), lambda: _return(2)) == 2

    @pytest.mark.asyncio
    async def test_timeout_cancels_and_allows_retry(self) -> None:
        lane = InflightLane[int]()
        gate = asyncio.Event()

        async def factory() -> int:
            await gate.wait()
            return 99

        blocked = asyncio.create_task(lane.run(("k",), factory))

        await asyncio.sleep(0.05)

        with pytest.raises(CoreException, match="timed out"):
            await lane.run(("k",), factory, timeout=0.05)

        gate.set()
        blocked.cancel()

        with pytest.raises(asyncio.CancelledError):
            await blocked

        assert await lane.run(("k",), lambda: _return(7)) == 7


async def _return(value: int) -> int:
    return value
