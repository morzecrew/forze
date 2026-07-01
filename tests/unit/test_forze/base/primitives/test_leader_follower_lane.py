"""Unit tests for :class:`~forze.base.primitives.lanes.LeaderFollowerLane`."""

import asyncio

import pytest

from forze.base.primitives.lanes import LeaderFollowerLane

# ----------------------- #


class TestLeaderFollowerLane:
    @pytest.mark.asyncio
    async def test_concurrent_callers_run_factory_once(self) -> None:
        lane = LeaderFollowerLane[int]()
        gate = asyncio.Event()
        calls = 0

        async def factory() -> int:
            nonlocal calls
            calls += 1
            await gate.wait()
            return 42

        leader = asyncio.create_task(lane.run("k", factory))
        await asyncio.sleep(0)
        follower = asyncio.create_task(lane.run("k", factory))
        await asyncio.sleep(0)

        gate.set()

        assert await leader == 42
        assert await follower == 42
        assert calls == 1  # the follower coalesced onto the leader

    @pytest.mark.asyncio
    async def test_distinct_keys_run_independently(self) -> None:
        lane = LeaderFollowerLane[int]()

        assert await lane.run("a", lambda: _return(1)) == 1
        assert await lane.run("b", lambda: _return(2)) == 2

    @pytest.mark.asyncio
    async def test_followers_share_leader_failure(self) -> None:
        lane = LeaderFollowerLane[int]()
        gate = asyncio.Event()

        async def factory() -> int:
            await gate.wait()
            raise RuntimeError("boom")

        leader = asyncio.create_task(lane.run("k", factory))
        await asyncio.sleep(0)
        follower = asyncio.create_task(lane.run("k", factory))
        await asyncio.sleep(0)

        gate.set()

        with pytest.raises(RuntimeError):
            await leader

        with pytest.raises(RuntimeError):
            await follower

    @pytest.mark.asyncio
    async def test_follower_retries_leadership_when_leader_cancelled(self) -> None:
        lane = LeaderFollowerLane[int]()
        gate = asyncio.Event()
        calls = 0

        async def factory() -> int:
            nonlocal calls
            calls += 1
            await gate.wait()
            return 7

        leader = asyncio.create_task(lane.run("k", factory))
        await asyncio.sleep(0)
        follower = asyncio.create_task(lane.run("k", factory))
        await asyncio.sleep(0)

        leader.cancel()
        await asyncio.sleep(0)
        gate.set()

        assert await follower == 7
        assert calls == 2  # the follower became the new leader

        with pytest.raises(asyncio.CancelledError):
            await leader

    @pytest.mark.asyncio
    async def test_on_result_runs_for_leader_only_after_the_result(self) -> None:
        lane = LeaderFollowerLane[int]()
        gate = asyncio.Event()
        warmed: list[int] = []

        async def factory() -> int:
            await gate.wait()
            return 5

        async def on_result(value: int) -> None:
            warmed.append(value)

        leader = asyncio.create_task(lane.run("k", factory, on_result=on_result))
        await asyncio.sleep(0)
        follower = asyncio.create_task(lane.run("k", factory, on_result=on_result))
        await asyncio.sleep(0)

        gate.set()

        assert await leader == 5
        assert await follower == 5
        assert warmed == [5]  # only the leader ran the post-step

    @pytest.mark.asyncio
    async def test_contains_reflects_inflight_state(self) -> None:
        lane = LeaderFollowerLane[int]()
        gate = asyncio.Event()

        async def factory() -> int:
            await gate.wait()
            return 1

        assert "k" not in lane

        leader = asyncio.create_task(lane.run("k", factory))
        await asyncio.sleep(0)
        assert "k" in lane  # a leader is in flight

        gate.set()
        await leader
        assert "k" not in lane  # cleared on completion

    @pytest.mark.asyncio
    async def test_clear_drops_tracked_futures(self) -> None:
        lane = LeaderFollowerLane[int]()
        gate = asyncio.Event()

        async def factory() -> int:
            await gate.wait()
            return 1

        leader = asyncio.create_task(lane.run("k", factory))
        await asyncio.sleep(0)
        assert "k" in lane

        lane.clear()
        assert "k" not in lane

        gate.set()
        await leader  # the leader still completes; clear only drops tracking


async def _return(value: int) -> int:
    return value
