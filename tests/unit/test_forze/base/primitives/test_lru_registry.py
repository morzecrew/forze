"""Unit tests for :class:`~forze.base.primitives.lru_registry` registries."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from forze.base.exceptions import CoreException
from forze.base.primitives.lru_registry import GuardedLruRegistry, SimpleLruRegistry

# ----------------------- #


class TestSimpleLruRegistry:
    def test_rejects_zero_max_entries(self) -> None:
        with pytest.raises(CoreException, match="max_entries"):
            SimpleLruRegistry(
                max_entries=0,
                create=AsyncMock(),
                dispose=AsyncMock(),
            )

    @pytest.mark.asyncio
    async def test_create_and_hit(self) -> None:
        create = AsyncMock(side_effect=lambda k: f"v-{k}")
        dispose = AsyncMock()
        reg = SimpleLruRegistry(max_entries=4, create=create, dispose=dispose)

        assert await reg.get_or_create("a") == "v-a"
        assert await reg.get_or_create("a") == "v-a"
        create.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_lru_evicts_oldest(self) -> None:
        create = AsyncMock(side_effect=lambda k: f"v-{k}")
        dispose = AsyncMock()
        reg = SimpleLruRegistry(max_entries=2, create=create, dispose=dispose)

        await reg.get_or_create("a")
        await reg.get_or_create("b")
        await reg.get_or_create("a")
        await reg.get_or_create("c")

        dispose.assert_any_await("v-b")
        assert await reg.get_or_create("a") == "v-a"
        assert await reg.get_or_create("c") == "v-c"
        assert reg.peek("b") is None

    @pytest.mark.asyncio
    async def test_evict_and_close_all(self) -> None:
        create = AsyncMock(side_effect=lambda k: f"v-{k}")
        dispose = AsyncMock()
        reg = SimpleLruRegistry(max_entries=4, create=create, dispose=dispose)

        await reg.get_or_create("a")
        await reg.get_or_create("b")
        await reg.evict("a")
        dispose.assert_any_await("v-a")

        await reg.close_all()
        dispose.assert_any_await("v-b")
        assert len(reg._entries) == 0

    @pytest.mark.asyncio
    async def test_concurrent_get_or_create_single_factory(self) -> None:
        gate = asyncio.Event()
        calls = 0

        async def slow_create(k: str) -> str:
            nonlocal calls
            calls += 1
            await gate.wait()
            return f"v-{k}"

        dispose = AsyncMock()
        reg = SimpleLruRegistry(max_entries=4, create=slow_create, dispose=dispose)

        t1 = asyncio.create_task(reg.get_or_create("x"))
        t2 = asyncio.create_task(reg.get_or_create("x"))
        await asyncio.sleep(0.05)
        gate.set()
        r1, r2 = await asyncio.gather(t1, t2)

        assert r1 == r2 == "v-x"
        assert calls == 1


class TestGuardedLruRegistry:
    def test_rejects_zero_max_entries(self) -> None:
        with pytest.raises(CoreException, match="max_entries"):
            GuardedLruRegistry(
                max_entries=0,
                create=AsyncMock(),
                dispose=AsyncMock(),
            )

    @pytest.mark.asyncio
    async def test_use_creates_and_reuses(self) -> None:
        create = AsyncMock(side_effect=lambda k: f"v-{k}")
        dispose = AsyncMock()
        reg = GuardedLruRegistry(max_entries=4, create=create, dispose=dispose)

        async with reg.use("a") as v1:
            assert v1 == "v-a"

        async with reg.use("a") as v2:
            assert v2 == "v-a"

        create.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_eviction_defers_dispose_while_in_use(self) -> None:
        create = AsyncMock(side_effect=lambda k: f"v-{k}")
        dispose = AsyncMock()
        reg = GuardedLruRegistry(max_entries=1, create=create, dispose=dispose)

        gate = asyncio.Event()

        async def work_a() -> None:
            async with reg.use("a") as v:
                assert v == "v-a"
                await gate.wait()

        t1 = asyncio.create_task(work_a())
        await asyncio.sleep(0.05)
        assert dispose.await_count == 0

        async with reg.use("b"):
            pass

        await asyncio.sleep(0.05)
        assert dispose.await_count == 0

        gate.set()
        await t1
        assert dispose.await_count == 1
        dispose.assert_awaited_with("v-a")

    @pytest.mark.asyncio
    async def test_recreate_after_deferred_dispose(self) -> None:
        create = AsyncMock(side_effect=["v-a-1", "v-b", "v-a-2"])
        dispose = AsyncMock()
        reg = GuardedLruRegistry(max_entries=1, create=create, dispose=dispose)

        gate = asyncio.Event()

        async def hold_a() -> None:
            async with reg.use("a"):
                await gate.wait()

        t1 = asyncio.create_task(hold_a())
        await asyncio.sleep(0.05)

        async with reg.use("b"):
            pass

        gate.set()
        await t1
        assert dispose.await_count == 1
        dispose.assert_awaited_with("v-a-1")

        async with reg.use("a") as v:
            assert v == "v-a-2"

        assert create.await_count == 3

    @pytest.mark.asyncio
    async def test_evict_in_use_drains_on_idle(self) -> None:
        create = AsyncMock(return_value="v-a")
        dispose = AsyncMock()
        reg = GuardedLruRegistry(max_entries=4, create=create, dispose=dispose)

        gate = asyncio.Event()

        async def hold() -> None:
            async with reg.use("a"):
                await gate.wait()

        t1 = asyncio.create_task(hold())
        await asyncio.sleep(0.05)

        await reg.evict("a")
        assert dispose.await_count == 0

        gate.set()
        await t1
        dispose.assert_awaited_once_with("v-a")

    @pytest.mark.asyncio
    async def test_close_all_disposes_active_and_draining(self) -> None:
        create = AsyncMock(side_effect=lambda k: f"v-{k}")
        dispose = AsyncMock()
        reg = GuardedLruRegistry(max_entries=1, create=create, dispose=dispose)

        gate = asyncio.Event()

        async def hold_a() -> None:
            async with reg.use("a"):
                await gate.wait()

        t1 = asyncio.create_task(hold_a())
        await asyncio.sleep(0.05)

        async with reg.use("b"):
            pass

        await reg.close_all()
        assert dispose.await_count == 2
        dispose.assert_any_await("v-a")
        dispose.assert_any_await("v-b")

        gate.set()
        await t1
