"""Unit tests for :class:`~forze.base.primitives.lru_registry` registries."""

import asyncio
from unittest.mock import AsyncMock, patch

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

    @pytest.mark.asyncio
    async def test_dedup_key_shares_slot(self) -> None:
        create = AsyncMock(side_effect=lambda k: f"v-{k}")
        dispose = AsyncMock()
        reg = SimpleLruRegistry(
            max_entries=4,
            create=create,
            dispose=dispose,
            dedup_key=lambda k: "shared",
        )

        assert await reg.get_or_create("a") == "v-a"
        assert await reg.get_or_create("b") == "v-a"
        create.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_dedup_evict_one_logical_keeps_shared_slot(self) -> None:
        create = AsyncMock(side_effect=lambda k: f"v-{k}")
        dispose = AsyncMock()
        reg = SimpleLruRegistry(
            max_entries=4,
            create=create,
            dispose=dispose,
            dedup_key=lambda k: "shared",
        )

        await reg.get_or_create("a")
        await reg.get_or_create("b")
        await reg.evict("a")
        dispose.assert_not_awaited()
        assert await reg.get_or_create("b") == "v-a"
        await reg.evict("b")
        dispose.assert_awaited_once_with("v-a")

    @pytest.mark.asyncio
    async def test_overflow_eviction_releases_dedup_index(self) -> None:
        create = AsyncMock(side_effect=lambda k: f"v-{k}")
        dispose = AsyncMock()
        reg = SimpleLruRegistry(
            max_entries=2,
            create=create,
            dispose=dispose,
            dedup_key=lambda k: k,
        )

        for key in ("a", "b", "c", "d", "e"):
            await reg.get_or_create(key)

        # Dedup index and init-lock maps must not retain evicted slots, or they would
        # grow unbounded with the number of distinct logical keys ever seen.
        assert len(reg._dedup.logical_to_resource) <= reg.max_entries
        assert len(reg._dedup.resource_refcount) <= reg.max_entries
        assert len(reg._dedup.resource_to_keys) <= reg.max_entries
        assert len(reg._init_locks) <= reg.max_entries
        assert "a" not in reg._dedup.logical_to_resource
        assert "e" in reg._dedup.logical_to_resource


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

    @pytest.mark.asyncio
    async def test_dedup_key_shares_slot_under_use(self) -> None:
        create = AsyncMock(side_effect=lambda k: f"v-{k}")
        dispose = AsyncMock()
        reg = GuardedLruRegistry(
            max_entries=4,
            create=create,
            dispose=dispose,
            dedup_key=lambda k: "shared",
        )

        async with reg.use("a") as v1:
            async with reg.use("b") as v2:
                assert v1 == v2 == "v-a"

        create.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_overflow_eviction_releases_dedup_index(self) -> None:
        create = AsyncMock(side_effect=lambda k: f"v-{k}")
        dispose = AsyncMock()
        reg = GuardedLruRegistry(
            max_entries=2,
            create=create,
            dispose=dispose,
            dedup_key=lambda k: k,
        )

        for key in ("a", "b", "c", "d", "e"):
            async with reg.use(key):
                pass

        assert len(reg._dedup.logical_to_resource) <= reg.max_entries
        assert len(reg._dedup.resource_refcount) <= reg.max_entries
        assert len(reg._dedup.resource_to_keys) <= reg.max_entries
        assert len(reg._init_locks) <= reg.max_entries
        assert "a" not in reg._dedup.logical_to_resource


class TestLruRegistryReentrancy:
    @pytest.mark.asyncio
    async def test_simple_create_reentrancy_raises(self) -> None:
        reg = SimpleLruRegistry(
            max_entries=4,
            create=AsyncMock(),
            dispose=AsyncMock(),
        )

        async def reentrant_create(key: str) -> str:
            return await reg.get_or_create(key)

        reg.create = reentrant_create  # type: ignore[method-assign]

        with pytest.raises(CoreException, match="Reentrant"):
            await reg.get_or_create("a")

    @pytest.mark.asyncio
    async def test_guarded_create_reentrancy_raises(self) -> None:
        reg = GuardedLruRegistry(
            max_entries=4,
            create=AsyncMock(),
            dispose=AsyncMock(),
        )

        async def reentrant_create(key: str) -> str:
            async with reg.use(key):
                return "v"

        reg.create = reentrant_create  # type: ignore[method-assign]

        with pytest.raises(CoreException, match="Reentrant"):
            async with reg.use("a"):
                pass


class TestGuardedDrainWait:
    @pytest.mark.asyncio
    async def test_await_not_draining_times_out(self) -> None:
        create = AsyncMock(return_value="v")
        dispose = AsyncMock()
        reg = GuardedLruRegistry(max_entries=4, create=create, dispose=dispose)
        slot = "stuck"
        entry = reg._make_entry(slot, "v")  # noqa: SLF001
        entry.mark_draining()
        reg._draining[slot] = entry  # noqa: SLF001

        with patch.object(type(entry), "wait_until_drained", AsyncMock()):
            with pytest.raises(CoreException, match="draining"):
                await reg._await_not_draining(slot)  # noqa: SLF001
