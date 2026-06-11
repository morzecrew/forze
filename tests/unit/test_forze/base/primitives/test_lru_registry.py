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

    @pytest.mark.asyncio
    async def test_peek_with_dedup_returns_none_for_unknown_key(self) -> None:
        create = AsyncMock(side_effect=lambda k: f"v-{k}")
        dispose = AsyncMock()
        reg = SimpleLruRegistry(
            max_entries=4,
            create=create,
            dispose=dispose,
            dedup_key=lambda k: k,
        )

        # dedup_key set but key never created -> slot lookup misses, returns None.
        assert reg.peek("missing") is None

    @pytest.mark.asyncio
    async def test_evict_unknown_dedup_key_is_noop(self) -> None:
        create = AsyncMock(side_effect=lambda k: f"v-{k}")
        dispose = AsyncMock()
        reg = SimpleLruRegistry(
            max_entries=4,
            create=create,
            dispose=dispose,
            dedup_key=lambda k: k,
        )

        # ``release`` finds no logical mapping for the key -> returns None early.
        await reg.evict("never-seen")
        dispose.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_race_disposes_late_value(self) -> None:
        # Simulate a concurrent winner: ``create`` itself registers the slot before
        # returning, so the post-create re-check finds an existing entry and the
        # freshly created value is disposed instead of stored.
        dispose = AsyncMock()

        async def racing_create(key: str) -> str:
            reg._entries["a"] = "winner"  # noqa: SLF001
            return f"loser-{key}"

        reg = SimpleLruRegistry(max_entries=4, create=racing_create, dispose=dispose)

        result = await reg.get_or_create("a")

        assert result == "winner"
        dispose.assert_awaited_once_with("loser-a")
        assert reg._entries["a"] == "winner"


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

    @pytest.mark.asyncio
    async def test_peek_active_missing_and_draining(self) -> None:
        create = AsyncMock(return_value="v-a")
        dispose = AsyncMock()
        reg = GuardedLruRegistry(max_entries=4, create=create, dispose=dispose)

        # No dedup_key: peek takes the ``slot = key`` branch and returns None
        # when nothing is registered.
        assert reg.peek("a") is None

        async with reg.use("a") as v:
            assert v == "v-a"
            # Active slot lookup returns the live value.
            assert reg.peek("a") == "v-a"

        # Move the entry into the draining map so peek falls through the active
        # lookup and reads from ``_draining``.
        slot = "a"
        entry = reg._slots.pop(slot)  # noqa: SLF001
        reg._draining[slot] = entry  # noqa: SLF001
        assert reg.peek("a") == "v-a"

        # Slot absent from both maps -> final ``return None``.
        del reg._draining[slot]  # noqa: SLF001
        assert reg.peek("a") is None

    @pytest.mark.asyncio
    async def test_peek_with_dedup_returns_none_for_unknown_key(self) -> None:
        create = AsyncMock(return_value="v-a")
        dispose = AsyncMock()
        reg = GuardedLruRegistry(
            max_entries=4,
            create=create,
            dispose=dispose,
            dedup_key=lambda k: k,
        )

        # dedup_key set but key never resolved -> slot lookup misses, returns None.
        assert reg.peek("missing") is None

    @pytest.mark.asyncio
    async def test_evict_idle_active_entry_disposes_immediately(self) -> None:
        create = AsyncMock(return_value="v-a")
        dispose = AsyncMock()
        reg = GuardedLruRegistry(max_entries=4, create=create, dispose=dispose)

        async with reg.use("a"):
            pass

        # Entry is active and idle (refcount 0) -> evict disposes immediately.
        await reg.evict("a")
        dispose.assert_awaited_once_with("v-a")
        assert "a" not in reg._slots  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_use_finds_entry_created_under_init_lock(self) -> None:
        # Inject the slot into ``_slots`` right after the init-lock is acquired but
        # before the post-init-lock re-check, so ``use`` finds an existing entry and
        # reuses it without ever invoking ``create``.
        create = AsyncMock(side_effect=AssertionError("create must not run"))
        dispose = AsyncMock()
        reg = GuardedLruRegistry(max_entries=4, create=create, dispose=dispose)

        original = type(reg)._lock_for_init  # noqa: SLF001

        async def lock_then_inject(self: object, slot: str) -> asyncio.Lock:
            lock = await original(self, slot)
            reg._slots[slot] = reg._make_entry(slot, "preexisting")  # noqa: SLF001
            return lock

        with patch.object(type(reg), "_lock_for_init", lock_then_inject):
            async with reg.use("a") as v:
                assert v == "preexisting"

        create.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_race_disposes_late_value(self) -> None:
        # ``create`` registers the slot itself, simulating a concurrent winner;
        # the post-create re-check disposes the new value and reuses the existing.
        dispose = AsyncMock()
        reg: GuardedLruRegistry[str, str, str]

        async def racing_create(key: str) -> str:
            reg._slots["a"] = reg._make_entry("a", "winner")  # noqa: SLF001
            return f"loser-{key}"

        reg = GuardedLruRegistry(max_entries=4, create=racing_create, dispose=dispose)

        async with reg.use("a") as v:
            assert v == "winner"

        dispose.assert_awaited_once_with("loser-a")

    @pytest.mark.asyncio
    async def test_evict_drains_entry_already_in_draining_map(self) -> None:
        create = AsyncMock(return_value="v-a")
        dispose = AsyncMock()
        reg = GuardedLruRegistry(max_entries=4, create=create, dispose=dispose)

        slot = "a"
        entry = reg._make_entry(slot, "v-a")  # noqa: SLF001
        entry.refcount = 1
        reg._dedup.slot_for("a")  # noqa: SLF001
        reg._draining[slot] = entry  # noqa: SLF001

        await reg.evict("a")

        # In-use draining entry: still deferred, not disposed immediately.
        dispose.assert_not_awaited()
        assert slot in reg._draining  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_evict_unknown_slot_is_noop(self) -> None:
        create = AsyncMock(return_value="v-a")
        dispose = AsyncMock()
        reg = GuardedLruRegistry(max_entries=4, create=create, dispose=dispose)

        # Register a logical key in the dedup index without any backing entry, so
        # ``release`` yields a slot but neither map holds it -> immediate stays None.
        reg._dedup.slot_for("a")  # noqa: SLF001

        await reg.evict("a")

        dispose.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_evict_unknown_dedup_key_returns_early(self) -> None:
        create = AsyncMock(return_value="v-a")
        dispose = AsyncMock()
        reg = GuardedLruRegistry(
            max_entries=4,
            create=create,
            dispose=dispose,
            dedup_key=lambda k: k,
        )

        # ``release`` finds no logical mapping -> evict returns before touching slots.
        await reg.evict("never-seen")
        dispose.assert_not_awaited()


class TestGuardedLruRegistryRaces:
    @pytest.mark.asyncio
    async def test_use_never_yields_value_disposed_by_concurrent_evict(self) -> None:
        """Regression: lookup -> refcount increment is atomic w.r.t. eviction.

        Before the fix, ``use()`` fetched the entry under the registry lock but
        incremented the refcount only after awaiting the entry condition. A task
        paused in that gap (simulated here by holding the entry condition) let
        ``evict()`` observe ``refcount == 0`` and dispose the value the first
        task was about to receive.
        """

        disposed: list[str] = []

        async def tracking_dispose(value: str) -> None:
            disposed.append(value)

        create = AsyncMock(return_value="v-a")
        reg = GuardedLruRegistry(
            max_entries=4,
            create=create,
            dispose=tracking_dispose,
        )

        async with reg.use("a"):
            pass

        entry = reg._slots["a"]  # noqa: SLF001
        # Hold the entry condition: the old code blocked here BEFORE the
        # refcount increment; the fixed code reserves under the registry lock
        # and only touches the condition on scope exit.
        await entry.condition.acquire()

        observed: dict[str, object] = {}

        async def task_a() -> None:
            async with reg.use("a") as value:
                observed["value"] = value
                observed["was_disposed_at_yield"] = value in disposed

        t_a = asyncio.create_task(task_a())
        await asyncio.sleep(0.05)

        # Concurrent eviction: must NOT dispose, the value is reserved by A.
        await reg.evict("a")
        assert disposed == []
        assert observed.get("was_disposed_at_yield") is False
        assert observed.get("value") == "v-a"

        entry.condition.release()
        await asyncio.wait_for(t_a, timeout=2)

        # Deferred disposal happens only after A's use scope exits.
        assert disposed == ["v-a"]

    @pytest.mark.asyncio
    async def test_new_entry_not_disposed_by_overflow_before_first_use(self) -> None:
        """The create path reserves the new entry before overflow eviction runs,
        so a concurrent insert can only drain it, never dispose it mid-use."""

        disposed: list[str] = []

        async def tracking_dispose(value: str) -> None:
            disposed.append(value)

        create = AsyncMock(side_effect=lambda k: f"v-{k}")
        reg = GuardedLruRegistry(
            max_entries=1,
            create=create,
            dispose=tracking_dispose,
        )

        gate = asyncio.Event()
        seen: dict[str, object] = {}

        async def hold_a() -> None:
            async with reg.use("a") as value:
                seen["a_disposed_mid_use"] = value in disposed
                await gate.wait()
                seen["a_disposed_after_wait"] = value in disposed

        t_a = asyncio.create_task(hold_a())
        await asyncio.sleep(0.05)

        # Overflow: "a" is the LRU entry but in use -> drained, not disposed.
        async with reg.use("b"):
            pass

        assert "v-a" not in disposed

        gate.set()
        await asyncio.wait_for(t_a, timeout=2)

        assert seen == {"a_disposed_mid_use": False, "a_disposed_after_wait": False}
        assert disposed == ["v-a"]

    @pytest.mark.asyncio
    async def test_dispose_error_during_drain_deregisters_slot(self) -> None:
        """A failing ``dispose`` in the drain path must still lift the draining
        barrier; otherwise every later ``use()`` of the slot hangs."""

        create = AsyncMock(side_effect=["v-1", "v-2"])
        dispose_calls: list[str] = []

        async def failing_dispose(value: str) -> None:
            dispose_calls.append(value)

            if len(dispose_calls) == 1:
                raise RuntimeError("dispose boom")

        reg = GuardedLruRegistry(
            max_entries=4,
            create=create,
            dispose=failing_dispose,
        )

        gate = asyncio.Event()

        async def hold() -> None:
            async with reg.use("a"):
                await gate.wait()

        t = asyncio.create_task(hold())
        await asyncio.sleep(0.05)

        await reg.evict("a")  # in use -> drain deferred until scope exit
        gate.set()

        # The dispose error propagates out of the use scope after cleanup.
        with pytest.raises(RuntimeError, match="dispose boom"):
            await asyncio.wait_for(t, timeout=2)

        # Slot is deregistered: no draining leftovers, fresh create succeeds.
        assert reg._draining == {}  # noqa: SLF001

        async def fresh() -> str:
            async with reg.use("a") as value:
                return value

        assert await asyncio.wait_for(fresh(), timeout=2) == "v-2"


class TestGuardedEntryDrain:
    @pytest.mark.asyncio
    async def test_wait_until_drained_resolves_when_barrier_set(self) -> None:
        dispose = AsyncMock()
        reg = GuardedLruRegistry(max_entries=4, create=AsyncMock(), dispose=dispose)
        entry = reg._make_entry("slot", "v")  # noqa: SLF001
        entry.mark_draining()

        waiter = asyncio.create_task(entry.wait_until_drained())
        await asyncio.sleep(0)
        assert not waiter.done()

        entry.draining_barrier.set()
        await waiter

        assert waiter.done()


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
