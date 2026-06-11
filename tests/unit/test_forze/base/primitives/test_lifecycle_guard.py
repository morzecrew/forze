"""Unit tests for :class:`~forze.base.primitives.lifecycle_guard.GuardedLifecycle`."""

import asyncio

import pytest

from forze.base.primitives.lifecycle_guard import GuardedLifecycle

# ----------------------- #


class _Owner:
    """Minimal client shape: resource field assigned only after success."""

    def __init__(self) -> None:
        self.resource: object | None = None
        self.lifecycle = GuardedLifecycle()
        self.setup_calls = 0
        self.teardown_calls = 0

    # ....................... #

    async def initialize(self, *, fail: bool = False, delay: float = 0.0) -> None:
        async def setup() -> None:
            self.setup_calls += 1

            if delay:
                await asyncio.sleep(delay)

            if fail:
                raise RuntimeError("setup failed")

            self.resource = object()

        await self.lifecycle.initialize(
            setup,
            ready=lambda: self.resource is not None,
        )

    # ....................... #

    async def close(self) -> None:
        async def teardown() -> None:
            self.teardown_calls += 1
            self.resource = None

        await self.lifecycle.close(teardown)


# ----------------------- #


class TestGuardedLifecycle:
    async def test_initialize_runs_setup_once(self) -> None:
        owner = _Owner()

        await owner.initialize()
        await owner.initialize()

        assert owner.setup_calls == 1
        assert owner.resource is not None

    # ....................... #

    async def test_concurrent_initialize_runs_setup_once(self) -> None:
        owner = _Owner()

        await asyncio.gather(*(owner.initialize(delay=0.01) for _ in range(10)))

        assert owner.setup_calls == 1
        assert owner.resource is not None

    # ....................... #

    async def test_initialize_after_close_runs_setup_again(self) -> None:
        owner = _Owner()

        await owner.initialize()
        await owner.close()
        await owner.initialize()

        assert owner.setup_calls == 2
        assert owner.resource is not None

    # ....................... #

    async def test_setup_failure_keeps_state_uninitialized_and_retries(self) -> None:
        owner = _Owner()

        with pytest.raises(RuntimeError, match="setup failed"):
            await owner.initialize(fail=True)

        assert owner.resource is None

        await owner.initialize()

        assert owner.setup_calls == 2
        assert owner.resource is not None

    # ....................... #

    async def test_close_is_idempotent(self) -> None:
        owner = _Owner()

        await owner.initialize()
        await owner.close()
        await owner.close()

        assert owner.resource is None
        assert owner.teardown_calls == 2

    # ....................... #

    async def test_close_waits_for_in_flight_initialize(self) -> None:
        owner = _Owner()
        order: list[str] = []

        async def setup() -> None:
            order.append("setup-start")
            await asyncio.sleep(0.05)
            owner.resource = object()
            order.append("setup-end")

        async def teardown() -> None:
            order.append("teardown")
            owner.resource = None

        async def init() -> None:
            await owner.lifecycle.initialize(
                setup,
                ready=lambda: owner.resource is not None,
            )

        init_task = asyncio.create_task(init())
        await asyncio.sleep(0.01)  # let initialize acquire the lock

        await owner.lifecycle.close(teardown)
        await init_task

        assert order == ["setup-start", "setup-end", "teardown"]
        assert owner.resource is None

    # ....................... #

    async def test_teardown_runs_inside_lock(self) -> None:
        guard = GuardedLifecycle()
        seen_locked: list[bool] = []

        async def teardown() -> None:
            seen_locked.append(guard._lock.locked())  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

        await guard.close(teardown)

        assert seen_locked == [True]
