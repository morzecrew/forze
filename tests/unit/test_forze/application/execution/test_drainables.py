"""Unit tests for the background-loop registry.

# covers: forze.application.execution.context.drainable.Drainables

Every background lifecycle step owns a task that runs for the life of the process, and nothing
outside the step could reach it — so shutdown could only ever cancel it, mid-work, whatever the
work was. The registry is how the runtime asks a loop to stop *between* units of work instead,
before teardown begins and while its clients are still open.
"""

from __future__ import annotations

import asyncio
from typing import final
from unittest.mock import MagicMock, patch

import attrs
import pytest

from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.application.execution.context import DrainableLoop, Drainables
from forze_mock import MockDepsModule

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class _FakeLoop:
    """A loop that records how it was asked to stop."""

    name: str
    stops: int = attrs.field(default=0, init=False)
    hangs: bool = False
    raises: bool = False

    @property
    def loop_name(self) -> str:
        return self.name

    async def stop(self, *, deadline: float) -> bool:
        self.stops += 1

        if self.raises:
            raise RuntimeError("wedged")

        if self.hangs:
            await asyncio.sleep(30)

        return True


# ----------------------- #


@pytest.mark.asyncio
async def test_every_registered_loop_is_stopped() -> None:
    registry = Drainables()
    loops = [_FakeLoop(name=f"loop-{i}") for i in range(3)]

    for one in loops:
        registry.register(one)

    assert await registry.stop_all(grace=1.0) == 3
    assert [one.stops for one in loops] == [1, 1, 1]


@pytest.mark.asyncio
async def test_loops_stop_concurrently_not_one_after_another() -> None:
    # These are independent workers on independent backends: a relay draining its outbox has
    # no reason to wait for a consumer to finish its message. Sequential stops would multiply
    # the shutdown budget by the number of loops.
    started = asyncio.Event()
    release = asyncio.Event()

    @final
    @attrs.define(slots=True, kw_only=True)
    class _Blocking:
        name: str

        @property
        def loop_name(self) -> str:
            return self.name

        async def stop(self, *, deadline: float) -> bool:
            started.set()
            await release.wait()
            return True

    fast = _FakeLoop(name="fast")
    registry = Drainables()
    registry.register(_Blocking(name="slow"))
    registry.register(fast)

    sweep = asyncio.create_task(registry.stop_all(grace=5.0))
    await started.wait()

    # The slow loop is still stopping, and the fast one has already been asked.
    assert fast.stops == 1

    release.set()
    assert await sweep == 2


@pytest.mark.asyncio
async def test_a_wedged_loop_is_cancelled_and_the_others_still_stop() -> None:
    # Teardown must not hang on one loop that will not come back.
    wedged = _FakeLoop(name="wedged", hangs=True)
    healthy = _FakeLoop(name="healthy")

    registry = Drainables()
    registry.register(wedged)
    registry.register(healthy)

    stopped = await registry.stop_all(grace=0.05)

    assert stopped == 1  # only the healthy one stopped cleanly
    assert wedged.stops == 1  # it was asked


@pytest.mark.asyncio
async def test_a_failing_loop_is_isolated_and_never_silent() -> None:
    broken = _FakeLoop(name="broken", raises=True)
    healthy = _FakeLoop(name="healthy")

    registry = Drainables()
    registry.register(broken)
    registry.register(healthy)

    logger_mock = MagicMock()

    with patch("forze.application.execution.context.drainable.logger", logger_mock):
        stopped = await registry.stop_all(grace=1.0)

    assert stopped == 1  # the healthy loop is unaffected by the broken one
    assert healthy.stops == 1
    logger_mock.error.assert_called_once()  # ...and the failure is reported, not swallowed


@pytest.mark.asyncio
async def test_an_empty_registry_costs_nothing() -> None:
    assert await Drainables().stop_all(grace=1.0) == 0


@pytest.mark.asyncio
async def test_the_runtime_stops_registered_loops_at_shutdown() -> None:
    # And does so *before* lifecycle teardown, so a loop whose graceful stop needs its database
    # — the outbox relay's drain — still has an open pool. Reverse-wave hook ordering could
    # never guarantee that.
    loop = _FakeLoop(name="relay")
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        runtime.get_context().drainables.register(loop)

    assert loop.stops == 1


def test_the_fake_satisfies_the_protocol() -> None:
    assert isinstance(_FakeLoop(name="x"), DrainableLoop)
