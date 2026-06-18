"""DST P2 exit gate: timed workloads run in virtual time, deterministically.

The native :class:`SimulationEventLoop` advances a virtual clock only when the loop
waits, so workloads spanning hours of ``asyncio.sleep`` / deadlines / backoff finish
in real-wall milliseconds — and because wall, monotonic, ids, and jitter all read the
bound simulation seams, the same seed reproduces byte-identically.
"""

from __future__ import annotations

import asyncio

import pytest

from forze.application.execution.context.deadline import bind_deadline, remaining_time
from forze.base.primitives import current_entropy_source, monotonic, uuid7
from forze_dst import (
    RealIOForbidden,
    SimulationDeadlock,
    SimulationEventLoop,
    run_simulation,
)

from examples.recipes.order_fulfillment.app import (
    ORDER_SPEC,
    build_context,
    deliver,
    place_order,
    relay_once,
    run_checkout,
)

# ----------------------- #


class TestVirtualClock:
    def test_sleeps_advance_virtual_time(self) -> None:
        async def scenario() -> float:
            await asyncio.sleep(60)
            await asyncio.sleep(2.5)
            return monotonic()

        assert run_simulation(scenario) == 62.5

    def test_large_span_is_instant(self) -> None:
        # 30 virtual days: returns at all only because time is virtual (a real loop
        # would hang the suite well past its timeout).
        async def scenario() -> float:
            await asyncio.sleep(86_400 * 30)
            return monotonic()

        assert run_simulation(scenario) == 86_400 * 30

    def test_concurrent_timers_resolve_in_order(self) -> None:
        async def scenario() -> list[int]:
            done: list[int] = []

            async def fire(after: int, tag: int) -> None:
                await asyncio.sleep(after)
                done.append(tag)

            await asyncio.gather(fire(30, 1), fire(10, 2), fire(20, 3))
            return done

        assert run_simulation(scenario) == [2, 3, 1]  # by virtual wake order

    def test_deadline_tracks_virtual_time(self) -> None:
        def _remaining() -> float:
            r = remaining_time()
            return -1.0 if r is None else r

        async def scenario() -> list[float]:
            trace: list[float] = []
            with bind_deadline(10.0):
                trace.append(_remaining())  # 10
                await asyncio.sleep(3)
                trace.append(_remaining())  # 7
                await asyncio.sleep(8)
                trace.append(_remaining())  # clamped 0
            return trace

        assert [round(x, 6) for x in run_simulation(scenario)] == [10.0, 7.0, 0.0]


class TestDeterminism:
    @staticmethod
    async def _jittered() -> list[tuple[float, str]]:
        trace: list[tuple[float, str]] = []
        for _ in range(6):
            delay = current_entropy_source().as_random().uniform(0.5, 1.5)
            await asyncio.sleep(delay)
            trace.append((round(monotonic(), 9), str(uuid7())))
        return trace

    def test_same_seed_is_byte_identical(self) -> None:
        a = run_simulation(self._jittered, seed=7)
        b = run_simulation(self._jittered, seed=7)
        assert a == b

    def test_different_seed_diverges(self) -> None:
        # Different jitter draws → different sleep durations AND ids.
        assert run_simulation(self._jittered, seed=7) != run_simulation(
            self._jittered, seed=8
        )


class TestLeakGuards:
    def test_thread_executor_forbidden(self) -> None:
        async def scenario() -> int:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, lambda: 1)

        with pytest.raises(RealIOForbidden):
            run_simulation(scenario)

    def test_deadlock_surfaced(self) -> None:
        async def scenario() -> None:
            await asyncio.Event().wait()  # nothing will ever set it

        with pytest.raises(SimulationDeadlock):
            run_simulation(scenario)

    def test_naive_epoch_rejected(self) -> None:
        # A naive epoch's timestamp() is host-timezone-dependent → non-reproducible time/ids.
        from datetime import datetime

        async def scenario() -> None:
            return None

        with pytest.raises(ValueError):
            run_simulation(scenario, epoch=datetime(2020, 1, 1))  # noqa: DTZ001 - the point

    def test_real_io_transports_forbidden(self) -> None:
        loop = SimulationEventLoop()
        try:
            makers = (
                loop._make_socket_transport,
                loop._make_ssl_transport,
                loop._make_datagram_transport,
                loop._make_read_pipe_transport,
                loop._make_write_pipe_transport,
                loop._make_subprocess_transport,
            )
            for make in makers:
                with pytest.raises(RealIOForbidden):
                    make(None, None)
        finally:
            loop.close()

    def test_cross_thread_wakeup_forbidden(self) -> None:
        loop = SimulationEventLoop()
        try:
            # A threadsafe wakeup can only come from a real foreign thread → refuse it.
            with pytest.raises(RealIOForbidden):
                loop.call_soon_threadsafe(lambda: None)
        finally:
            loop.close()

    def test_selector_registration_forbidden(self) -> None:
        loop = SimulationEventLoop()
        try:
            with pytest.raises(RealIOForbidden):
                loop._selector.register(0, 0)
            with pytest.raises(RealIOForbidden):
                loop._selector.unregister(0)
            assert loop._selector.get_map() == {}  # no fds ever registered
        finally:
            loop.close()

    def test_clock_helpers_are_inert_at_zero(self) -> None:
        loop = SimulationEventLoop()
        try:
            loop._write_to_self()  # single-threaded no-op
            before = loop.time()
            loop._advance(0.0)  # non-positive advance is a no-op
            loop._advance(-5.0)
            assert loop.time() == before
        finally:
            loop.close()


class TestFullStackUnderSimulation:
    """The whole aggregate → saga → outbox → relay → inbox flow, on the sim loop."""

    @staticmethod
    async def _order_flow() -> tuple[str, str]:
        ctx = build_context()
        order_id, inventory_id = await place_order(ctx)
        await run_checkout(ctx, order_id, inventory_id)
        messages = await relay_once(ctx)
        await deliver(ctx, messages[0])
        order = await ctx.document.query(ORDER_SPEC).get(order_id)
        return str(order_id), order.status

    def test_runs_and_is_deterministic(self) -> None:
        a = run_simulation(self._order_flow, seed=1)
        b = run_simulation(self._order_flow, seed=1)
        assert a == b
        assert a[1] == "confirmed"
