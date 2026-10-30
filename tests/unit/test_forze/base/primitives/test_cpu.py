"""Tests for the CPU / blocking-work offload seam (run_cpu / run_cpu_map / checkpoint)."""

from __future__ import annotations

import asyncio
import contextvars
import threading
import time
from typing import Any

import pytest

from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import (
    CancelToken,
    InlineCpuExecutor,
    ThreadPoolCpuExecutor,
    bind_cpu_executor,
    bind_deadline,
    checkpoint,
    current_cpu_executor,
    run_cpu,
    run_cpu_map,
)
from forze.base.primitives.cpu import _CANCEL

pytestmark = pytest.mark.asyncio

# ----------------------- #

_probe: contextvars.ContextVar[str] = contextvars.ContextVar("probe", default="unset")


def _double(x: int) -> int:
    return x * 2


# ----------------------- #


class TestRunCpu:
    async def test_runs_and_returns_on_default_pool(self) -> None:
        assert await run_cpu(_double, 21) == 42

    async def test_passes_args_and_kwargs(self) -> None:
        def add(a: int, b: int, *, c: int) -> int:
            return a + b + c

        assert await run_cpu(add, 1, 2, c=3) == 6

    async def test_inline_executor(self) -> None:
        with bind_cpu_executor(InlineCpuExecutor()):
            assert await run_cpu(_double, 10) == 20

    async def test_propagates_function_exception(self) -> None:
        def boom() -> None:
            raise ValueError("kaboom")

        with bind_cpu_executor(InlineCpuExecutor()):
            with pytest.raises(ValueError, match="kaboom"):
                await run_cpu(boom)

    async def test_copies_caller_context_into_worker(self) -> None:
        # A contextvar set by the caller must be visible inside the offloaded fn,
        # so tenant/tracing/log context stays correlated in the worker.
        _probe.set("tenant-A")

        def read_probe() -> str:
            return _probe.get()

        assert await run_cpu(read_probe) == "tenant-A"


class TestLabel:
    async def test_label_unwraps_partial(self) -> None:
        # The simulation cost-model label must survive a functools.partial wrapper.
        import functools

        captured: dict[str, str | None] = {}

        class _LabelCapture:
            async def run(self, fn, *, label=None):  # type: ignore[no-untyped-def]
                captured["label"] = label
                return fn()

        with bind_cpu_executor(_LabelCapture()):  # type: ignore[arg-type]
            await run_cpu(functools.partial(_double), 5)

        assert captured["label"] is not None
        assert captured["label"].endswith("_double")


class TestBindCpuExecutor:
    async def test_binds_and_restores(self) -> None:
        before = current_cpu_executor()
        inline = InlineCpuExecutor()

        with bind_cpu_executor(inline):
            assert current_cpu_executor() is inline

        assert current_cpu_executor() is before

    async def test_default_is_inline_and_unbound(self) -> None:
        from forze.base.primitives import cpu_executor_bound

        # Nothing bound: the fallback is inline (no hidden global pool) and the
        # context reports unbound, so a runtime knows it may own its own pool.
        assert isinstance(current_cpu_executor(), InlineCpuExecutor)
        assert cpu_executor_bound() is False

        with bind_cpu_executor(ThreadPoolCpuExecutor(max_workers=1)):
            assert cpu_executor_bound() is True

        assert cpu_executor_bound() is False


class TestThreadPoolCpuExecutor:
    async def test_first_use_from_two_threads_creates_one_pool(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from forze.base.primitives import cpu

        real = cpu.ThreadPoolExecutor
        created: list[object] = []

        def slow_pool(**kwargs: Any) -> Any:
            time.sleep(0.05)  # widen the window between the check and the assignment
            pool = real(**kwargs)
            created.append(pool)
            return pool

        monkeypatch.setattr(cpu, "ThreadPoolExecutor", slow_pool)
        executor = ThreadPoolCpuExecutor(max_workers=1)
        barrier = threading.Barrier(2)
        seen: list[object] = []

        def first_use() -> None:
            barrier.wait()
            seen.append(executor._require_pool())  # pyright: ignore[reportPrivateUsage]

        threads = [threading.Thread(target=first_use) for _ in range(2)]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join(timeout=5)

        try:
            assert len(created) == 1
            assert seen[0] is seen[1]

        finally:
            executor.close()

        for pool in created:
            pool.shutdown()  # type: ignore[attr-defined]

    async def test_a_closed_executor_refuses_rather_than_building_a_new_pool(self) -> None:
        executor = ThreadPoolCpuExecutor(max_workers=1)
        assert await executor.run(lambda: 6 * 7) == 42

        executor.close()

        with pytest.raises(CoreException) as caught:
            await executor.run(lambda: 1)

        assert caught.value.kind is ExceptionKind.UNAVAILABLE
        assert caught.value.code == "cpu_executor_closed"
        assert executor._pool is None  # pyright: ignore[reportPrivateUsage]


    async def test_a_submit_racing_close_is_refused_as_closed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        executor = ThreadPoolCpuExecutor(max_workers=1)
        pool = executor._require_pool()  # pyright: ignore[reportPrivateUsage]
        executor.close()
        # The pool handed out just before the close, as a concurrent first use would see it.
        monkeypatch.setattr(ThreadPoolCpuExecutor, "_require_pool", lambda self: pool)

        with pytest.raises(CoreException) as caught:
            await executor.run(lambda: 1)

        assert caught.value.kind is ExceptionKind.UNAVAILABLE
        assert caught.value.code == "cpu_executor_closed"

class TestDeadline:
    async def test_already_passed_deadline_raises_at_entry(self) -> None:
        with bind_deadline(0.0), pytest.raises(CoreException) as ei:
            await run_cpu(_double, 1)

        assert ei.value.kind is ExceptionKind.TIMEOUT
        assert ei.value.code == "cpu_offload_deadline"

    async def test_no_deadline_runs_normally(self) -> None:
        assert await run_cpu(_double, 4) == 8

    async def test_deadline_false_ignores_passed_deadline(self) -> None:
        # Best-effort offload: a passed deadline must NOT kill it.
        with bind_deadline(0.0):
            assert await run_cpu(_double, 9, deadline=False) == 18

    async def test_deadline_false_neutralizes_checkpoint(self) -> None:
        # deadline=False must also clear the deadline in the worker, so a checkpoint()
        # inside the offloaded code doesn't re-impose the budget the caller opted out of.
        def work() -> str:
            checkpoint()
            return "ok"

        with bind_deadline(0.0), bind_cpu_executor(InlineCpuExecutor()):
            assert await run_cpu(work, deadline=False) == "ok"


class TestCheckpoint:
    async def test_noop_outside_offload(self) -> None:
        # No active token and no deadline → no-op.
        checkpoint()

    async def test_raises_when_deadline_passed(self) -> None:
        def with_checkpoint() -> str:
            checkpoint()
            return "unreached"

        with bind_deadline(0.0), bind_cpu_executor(InlineCpuExecutor()):
            with pytest.raises(CoreException) as ei:
                await run_cpu(with_checkpoint)

        assert ei.value.code == "cpu_offload_deadline"

    async def test_raises_when_token_cancelled(self) -> None:
        token = CancelToken()
        token.cancel()
        reset = _CANCEL.set(token)
        try:
            with pytest.raises(CoreException) as ei:
                checkpoint()
        finally:
            _CANCEL.reset(reset)

        assert ei.value.code == "cpu_offload_cancelled"


class TestCancelToken:
    async def test_cancel_is_observable_and_idempotent(self) -> None:
        token = CancelToken()
        assert token.cancelled is False
        token.cancel()
        token.cancel()
        assert token.cancelled is True


class TestRunCpuMap:
    async def test_maps_in_order(self) -> None:
        assert await run_cpu_map(range(5), _double, chunk_size=2) == [0, 2, 4, 6, 8]

    async def test_empty_input(self) -> None:
        assert await run_cpu_map([], _double, chunk_size=4) == []

    async def test_single_chunk(self) -> None:
        assert await run_cpu_map([1, 2, 3], _double, chunk_size=100) == [2, 4, 6]

    async def test_rejects_bad_chunk_size(self) -> None:
        with pytest.raises(CoreException) as ei:
            await run_cpu_map([1], _double, chunk_size=0)

        assert ei.value.kind is ExceptionKind.PRECONDITION

    async def test_consumes_lazily_and_stops_on_deadline(self) -> None:
        # An unbounded source must not be materialized up front: with a passed deadline,
        # only the first chunk is pulled before the per-chunk check aborts.
        pulled: list[int] = []

        def unbounded():  # type: ignore[no-untyped-def]
            i = 0
            while True:
                pulled.append(i)
                yield i
                i += 1

        with bind_deadline(0.0), pytest.raises(CoreException):
            await run_cpu_map(unbounded(), _double, chunk_size=4)

        assert len(pulled) <= 4


class TestCooperativeCancellation:
    async def test_outer_cancel_signals_the_worker_token(self) -> None:
        # An outer-task cancel must flip the shared CancelToken so a running
        # worker can observe it (layer 2 of the cancellation contract).
        worker_started = threading.Event()
        observed_cancel = threading.Event()

        def blocking() -> None:
            worker_started.set()
            token = _CANCEL.get()
            assert token is not None
            # Cooperative poll — a real fn would call checkpoint() at chunk edges.
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                if token.cancelled:
                    observed_cancel.set()
                    return
                time.sleep(0.005)

        # Real pool so the worker runs concurrently with the loop.
        with bind_cpu_executor(ThreadPoolCpuExecutor(max_workers=2)):
            task = asyncio.ensure_future(run_cpu(blocking))

            assert await asyncio.to_thread(worker_started.wait, 2.0)
            task.cancel()

            with pytest.raises(asyncio.CancelledError):
                await task

            assert await asyncio.to_thread(observed_cancel.wait, 2.0)
