"""Tests for the CPU / blocking-work offload seam (run_cpu / run_cpu_map / checkpoint)."""

from __future__ import annotations

import asyncio
import contextvars
import functools
import json
import math
import os
import pickle
import signal
import sys
import threading
import time
from collections.abc import Iterator
from concurrent.futures import BrokenExecutor
from typing import Any
from unittest.mock import MagicMock

import pytest

from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import (
    CancelToken,
    InlineCpuExecutor,
    InterpreterPoolCpuExecutor,
    ProcessPoolCpuExecutor,
    ThreadPoolCpuExecutor,
    bind_cpu_executor,
    bind_deadline,
    bind_isolated_cpu_executor,
    checkpoint,
    current_cpu_executor,
    current_isolated_cpu_executor,
    hold_isolated_cpu_executor,
    isolated_cpu_executor_bound,
    run_cpu,
    run_cpu_map,
)
from forze.base.primitives import cpu as cpu_module
from forze.base.primitives.cpu import _CANCEL

pytestmark = pytest.mark.asyncio

# ----------------------- #

_probe: contextvars.ContextVar[str] = contextvars.ContextVar("probe", default="unset")


def _double(x: int) -> int:
    return x * 2


class _UnpicklableBack(Exception):
    """Pickles fine, but unpickling calls ``cls(message)`` and the constructor needs two args."""

    def __init__(self, first: str, second: str) -> None:
        super().__init__(f"{first} {second}")


def _raise_unpicklable_back() -> None:
    raise _UnpicklableBack("only", "here")


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

    async def test_a_submit_error_from_an_open_executor_is_not_relabelled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        executor = ThreadPoolCpuExecutor(max_workers=1)
        loop = asyncio.get_running_loop()

        def _refuse(*_: object) -> None:
            raise RuntimeError("cannot schedule new futures after interpreter shutdown")

        monkeypatch.setattr(loop, "run_in_executor", _refuse)

        try:
            with pytest.raises(RuntimeError, match="interpreter shutdown"):
                await executor.run(lambda: 1)

        finally:
            executor.close()

    async def test_a_first_use_that_waited_on_the_lock_takes_the_pool_built_meanwhile(self) -> None:
        from forze.base.primitives import cpu

        executor = ThreadPoolCpuExecutor(max_workers=1)
        built = cpu.ThreadPoolExecutor(max_workers=1)

        class _BuiltMeanwhile:
            # Another thread builds the pool while this one waits for the lock.
            def __enter__(self) -> None:
                executor._pool = built  # pyright: ignore[reportPrivateUsage]

            def __exit__(self, *_: object) -> None:
                return None

        executor._lock = _BuiltMeanwhile()  # type: ignore[assignment]  # pyright: ignore[reportPrivateUsage, reportAttributeAccessIssue]

        try:
            assert executor._require_pool() is built  # pyright: ignore[reportPrivateUsage]

        finally:
            built.shutdown()

    async def test_closing_an_unused_executor_is_a_no_op(self) -> None:
        executor = ThreadPoolCpuExecutor(max_workers=1)
        executor.close()

        assert executor._pool is None  # pyright: ignore[reportPrivateUsage]


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


# ----------------------- #
# Isolated executors: stdlib callables only, so a worker process or interpreter can always
# resolve them (a test module is not importable in a subinterpreter: forze pulls in orjson).

_needs_interpreters = pytest.mark.skipif(
    sys.version_info < (3, 14), reason="InterpreterPoolExecutor is Python 3.14+"
)


@pytest.fixture(scope="module")
def process_executor() -> Iterator[ProcessPoolCpuExecutor]:
    # One shared pool: starting worker processes dominates these tests' cost.
    executor = ProcessPoolCpuExecutor(max_workers=2)
    yield executor
    executor.close()


class TestProcessPoolCpuExecutor:
    async def test_returns_result(self, process_executor: ProcessPoolCpuExecutor) -> None:
        result = await process_executor.run(functools.partial(math.factorial, 20))

        assert result == math.factorial(20)

    async def test_runs_in_other_processes(self, process_executor: ProcessPoolCpuExecutor) -> None:
        pids = await asyncio.gather(
            process_executor.run(os.getpid), process_executor.run(os.getpid)
        )

        assert os.getpid() not in pids

    async def test_propagates_exception_with_remote_traceback(
        self, process_executor: ProcessPoolCpuExecutor
    ) -> None:
        with pytest.raises(json.JSONDecodeError) as ei:
            await process_executor.run(functools.partial(json.loads, "not json"))

        assert ei.value.__cause__ is not None
        assert "decoder.py" in str(ei.value.__cause__)

    async def test_refuses_unpicklable_before_submitting(self) -> None:
        executor = ProcessPoolCpuExecutor(max_workers=1)

        with pytest.raises(CoreException) as ei:
            await executor.run(functools.partial(print, threading.Lock()), label="site.fn")

        assert ei.value.kind is ExceptionKind.CONFIGURATION
        assert ei.value.code == "cpu_offload_not_picklable"
        assert "site.fn" in str(ei.value)
        assert executor._pool is None  # refused before the pool was even created

    async def test_close_terminates_an_abandoned_worker(self) -> None:
        executor = ProcessPoolCpuExecutor(max_workers=1)

        with (
            bind_isolated_cpu_executor(executor),
            bind_deadline(0.2),
            pytest.raises(CoreException) as ei,
        ):
            await run_cpu(time.sleep, 30, isolated=True)

        assert ei.value.code == "cpu_offload_deadline"
        assert executor._pool is not None
        workers = list(executor._pool._processes.values())

        started = time.monotonic()
        executor.close()
        await asyncio.to_thread(lambda: [w.join(10) for w in workers])

        assert time.monotonic() - started < 10
        assert not any(w.is_alive() for w in workers)
        assert executor._pool is None
        executor.close()  # idempotent

    async def test_a_closed_executor_refuses_rather_than_building_a_new_pool(self) -> None:
        executor = ProcessPoolCpuExecutor(max_workers=1)
        executor.close()

        with pytest.raises(CoreException) as ei:
            await executor.run(os.getpid)

        assert ei.value.kind is ExceptionKind.UNAVAILABLE
        assert ei.value.code == "cpu_executor_closed"
        assert executor._pool is None

    async def test_first_use_from_two_threads_creates_one_pool(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import concurrent.futures

        real = concurrent.futures.ProcessPoolExecutor
        created: list[Any] = []

        def slow_pool(**kwargs: Any) -> Any:
            time.sleep(0.05)  # widen the window between the check and the assignment
            pool = real(**kwargs)
            created.append(pool)
            return pool

        monkeypatch.setattr(concurrent.futures, "ProcessPoolExecutor", slow_pool)
        executor = ProcessPoolCpuExecutor(max_workers=1)
        barrier = threading.Barrier(2)
        seen: list[object] = []

        def first_use() -> None:
            barrier.wait()
            seen.append(executor._require_pool())

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

    async def test_an_outcome_that_cannot_come_back_fails_only_its_own_call(self) -> None:
        executor = ProcessPoolCpuExecutor(max_workers=2)

        try:
            bad, good = await asyncio.gather(
                executor.run(_raise_unpicklable_back),
                executor.run(functools.partial(divmod, 7, 2)),
                return_exceptions=True,
            )

            assert good == (3, 1)  # the concurrent call is untouched ...
            assert isinstance(bad, CoreException)
            assert bad.code == "cpu_offload_result_not_picklable"
            assert "_UnpicklableBack" in str(bad.__cause__)  # with the worker's traceback
            assert await executor.run(os.getpid) != os.getpid()  # ... and the pool still works

        finally:
            executor.close()

    async def test_a_result_the_worker_cannot_pickle_fails_only_its_own_call(self) -> None:
        executor = ProcessPoolCpuExecutor(max_workers=1)

        try:
            with pytest.raises(CoreException) as ei:
                await executor.run(threading.Lock)

            assert ei.value.code == "cpu_offload_result_not_picklable"
            assert await executor.run(functools.partial(divmod, 7, 2)) == (3, 1)

        finally:
            executor.close()

    async def test_dead_worker_fails_its_call_and_the_next_call_gets_a_fresh_pool(self) -> None:
        executor = ProcessPoolCpuExecutor(max_workers=1)

        try:
            pid = await executor.run(os.getpid)
            running = asyncio.ensure_future(executor.run(functools.partial(time.sleep, 30)))
            await asyncio.sleep(0)  # one loop turn: the call is submitted, not finished
            assert executor._pool is not None and executor._pool._pending_work_items
            os.kill(pid, signal.SIGKILL)

            with pytest.raises(CoreException) as ei:
                await running

            assert ei.value.kind is ExceptionKind.UNAVAILABLE
            assert ei.value.code == "cpu_offload_worker_lost"
            assert isinstance(ei.value.__cause__, BrokenExecutor)
            assert await executor.run(os.getpid) != pid

        finally:
            executor.close()

    async def test_a_submit_racing_close_is_refused_as_closed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        executor = ProcessPoolCpuExecutor(max_workers=1)
        pool = executor._require_pool()
        executor.close()
        # The pool handed out just before the close, as a concurrent call would see it.
        monkeypatch.setattr(ProcessPoolCpuExecutor, "_require_pool", lambda self: pool)

        with pytest.raises(CoreException) as ei:
            await executor.run(os.getpid)

        assert ei.value.kind is ExceptionKind.UNAVAILABLE
        assert ei.value.code == "cpu_executor_closed"

    async def test_a_submit_racing_a_dropped_broken_pool_is_a_lost_worker(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        executor = ProcessPoolCpuExecutor(max_workers=1)
        pool = executor._require_pool()
        # Another call found the pool broken, dropped it and shut it down meanwhile.
        executor._pool = None
        pool.shutdown()
        monkeypatch.setattr(ProcessPoolCpuExecutor, "_require_pool", lambda self: pool)

        with pytest.raises(CoreException) as ei:
            await executor.run(os.getpid)

        assert ei.value.code == "cpu_offload_worker_lost"

    async def test_a_submit_error_from_an_open_executor_is_not_relabelled(self) -> None:
        executor = ProcessPoolCpuExecutor(max_workers=1)
        executor._require_pool().shutdown()  # not by close(), and still the current pool

        try:
            with pytest.raises(RuntimeError):
                await executor.run(os.getpid)

        finally:
            executor.close()

    async def test_an_abandoned_running_call_logs_one_warning(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        logger = MagicMock()
        monkeypatch.setattr(cpu_module, "logger", logger)
        executor = ProcessPoolCpuExecutor(max_workers=1)

        try:
            with (
                bind_isolated_cpu_executor(executor),
                bind_deadline(0.5),
                pytest.raises(CoreException),
            ):
                await run_cpu(time.sleep, 30, isolated=True)

        finally:
            executor.close()

        logger.warning.assert_called_once()
        assert logger.warning.call_args.args[1:] == ("sleep",)  # the call site, no arguments

    async def test_close_kills_a_worker_that_ignores_sigterm(self) -> None:
        executor = ProcessPoolCpuExecutor(max_workers=1)
        await executor.run(functools.partial(signal.signal, signal.SIGTERM, signal.SIG_IGN))

        with (
            bind_isolated_cpu_executor(executor),
            bind_deadline(0.2),
            pytest.raises(CoreException),
        ):
            await run_cpu(time.sleep, 30, isolated=True)

        assert executor._pool is not None
        workers = list(executor._pool._processes.values())
        executor.close()

        # Killed by close(), so it is reaped long before the 30 s sleep would end.
        await asyncio.to_thread(lambda: [w.join(5) for w in workers])
        assert not any(w.is_alive() for w in workers)


class TestInterpreterPoolCpuExecutor:
    async def test_refuses_construction_without_interpreter_pool(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cpu_module, "_interpreter_pool_type", lambda: None)

        with pytest.raises(CoreException) as ei:
            InterpreterPoolCpuExecutor()

        assert ei.value.kind is ExceptionKind.CONFIGURATION
        assert ei.value.code == "cpu_offload_interpreters_unavailable"

    @_needs_interpreters
    async def test_runs_in_other_interpreters_of_this_process(self) -> None:
        from concurrent import interpreters

        executor = InterpreterPoolCpuExecutor(max_workers=2)

        try:
            with bind_isolated_cpu_executor(executor):
                ran_in = await asyncio.gather(
                    run_cpu(interpreters.get_current, isolated=True),
                    run_cpu(interpreters.get_current, isolated=True),
                )
                pid = await run_cpu(os.getpid, isolated=True)

            main_id = interpreters.get_current().id
            assert all(interp.id != main_id for interp in ran_in)
            assert pid == os.getpid()

        finally:
            executor.close()

    @_needs_interpreters
    async def test_propagates_exception_with_remote_traceback(self) -> None:
        executor = InterpreterPoolCpuExecutor(max_workers=1)

        try:
            with pytest.raises(json.JSONDecodeError) as ei:
                await executor.run(functools.partial(json.loads, "not json"))

            assert ei.value.__cause__ is not None
            assert "decoder.py" in str(ei.value.__cause__)

        finally:
            executor.close()

    @_needs_interpreters
    async def test_a_closed_executor_refuses_rather_than_building_a_new_pool(self) -> None:
        executor = InterpreterPoolCpuExecutor(max_workers=1)
        executor.close()

        with pytest.raises(CoreException) as ei:
            await executor.run(os.getpid)

        assert ei.value.kind is ExceptionKind.UNAVAILABLE
        assert ei.value.code == "cpu_executor_closed"

    @_needs_interpreters
    async def test_a_submit_racing_close_is_refused_as_closed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        executor = InterpreterPoolCpuExecutor(max_workers=1)
        pool = executor._require_pool()
        executor.close()
        monkeypatch.setattr(InterpreterPoolCpuExecutor, "_require_pool", lambda self: pool)

        with pytest.raises(CoreException) as ei:
            await executor.run(os.getpid)

        assert ei.value.code == "cpu_executor_closed"

    @_needs_interpreters
    async def test_refuses_unshareable_call(self) -> None:
        executor = InterpreterPoolCpuExecutor(max_workers=1)

        try:
            with pytest.raises(CoreException) as ei:
                await executor.run(functools.partial(print, threading.Lock()), label="site.fn")

            assert ei.value.kind is ExceptionKind.CONFIGURATION
            assert ei.value.code == "cpu_offload_not_shareable"
            assert "site.fn" in str(ei.value)

        finally:
            executor.close()


class TestPickledInlineCpuExecutor:
    async def test_runs_the_call_on_a_pickled_copy(self) -> None:
        with bind_isolated_cpu_executor(InlineCpuExecutor(pickled=True)):
            assert await run_cpu(divmod, 7, 2, isolated=True) == (3, 1)

    async def test_refuses_a_lambda_as_a_worker_process_would(self) -> None:
        with (
            bind_isolated_cpu_executor(InlineCpuExecutor(pickled=True)),
            pytest.raises(CoreException) as ei,
        ):
            await run_cpu(lambda: 1, isolated=True)

        assert ei.value.code == "cpu_offload_not_picklable"

    async def test_raises_the_call_exception_with_its_traceback(self) -> None:
        with pytest.raises(json.JSONDecodeError) as ei:
            await InlineCpuExecutor(pickled=True).run(functools.partial(json.loads, "not json"))

        assert "decoder.py" in str(ei.value.__cause__)

    async def test_refuses_an_outcome_that_cannot_come_back(self) -> None:
        with pytest.raises(CoreException) as ei:
            await InlineCpuExecutor(pickled=True).run(_raise_unpicklable_back)

        assert ei.value.code == "cpu_offload_result_not_picklable"


class TestIsolatedRunCpu:
    async def test_refuses_when_no_isolated_executor_is_bound(self) -> None:
        assert current_isolated_cpu_executor() is None

        with pytest.raises(CoreException) as ei:
            await run_cpu(_double, 1, isolated=True)

        assert ei.value.kind is ExceptionKind.CONFIGURATION
        assert ei.value.code == "cpu_offload_isolated_unbound"
        assert "_double" in str(ei.value)

    async def test_uses_the_isolated_slot_with_a_bare_picklable_thunk(self) -> None:
        seen: list[object] = []

        class _Capture:
            async def run(self, fn, *, label=None):  # type: ignore[no-untyped-def]
                seen.append(fn)
                return fn()

        class _Unused:
            async def run(self, fn, *, label=None):  # type: ignore[no-untyped-def]
                raise AssertionError("the default executor must not run isolated work")

        with (
            bind_cpu_executor(_Unused()),  # type: ignore[arg-type]
            bind_isolated_cpu_executor(_Capture()),  # type: ignore[arg-type]
        ):
            assert await run_cpu(_double, 21, isolated=True) == 42

        # No context copy wrapped around it: the thunk pickles as it is.
        assert pickle.loads(pickle.dumps(seen[0]))() == 42

    async def test_binds_and_restores(self) -> None:
        inline = InlineCpuExecutor()

        with bind_isolated_cpu_executor(inline):
            assert current_isolated_cpu_executor() is inline
            assert isolated_cpu_executor_bound() is True

        assert current_isolated_cpu_executor() is None
        assert isolated_cpu_executor_bound() is False

    async def test_a_held_executor_reaches_a_context_that_binds_none(self) -> None:
        held, bound = InlineCpuExecutor(), InlineCpuExecutor()

        with hold_isolated_cpu_executor(held):
            # A fresh context binds nothing, like an ASGI request task.
            elsewhere = contextvars.Context()
            assert elsewhere.run(current_isolated_cpu_executor) is held
            assert elsewhere.run(isolated_cpu_executor_bound) is False

            with bind_isolated_cpu_executor(bound):
                assert current_isolated_cpu_executor() is bound  # a binding wins

        assert contextvars.Context().run(current_isolated_cpu_executor) is None

    async def test_runs_in_a_worker_process(self, process_executor: ProcessPoolCpuExecutor) -> None:
        with bind_isolated_cpu_executor(process_executor):
            assert await run_cpu(os.getpid, isolated=True) != os.getpid()
            assert await run_cpu(divmod, 7, 2, isolated=True) == (3, 1)

    async def test_refuses_a_local_function_naming_it(
        self, process_executor: ProcessPoolCpuExecutor
    ) -> None:
        def local_compute() -> int:
            return 1

        with bind_isolated_cpu_executor(process_executor), pytest.raises(CoreException) as ei:
            await run_cpu(local_compute, isolated=True)

        assert ei.value.code == "cpu_offload_not_picklable"
        assert "local_compute" in str(ei.value)

    async def test_cancellation_stops_the_wait(self) -> None:
        executor = ProcessPoolCpuExecutor(max_workers=1)

        try:
            with bind_isolated_cpu_executor(executor):
                task = asyncio.ensure_future(run_cpu(time.sleep, 30, isolated=True))
                await asyncio.sleep(0.2)
                task.cancel()

                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 10)

        finally:
            executor.close()
