"""Unit tests for :func:`~forze_temporal.temporal_worker_lifecycle_step`.

The worker's own polling belongs to the SDK and is exercised against a real server; what is
checked here is the *supervision* the step wraps around it — the wiring refusals, the boot
failure when the client is not connected yet, the stop ordering, and the crash ceiling.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import ClassVar
from unittest.mock import MagicMock, patch

import pytest

from forze.base.exceptions import CoreException, ExceptionKind

pytest.importorskip("temporalio")

from forze.application.execution import Deps, ExecutionRuntime
from forze.testing import context_from_deps
from forze_temporal import (
    DEFAULT_WORKER_GRACEFUL_SHUTDOWN,
    temporal_worker_lifecycle_step,
)
from forze_temporal.kernel.client import TemporalClient

# ----------------------- #


class _StubWorker:
    """Stands in for ``temporalio.worker.Worker``; records how it was built and stopped."""

    # Per-class on purpose: each subclass gets its own registry, reset by the fixture
    # below, so one test's workers can never be counted by another's.
    instances: ClassVar[list[_StubWorker]] = []

    def __init__(self, client, **kwargs) -> None:
        self.client = client
        self.kwargs = kwargs
        self.is_shutdown = False
        self.stopping_at_shutdown: bool | None = None
        self.run_calls = 0
        self._stopped = asyncio.Event()
        type(self).instances.append(self)

    async def run(self) -> None:
        self.run_calls += 1
        await self._stopped.wait()

    async def shutdown(self) -> None:
        self.is_shutdown = True
        self._stopped.set()


class _CrashingWorker(_StubWorker):
    """A worker whose every run dies immediately — the crash-loop subject."""

    async def run(self) -> None:
        self.run_calls += 1
        raise RuntimeError("poller died")


class _FailingShutdownWorker(_StubWorker):
    """Its shutdown raises — a broken bridge, a connection already gone."""

    async def shutdown(self) -> None:
        self.is_shutdown = True
        self._stopped.set()

        raise RuntimeError("shutdown blew up")


class _UnbuildableWorker(_StubWorker):
    """Construction fails, as it does for an undecorated workflow class."""

    def __init__(self, client, **kwargs) -> None:
        super().__init__(client, **kwargs)

        raise ValueError("Workflow Foo missing attributes, was it decorated?")


@pytest.fixture(autouse=True)
def _reset_stub_workers():
    for kind in (_StubWorker, _CrashingWorker, _UnbuildableWorker, _FailingShutdownWorker):
        kind.instances = []

    yield

    for kind in (_StubWorker, _CrashingWorker, _UnbuildableWorker, _FailingShutdownWorker):
        kind.instances = []


def _connected_client() -> TemporalClient:
    client = TemporalClient()
    object.__setattr__(client, "_TemporalClient__client", MagicMock(name="native"))

    return client


def _ctx():
    return context_from_deps(Deps.plain({}))


class _Wf:
    """Registered workflow stand-in; the step only forwards it to the SDK."""


# ----------------------- #


class TestWiringRefusals:
    """Configuration that cannot work is refused where it is written."""

    def test_empty_registration_is_refused(self) -> None:
        """A worker with nothing registered polls forever and answers nothing.

        Its workflow tasks time out and retry, so the queue silently stalls behind a
        process that looks perfectly healthy.
        """

        with pytest.raises(CoreException, match="registers no workflows") as excinfo:
            temporal_worker_lifecycle_step(
                client=_connected_client(),
                task_queue="tq",
            )

        assert excinfo.value.kind is ExceptionKind.CONFIGURATION

    @pytest.mark.parametrize("grace", [timedelta(0), timedelta(seconds=-1)])
    def test_non_positive_graceful_shutdown_is_refused(self, grace: timedelta) -> None:
        """Zero grace means cutting activities off mid-flight at every deploy."""

        with pytest.raises(CoreException, match="graceful_shutdown must be positive"):
            temporal_worker_lifecycle_step(
                client=_connected_client(),
                task_queue="tq",
                workflows=[_Wf],
                graceful_shutdown=grace,
            )

    def test_default_window_fits_the_runtime_drain_budget(self) -> None:
        """The default must be a window the default runtime can actually honour.

        ``stop_all`` gives every drainable loop one shared deadline of
        ``shutdown_step_timeout``, and cancels them **all** when it fires. A worker
        window larger than that budget therefore does not merely fail to be honoured —
        it spends the other loops' shutdown time too. Pinned rather than commented,
        because the two constants live in different packages and would drift silently.
        """

        assert ExecutionRuntime().shutdown_step_timeout >= DEFAULT_WORKER_GRACEFUL_SHUTDOWN

    def test_step_declares_it_needs_a_long_running_host(self) -> None:
        """A serverless profile must refuse this step at assembly, not at 3am."""

        step = temporal_worker_lifecycle_step(
            client=_connected_client(),
            task_queue="tq",
            workflows=[_Wf],
        )

        assert step.requires_long_running is True


# ....................... #


class TestStartup:
    """What startup does, and what it refuses to defer into a detached task."""

    @pytest.mark.asyncio
    async def test_unconnected_client_fails_at_boot(self) -> None:
        """A lifecycle that ordered the worker before the client says so immediately.

        Resolving ``native`` inside the supervised task instead would turn a wiring
        mistake into an endless crash-restart loop against a client that never connects.
        """

        step = temporal_worker_lifecycle_step(
            client=TemporalClient(),
            task_queue="tq",
            workflows=[_Wf],
        )

        with pytest.raises(CoreException, match="not initialized"):
            await step.startup(_ctx())

        assert step.startup.task is None  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_startup_registers_the_loop_for_drain(self) -> None:
        """The runtime stops registered loops before teardown; an unregistered one is cancelled."""

        step = temporal_worker_lifecycle_step(
            client=_connected_client(),
            task_queue="tq",
            workflows=[_Wf],
        )
        ctx = _ctx()

        with patch("forze_temporal.execution.lifecycle.worker.Worker", _StubWorker):
            await step.startup(ctx)
            await asyncio.sleep(0)

            try:
                assert step.startup.loop_name in {  # type: ignore[attr-defined]
                    loop.loop_name for loop in ctx.drainables.loops
                }

            finally:
                await step.shutdown(ctx)

    @pytest.mark.asyncio
    async def test_worker_is_built_on_the_shared_connection(self) -> None:
        """One connection: the worker rides ``native``, codec and interceptors included."""

        client = _connected_client()
        step = temporal_worker_lifecycle_step(
            client=client,
            task_queue="orders-tq",
            workflows=[_Wf],
            max_concurrent_activities=7,
            graceful_shutdown=timedelta(seconds=11),
        )
        ctx = _ctx()

        with patch("forze_temporal.execution.lifecycle.worker.Worker", _StubWorker):
            await step.startup(ctx)
            await asyncio.sleep(0)

            worker = _StubWorker.instances[0]
            assert worker.client is client.native
            assert worker.kwargs["task_queue"] == "orders-tq"
            assert worker.kwargs["max_concurrent_activities"] == 7
            assert worker.kwargs["graceful_shutdown_timeout"] == timedelta(seconds=11)

            await step.shutdown(ctx)

    @pytest.mark.asyncio
    async def test_duplicate_startup_does_not_orphan_a_worker(self) -> None:
        """A second startup on the same hook must not leak the first worker's task."""

        step = temporal_worker_lifecycle_step(
            client=_connected_client(),
            task_queue="tq",
            workflows=[_Wf],
        )
        ctx = _ctx()

        with patch("forze_temporal.execution.lifecycle.worker.Worker", _StubWorker):
            await step.startup(ctx)
            await asyncio.sleep(0)
            first = step.startup.task  # type: ignore[attr-defined]

            await step.startup(ctx)
            await asyncio.sleep(0)

            assert step.startup.task is first  # type: ignore[attr-defined]
            assert len(_StubWorker.instances) == 1

            await step.shutdown(ctx)


# ....................... #


class TestStop:
    """Bringing the worker to rest."""

    @pytest.mark.asyncio
    async def test_stop_signals_before_shutting_the_worker_down(self) -> None:
        """The ordering that keeps teardown from spawning a fresh worker.

        ``run_supervised`` reads a run that returns with no stop pending as a fault and
        restarts it. Shutting the worker down first would therefore race a new worker into
        existence during shutdown — and that one nobody stops.
        """

        recorded: list[bool] = []
        step = temporal_worker_lifecycle_step(
            client=_connected_client(),
            task_queue="tq",
            workflows=[_Wf],
        )
        startup = step.startup
        ctx = _ctx()

        class _Recording(_StubWorker):
            async def shutdown(self) -> None:
                recorded.append(startup.control.stopping)  # type: ignore[attr-defined]
                await super().shutdown()

        with patch("forze_temporal.execution.lifecycle.worker.Worker", _Recording):
            await step.startup(ctx)
            await asyncio.sleep(0)
            await step.shutdown(ctx)

        assert recorded == [True]

    @pytest.mark.asyncio
    async def test_stop_ends_the_loop_without_a_restart(self) -> None:
        """A stopped worker stays stopped — exactly one worker was ever built."""

        step = temporal_worker_lifecycle_step(
            client=_connected_client(),
            task_queue="tq",
            workflows=[_Wf],
        )
        ctx = _ctx()

        with patch("forze_temporal.execution.lifecycle.worker.Worker", _StubWorker):
            await step.startup(ctx)
            await asyncio.sleep(0)
            await step.shutdown(ctx)
            await asyncio.sleep(0.05)

            assert len(_StubWorker.instances) == 1
            assert step.startup.task.done()  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_shutdown_before_the_first_tick_never_builds_a_worker(self) -> None:
        """Startup and shutdown back to back — a failed boot, a health-check flap.

        The supervised task has not run yet, so ``stop`` finds no worker to shut down.
        Without the stop check at the top of the run, the task would then build one that
        nobody asks to stop: it polls out the whole grace window and is cancelled
        mid-task, which is precisely the blunt teardown drain exists to avoid.
        """

        step = temporal_worker_lifecycle_step(
            client=_connected_client(),
            task_queue="tq",
            workflows=[_Wf],
        )
        ctx = _ctx()

        with patch("forze_temporal.execution.lifecycle.worker.Worker", _StubWorker):
            await step.startup(ctx)
            await step.shutdown(ctx)
            await asyncio.sleep(0.05)

        assert _StubWorker.instances == []
        assert step.startup.task.done()  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_stop_returns_within_its_deadline(self) -> None:
        """A worker that will not stop must not spend the deadline it was handed.

        The runtime stops every drainable loop against one shared deadline and cancels
        them **all** when it fires, so a ``stop`` that waits out its own graceful window
        regardless is charging other loops for its overrun.
        """

        class _WedgedWorker(_StubWorker):
            """Shutdown never completes — the activity that will not let go."""

            async def shutdown(self) -> None:
                self.is_shutdown = True
                await asyncio.Event().wait()

        step = temporal_worker_lifecycle_step(
            client=_connected_client(),
            task_queue="tq",
            workflows=[_Wf],
            graceful_shutdown=timedelta(seconds=30),
        )
        ctx = _ctx()

        with patch("forze_temporal.execution.lifecycle.worker.Worker", _WedgedWorker):
            await step.startup(ctx)
            await asyncio.sleep(0)

            clock = asyncio.get_running_loop()
            started = clock.time()
            stopped = await step.startup.stop(deadline=started + 0.2)  # type: ignore[attr-defined]
            elapsed = clock.time() - started

        assert stopped is False  # it never came to rest on its own
        assert elapsed < 2.0, f"stop overran its 0.2s deadline by {elapsed:.1f}s"

    @pytest.mark.asyncio
    async def test_a_failing_worker_shutdown_still_stops_the_loop(self) -> None:
        """Asking the worker to stop is advisory; bringing the loop down is the outcome.

        A shutdown that raises must not take the rest of the stop sequence with it — the
        loop would keep running until something else cancelled it — and it must not
        vanish either.
        """

        step = temporal_worker_lifecycle_step(
            client=_connected_client(),
            task_queue="tq",
            workflows=[_Wf],
        )
        ctx = _ctx()

        recorded = MagicMock()

        with (
            patch(
                "forze_temporal.execution.lifecycle.worker.Worker",
                _FailingShutdownWorker,
            ),
            patch("forze_temporal.execution.lifecycle.worker.logger", recorded),
        ):
            await step.startup(ctx)
            await asyncio.sleep(0)

            # Must not raise: the failure is the worker's, not the stop sequence's.
            await step.shutdown(ctx)
            await asyncio.sleep(0)

        assert step.startup.task.done()  # type: ignore[attr-defined]
        assert recorded.error.call_count == 1
        assert "shutdown failed" in recorded.error.call_args.args[0]

    @pytest.mark.asyncio
    async def test_stop_is_idempotent(self) -> None:
        """The runtime stops loops before teardown, then the step's own hook asks again."""

        step = temporal_worker_lifecycle_step(
            client=_connected_client(),
            task_queue="tq",
            workflows=[_Wf],
        )
        ctx = _ctx()

        with patch("forze_temporal.execution.lifecycle.worker.Worker", _StubWorker):
            await step.startup(ctx)
            await asyncio.sleep(0)
            await step.shutdown(ctx)
            await step.shutdown(ctx)


# ....................... #


class TestCrashSupervision:
    """A worker that dies is rebuilt; a worker that keeps dying is not."""

    @pytest.mark.asyncio
    async def test_crashing_worker_is_rebuilt(self) -> None:
        """Restart means a *new* worker — a shut-down or crashed one cannot be re-run."""

        step = temporal_worker_lifecycle_step(
            client=_connected_client(),
            task_queue="tq",
            workflows=[_Wf],
            restart_backoff=timedelta(milliseconds=1),
            max_consecutive_crashes=3,
        )
        ctx = _ctx()

        with patch("forze_temporal.execution.lifecycle.worker.Worker", _CrashingWorker):
            await step.startup(ctx)
            await asyncio.wait_for(step.startup.task, timeout=5)  # type: ignore[attr-defined]

        assert len(_CrashingWorker.instances) == 3
        assert all(worker.run_calls == 1 for worker in _CrashingWorker.instances)

    @pytest.mark.asyncio
    async def test_a_worker_that_cannot_be_built_is_terminal(self) -> None:
        """Building a worker is validation, so a failure there never gets better.

        Treated as an ordinary crash it would rebuild-and-fail forever — one critical
        log per backoff, for the lifetime of a process that will never do any work.
        """

        step = temporal_worker_lifecycle_step(
            client=_connected_client(),
            task_queue="tq",
            workflows=[_Wf],
            restart_backoff=timedelta(milliseconds=1),
        )
        ctx = _ctx()

        with patch("forze_temporal.execution.lifecycle.worker.Worker", _UnbuildableWorker):
            await step.startup(ctx)
            await asyncio.wait_for(step.startup.task, timeout=5)  # type: ignore[attr-defined]
            await asyncio.sleep(0.05)

        # One attempt, and no ceiling was needed to stop it.
        assert len(_UnbuildableWorker.instances) == 1
        assert step.startup.task.done()  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_crash_ceiling_ends_supervision(self) -> None:
        """Terminal rather than a quieter kind of down: the loop stops and stays stopped."""

        step = temporal_worker_lifecycle_step(
            client=_connected_client(),
            task_queue="tq",
            workflows=[_Wf],
            restart_backoff=timedelta(milliseconds=1),
            max_consecutive_crashes=2,
        )
        ctx = _ctx()

        with patch("forze_temporal.execution.lifecycle.worker.Worker", _CrashingWorker):
            await step.startup(ctx)
            await asyncio.wait_for(step.startup.task, timeout=5)  # type: ignore[attr-defined]
            await asyncio.sleep(0.05)

        assert len(_CrashingWorker.instances) == 2
        assert step.startup.task.done()  # type: ignore[attr-defined]
