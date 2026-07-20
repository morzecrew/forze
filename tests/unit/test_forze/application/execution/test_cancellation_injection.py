"""Cancellation-injection harness for the operation pipeline.

Cancels the operation task at a chosen pipeline stage — before hook, inside
the transactional handler, or during the post-commit deferred drain — and
asserts the engine's cancellation invariants:

- cancellation before/inside the transaction rolls back: after-commit hooks
  never run, the handler is not re-entered, and the engine stays healthy;
- cancellation during the post-commit drain cannot tear it: every after-commit
  hook still runs (the transaction is committed) and the cancellation then
  surfaces as a non-retryable ``commit_ambiguous`` error (the commit landed —
  the caller must not see a plain cancel it could retry into a duplicate);
- in every case the drain gate releases its slot and a fresh operation runs
  normally afterwards.
"""

from __future__ import annotations

import asyncio

import attrs
import pytest

from forze.application.contracts.execution import BeforeStep, Handler, OnSuccessStep
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_deps

# ----------------------- #

STAGE_BEFORE = "before"
STAGE_HANDLER = "handler"
STAGE_AFTER_COMMIT_DRAIN = "after_commit_drain"


@pytest.fixture
def ctx() -> ExecutionContext:
    return context_from_deps(MockDepsModule()())


@attrs.define(slots=True)
class _Probe:
    """Shared stage log + synchronization points for one injected run."""

    events: list[str] = attrs.field(factory=list)
    reached: asyncio.Event = attrs.field(factory=asyncio.Event)
    release: asyncio.Event = attrs.field(factory=asyncio.Event)

    # ....................... #

    async def hang(self) -> None:
        """Signal the orchestrator and park until cancelled."""

        self.reached.set()
        await asyncio.Event().wait()


@attrs.define(slots=True, kw_only=True)
class _StageHandler(Handler[str, str]):
    probe: _Probe
    stage: str

    async def __call__(self, args: str) -> str:
        self.probe.events.append("handler:start")

        if self.stage == STAGE_HANDLER:
            await self.probe.hang()

        self.probe.events.append("handler:done")

        return args


@attrs.define(slots=True, kw_only=True)
class _Echo(Handler[str, str]):
    async def __call__(self, args: str) -> str:
        return args


def _build(probe: _Probe, stage: str) -> FrozenOperationRegistry:
    """One operation with a before hook, a mock-route tx, and two after-commit hooks."""

    def _before_factory(_ctx: ExecutionContext):
        async def before(_args: str) -> None:
            probe.events.append("before:start")

            if stage == STAGE_BEFORE:
                await probe.hang()

            probe.events.append("before:done")

        return before

    def _ac_factory(name: str, *, blocking: bool):
        def factory(_ctx: ExecutionContext):
            async def on_success(_args: str, _result: str) -> None:
                if blocking and stage == STAGE_AFTER_COMMIT_DRAIN:
                    probe.reached.set()
                    await probe.release.wait()

                probe.events.append(name)

            return on_success

        return factory

    return (
        OperationRegistry(
            handlers={
                "op": lambda _ctx: _StageHandler(probe=probe, stage=stage),
                "noop": lambda _ctx: _Echo(),
            }
        )
        .bind("op")
        .bind_outer()
        .before(BeforeStep(id="b", factory=_before_factory))
        .finish()
        .bind_tx()
        .set_route("mock")
        .after_commit(
            OnSuccessStep(id="ac1", factory=_ac_factory("ac1", blocking=True)),
            OnSuccessStep(id="ac2", factory=_ac_factory("ac2", blocking=False)),
        )
        .finish(deep=True)
        .freeze()
    )


async def _run_and_cancel(
    reg: FrozenOperationRegistry,
    ctx: ExecutionContext,
    probe: _Probe,
    *,
    release_after_cancel: bool = False,
    expect_commit_ambiguous: bool = False,
) -> None:
    """Invoke ``op`` as a task, cancel it once the stage is reached, await the outcome.

    Before the commit point the cancellation re-raises raw; at or after it the
    engine converts it to a non-retryable ``commit_ambiguous`` error instead.
    """

    task = asyncio.create_task(reg.resolve("op", ctx)("x"))

    await probe.reached.wait()
    task.cancel()

    if release_after_cancel:
        await asyncio.sleep(0)
        probe.release.set()

    if expect_commit_ambiguous:
        with pytest.raises(CoreException) as ei:
            await task

        assert ei.value.kind is ExceptionKind.INTERNAL
        assert ei.value.code == "commit_ambiguous"
        return

    with pytest.raises(asyncio.CancelledError):
        await task


async def _assert_engine_healthy(
    reg: FrozenOperationRegistry, ctx: ExecutionContext
) -> None:
    """The shared engine state survived: gate slot released, fresh ops run."""

    assert ctx.drain_gate.in_flight == 0
    assert await reg.resolve("noop", ctx)("ping") == "ping"


# ----------------------- #


class TestCancellationInjection:
    @pytest.mark.asyncio
    async def test_cancel_in_before_hook_skips_handler_and_commit(
        self, ctx: ExecutionContext
    ) -> None:
        probe = _Probe()
        reg = _build(probe, STAGE_BEFORE)

        await _run_and_cancel(reg, ctx, probe)

        # The pre-guard was interrupted: no handler, no transaction, no
        # after-commit work.
        assert probe.events == ["before:start"]
        await _assert_engine_healthy(reg, ctx)

    @pytest.mark.asyncio
    async def test_cancel_in_handler_rolls_back_and_skips_after_commit(
        self, ctx: ExecutionContext
    ) -> None:
        probe = _Probe()
        reg = _build(probe, STAGE_HANDLER)

        await _run_and_cancel(reg, ctx, probe)

        # The transaction followed the rollback path: the handler never
        # completed and the after-commit hooks never ran.
        assert probe.events == ["before:start", "before:done", "handler:start"]
        await _assert_engine_healthy(reg, ctx)

    @pytest.mark.asyncio
    async def test_cancel_during_after_commit_drain_completes_all_hooks(
        self, ctx: ExecutionContext
    ) -> None:
        probe = _Probe()
        reg = _build(probe, STAGE_AFTER_COMMIT_DRAIN)

        await _run_and_cancel(
            reg, ctx, probe, release_after_cancel=True, expect_commit_ambiguous=True
        )

        # The transaction committed before the cancellation landed, so the
        # drain is a critical section: every after-commit hook ran, and the
        # cancellation surfaced only afterwards — as ``commit_ambiguous``, not
        # as a plain cancel the caller could retry into a duplicate.
        assert probe.events[:3] == ["before:start", "before:done", "handler:start"]
        assert "handler:done" in probe.events
        assert "ac1" in probe.events
        assert "ac2" in probe.events
        await _assert_engine_healthy(reg, ctx)

    @pytest.mark.asyncio
    async def test_uncancelled_run_executes_full_pipeline(
        self, ctx: ExecutionContext
    ) -> None:
        """Baseline: the same plan without injection runs every stage in order."""

        probe = _Probe()
        reg = _build(probe, stage="none")

        assert await reg.resolve("op", ctx)("x") == "x"
        assert probe.events == [
            "before:start",
            "before:done",
            "handler:start",
            "handler:done",
            "ac1",
            "ac2",
        ]
        await _assert_engine_healthy(reg, ctx)
