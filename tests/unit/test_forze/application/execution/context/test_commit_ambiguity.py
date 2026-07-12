"""A deadline or cancellation that tears (or follows) a transaction commit is non-retryable.

If the invocation deadline — or a plain cancellation, e.g. a client disconnect — fires at
or after the root transaction's commit point, the commit may have (or has) landed — the
outcome is ambiguous. The engine surfaces a non-retryable ``commit_ambiguous`` error
rather than a retryable ``deadline_exceeded`` or a raw ``CancelledError``, so an
at-least-once caller does not blindly retry into a duplicate. A deadline or cancellation
during the body (before any commit) keeps its retryable semantics (nothing committed).
"""

from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import AsyncGenerator

import attrs
import pytest

from forze.application.contracts.execution import Handler
from forze.application.contracts.transaction import (
    IsolationLevel,
    TransactionManagerPort,
    TransactionScopeKey,
)
from forze.application.execution import ExecutionContext, bind_deadline
from forze.application.execution.context.active_operation import active_operation_var
from forze.application.execution.context.commit_state import (
    commit_started,
    mark_commit_started,
    reset_commit_started,
)
from forze.application.execution.context.transaction import TransactionContext
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockDepsModule
from forze_mock.adapters import MockTxManagerAdapter
from tests.support.execution_context import context_from_deps

# ----------------------- #


class _SlowCommitTxManager(TransactionManagerPort):
    """Manager whose commit (the post-yield step) awaits, so a deadline can tear it."""

    def __init__(self, commit_delay: float = 10.0) -> None:
        self.commit_delay = commit_delay

    @property
    def scope_key(self) -> TransactionScopeKey:
        return TransactionScopeKey("slow")

    def transaction(
        self, *, read_only: bool = False, isolation: IsolationLevel | None = None
    ) -> AbstractAsyncContextManager[None]:
        delay = self.commit_delay

        @asynccontextmanager
        async def _cm() -> AsyncGenerator[None]:
            yield
            # Post-yield == the driver commit; await so a deadline can land mid-commit.
            await asyncio.sleep(delay)

        return _cm()


class _RecordingTxManager(TransactionManagerPort):
    """Records the outcome; an optional commit delay lets a cancellation land mid-commit.

    ``in_commit`` is set right before the commit await, so a test can cancel the
    driving task while the scope is parked inside the driver commit.
    """

    def __init__(self, commit_delay: float = 0.0) -> None:
        self.commit_delay = commit_delay
        self.committed = False
        self.rolled_back = False
        self.in_commit = asyncio.Event()

    @property
    def scope_key(self) -> TransactionScopeKey:
        return TransactionScopeKey("rec")

    def transaction(
        self, *, read_only: bool = False, isolation: IsolationLevel | None = None
    ) -> AbstractAsyncContextManager[None]:
        @asynccontextmanager
        async def _cm() -> AsyncGenerator[None]:
            try:
                yield

                if self.commit_delay > 0:
                    self.in_commit.set()
                    await asyncio.sleep(self.commit_delay)

            except BaseException:
                self.rolled_back = True
                raise

            self.committed = True

        return _cm()


def _tx_with(manager: TransactionManagerPort) -> TransactionContext:
    tx = TransactionContext()
    tx.lock(lambda _route: manager)
    return tx


@pytest.fixture
def ctx() -> ExecutionContext:
    return context_from_deps(MockDepsModule()())


# ----------------------- #


class TestCommitStartedFlag:
    """The transaction scope marks the commit point; the mark survives to the boundary."""

    @pytest.mark.asyncio
    async def test_flag_set_when_commit_is_torn(self) -> None:
        reset_commit_started()
        tx = _tx_with(_SlowCommitTxManager(commit_delay=10.0))

        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.02):
                async with tx.scope("slow"):
                    pass  # body instant; the commit (post-yield sleep) is torn

        assert commit_started() is True
        reset_commit_started()

    @pytest.mark.asyncio
    async def test_flag_not_set_when_body_is_torn(self) -> None:
        reset_commit_started()
        tx = _tx_with(MockTxManagerAdapter())  # instant, awaitless commit

        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.02):
                async with tx.scope("mock"):
                    await asyncio.sleep(10)  # torn in the body -> rollback, no mark

        assert commit_started() is False

    @pytest.mark.asyncio
    async def test_flag_set_after_a_clean_commit(self) -> None:
        reset_commit_started()
        tx = _tx_with(MockTxManagerAdapter())

        async with tx.scope("mock"):
            pass

        # Marked on the clean path too; the boundary (not the scope) clears it.
        assert commit_started() is True
        reset_commit_started()


class TestCommitAmbiguousClassification:
    """The invocation boundary reclassifies a deadline based on the commit mark."""

    @pytest.mark.asyncio
    async def test_deadline_at_or_after_commit_is_non_retryable(
        self, ctx: ExecutionContext
    ) -> None:
        reset_commit_started()

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class CommitThenStallHandler(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                # Stand-in for the tx scope reaching its commit point (proven to be
                # set there by TestCommitStartedFlag); then stall so the deadline
                # fires with the commit already marked.
                mark_commit_started()
                await asyncio.Event().wait()
                return args

        reg = OperationRegistry(
            handlers={"op": lambda _c: CommitThenStallHandler()}
        ).freeze()
        resolved = reg.resolve("op", ctx)

        with bind_deadline(0.05):
            with pytest.raises(CoreException) as ei:
                await resolved("x")

        assert ei.value.kind is ExceptionKind.INTERNAL
        assert ei.value.code == "commit_ambiguous"
        # The top-level boundary cleared the flag so it does not leak to a sibling op.
        assert commit_started() is False

    @pytest.mark.asyncio
    async def test_deadline_before_commit_stays_retryable(
        self, ctx: ExecutionContext
    ) -> None:
        reset_commit_started()

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class StallHandler(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                await asyncio.Event().wait()  # never marks a commit
                return args

        reg = OperationRegistry(handlers={"op": lambda _c: StallHandler()}).freeze()
        resolved = reg.resolve("op", ctx)

        with bind_deadline(0.05):
            with pytest.raises(CoreException) as ei:
                await resolved("x")

        assert ei.value.kind is ExceptionKind.TIMEOUT
        assert ei.value.code == "deadline_exceeded"

    @pytest.mark.asyncio
    async def test_stale_flag_from_out_of_operation_commit_does_not_false_positive(
        self, ctx: ExecutionContext
    ) -> None:
        # Simulate a root transaction that committed OUTSIDE an operation boundary (the
        # inbox consumer's per-message tx, cross-aggregate invariant enforcement): the
        # flag is left set. The next operation must reset it at entry, so a body-timeout
        # stays a retryable deadline rather than a false ``commit_ambiguous``.
        reset_commit_started()
        mark_commit_started()  # stale, from before this operation

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class StallHandler(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                await asyncio.Event().wait()  # times out in the body; never commits
                return args

        reg = OperationRegistry(handlers={"op": lambda _c: StallHandler()}).freeze()
        resolved = reg.resolve("op", ctx)

        with bind_deadline(0.05):
            with pytest.raises(CoreException) as ei:
                await resolved("x")

        assert ei.value.kind is ExceptionKind.TIMEOUT
        assert ei.value.code == "deadline_exceeded"


class TestCancellationAtCommit:
    """A plain cancellation is classified by where it lands relative to the commit point."""

    @pytest.mark.asyncio
    async def test_cancel_landing_in_commit_surfaces_commit_ambiguous(self) -> None:
        # A cancel (a client disconnect) tearing the driver commit of an operation's
        # transaction is the same ambiguity as a deadline tearing it: non-retryable.
        reset_commit_started()
        manager = _RecordingTxManager(commit_delay=10.0)
        tx = _tx_with(manager)

        async def driver() -> None:
            # The invoke boundary marks the operation's owning task; the scope
            # converts a cancel only inside that boundary.
            task = asyncio.current_task()
            assert task is not None
            active_operation_var.set(task)

            async with tx.scope("rec"):
                pass

        task = asyncio.create_task(driver())
        await manager.in_commit.wait()
        task.cancel()

        with pytest.raises(CoreException) as ei:
            await task

        assert ei.value.kind is ExceptionKind.INTERNAL
        assert ei.value.code == "commit_ambiguous"
        assert task.cancelled() is False  # converted, not cancelled
        assert manager.committed is False  # torn mid-commit: outcome unknown
        reset_commit_started()

    @pytest.mark.asyncio
    async def test_cancel_during_body_rolls_back_and_reraises(self) -> None:
        reset_commit_started()
        manager = _RecordingTxManager()
        tx = _tx_with(manager)
        in_body = asyncio.Event()

        async def driver() -> None:
            task = asyncio.current_task()
            assert task is not None
            active_operation_var.set(task)

            async with tx.scope("rec"):
                in_body.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(driver())
        await in_body.wait()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert task.cancelled() is True  # retryable-by-cancellation, unchanged
        assert manager.rolled_back is True
        assert manager.committed is False

    @pytest.mark.asyncio
    async def test_cancel_during_post_commit_drain_surfaces_commit_ambiguous(
        self,
    ) -> None:
        reset_commit_started()
        manager = _RecordingTxManager()
        tx = _tx_with(manager)
        in_drain = asyncio.Event()
        release = asyncio.Event()
        drained = False

        async def cb() -> None:
            nonlocal drained
            in_drain.set()
            await release.wait()
            drained = True

        async def driver() -> None:
            task = asyncio.current_task()
            assert task is not None
            active_operation_var.set(task)

            async with tx.scope("rec"):
                await tx.run_or_defer(cb)

        task = asyncio.create_task(driver())
        await in_drain.wait()
        task.cancel()
        release.set()

        with pytest.raises(CoreException) as ei:
            await task

        assert ei.value.kind is ExceptionKind.INTERNAL
        assert ei.value.code == "commit_ambiguous"
        assert manager.committed is True  # committed; the caller still must not retry
        assert drained is True  # the shielded drain ran to completion first
        reset_commit_started()

    @pytest.mark.asyncio
    async def test_cancel_in_commit_outside_an_operation_stays_cancelled(self) -> None:
        # Framework-internal transactions outside an operation boundary (an inbox
        # consumer's per-message tx, a saga step) keep raw cancellation semantics:
        # a shutdown cancel must reach the consumer loop as a cancel, never be
        # reclassified into an error a poison-handling loop would swallow.
        reset_commit_started()
        manager = _RecordingTxManager(commit_delay=10.0)
        tx = _tx_with(manager)

        async def driver() -> None:
            async with tx.scope("rec"):
                pass

        task = asyncio.create_task(driver())
        await manager.in_commit.wait()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert task.cancelled() is True


class TestCancellationThroughOperationBoundary:
    """A client-disconnect cancel tearing an operation's commit is non-retryable end-to-end."""

    @pytest.mark.asyncio
    async def test_cancel_tearing_operation_commit_is_non_retryable(
        self, ctx: ExecutionContext
    ) -> None:
        reset_commit_started()
        manager = _RecordingTxManager(commit_delay=10.0)
        tx = _tx_with(manager)

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class SlowCommitHandler(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                async with tx.scope("rec"):
                    return args

        reg = OperationRegistry(
            handlers={"op": lambda _c: SlowCommitHandler()}
        ).freeze()
        resolved = reg.resolve("op", ctx)

        task = asyncio.create_task(resolved("x"))
        await manager.in_commit.wait()
        task.cancel()

        with pytest.raises(CoreException) as ei:
            await task

        assert ei.value.kind is ExceptionKind.INTERNAL
        assert ei.value.code == "commit_ambiguous"
        assert task.cancelled() is False  # converted, not cancelled
        assert manager.committed is False  # torn mid-commit: outcome unknown
