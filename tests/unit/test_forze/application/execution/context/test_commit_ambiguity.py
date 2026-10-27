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
from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager, asynccontextmanager, nullcontext
from types import SimpleNamespace
from typing import Any

import attrs
import pytest
import structlog
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

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
from forze.application.execution.operations.run import invoke as invoke_module
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

        with bind_deadline(0.05), pytest.raises(CoreException) as ei:
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

        with bind_deadline(0.05), pytest.raises(CoreException) as ei:
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

        with bind_deadline(0.05), pytest.raises(CoreException) as ei:
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

    @pytest.mark.asyncio
    async def test_cancel_after_the_scope_leaves_the_mark_for_the_canceller(
        self, ctx: ExecutionContext
    ) -> None:
        # The transaction committed and its scope is gone, so nothing converts the cancel;
        # the boundary that cancelled reads the mark instead of a plain interruption.
        reset_commit_started()
        manager = _RecordingTxManager()
        tx = _tx_with(manager)
        parked = asyncio.Event()

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class CommitThenWait(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                async with tx.scope("rec"):
                    pass

                parked.set()
                await asyncio.sleep(10)
                return args

        reg = OperationRegistry(handlers={"op": lambda _c: CommitThenWait()}).freeze()
        resolved = reg.resolve("op", ctx)

        async def _canceller_view() -> bool:
            try:
                await resolved("x")

            except asyncio.CancelledError:
                return commit_started()

            return False

        task = asyncio.create_task(_canceller_view())
        await parked.wait()
        task.cancel()

        assert manager.committed is True
        assert await task is True


class TestCancellationAfterCommitIsRecorded:
    """A cancel the scope no longer covers is recorded at the top-level boundary, not converted.

    Only the CLI owns its result channel and its process, so every other boundary keeps
    the raw cancellation; the warning log and the span event are what tell an operator
    the commit may have landed.
    """

    @staticmethod
    def _tracer() -> tuple[Any, InMemorySpanExporter]:
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))

        return provider.get_tracer("test"), exporter

    @staticmethod
    def _ambiguous(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [entry for entry in logs if entry.get("code") == "commit_ambiguous"]

    @staticmethod
    async def _cancel_while_parked(
        resolved: Any, parked: asyncio.Event, tracer: Any
    ) -> asyncio.Task[None]:
        async def _drive() -> None:
            with tracer.start_as_current_span("boundary"):
                await resolved("x")

        task = asyncio.create_task(_drive())
        await parked.wait()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        return task

    @pytest.mark.asyncio
    async def test_cancel_after_the_scope_is_logged_and_still_cancels(
        self, ctx: ExecutionContext
    ) -> None:
        reset_commit_started()
        manager = _RecordingTxManager()
        tx = _tx_with(manager)
        parked = asyncio.Event()

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class CommitThenWait(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                async with tx.scope("rec"):
                    pass

                parked.set()
                await asyncio.sleep(10)
                return args

        reg = OperationRegistry(handlers={"op": lambda _c: CommitThenWait()}).freeze()
        tracer, exporter = self._tracer()

        with structlog.testing.capture_logs() as logs:
            task = await self._cancel_while_parked(reg.resolve("op", ctx), parked, tracer)

        assert task.cancelled() is True  # recorded, never converted
        assert manager.committed is True
        [entry] = self._ambiguous(logs)
        assert entry["log_level"] == "warning"
        assert entry["operation"] == "op"
        [span] = exporter.get_finished_spans()
        [event] = span.events
        assert event.name == "forze.commit_ambiguous"
        assert dict(event.attributes or {}) == {"forze.operation": "op"}
        reset_commit_started()

    @pytest.mark.asyncio
    async def test_cancel_before_the_commit_records_nothing(self, ctx: ExecutionContext) -> None:
        reset_commit_started()
        parked = asyncio.Event()

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class WaitInBody(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                parked.set()
                await asyncio.sleep(10)  # never reaches a commit
                return args

        reg = OperationRegistry(handlers={"op": lambda _c: WaitInBody()}).freeze()
        tracer, exporter = self._tracer()

        with structlog.testing.capture_logs() as logs:
            await self._cancel_while_parked(reg.resolve("op", ctx), parked, tracer)

        assert self._ambiguous(logs) == []
        [span] = exporter.get_finished_spans()
        assert len(span.events) == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize("broken", ["logger", "span"])
    async def test_a_failing_record_never_replaces_the_cancellation(
        self, ctx: ExecutionContext, monkeypatch: pytest.MonkeyPatch, broken: str
    ) -> None:
        def _boom(*_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("telemetry is down")

        if broken == "logger":
            stub = SimpleNamespace(warning=_boom, debug=_boom)
            monkeypatch.setattr(invoke_module, "logger", stub)

        else:
            monkeypatch.setattr("opentelemetry.trace.get_current_span", _boom)

        reset_commit_started()
        tx = _tx_with(_RecordingTxManager())
        parked = asyncio.Event()

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class CommitThenWait(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                async with tx.scope("rec"):
                    pass

                parked.set()
                await asyncio.sleep(10)
                return args

        reg = OperationRegistry(handlers={"op": lambda _c: CommitThenWait()}).freeze()
        # No SDK span: starting one would itself call the broken ``get_current_span``.
        tracer = SimpleNamespace(start_as_current_span=lambda _name: nullcontext())
        task = await self._cancel_while_parked(reg.resolve("op", ctx), parked, tracer)

        assert task.cancelled() is True
        reset_commit_started()

    @pytest.mark.asyncio
    async def test_a_nested_commit_is_recorded_once_for_the_top_level_operation(
        self, ctx: ExecutionContext
    ) -> None:
        reset_commit_started()
        manager = _RecordingTxManager()
        tx = _tx_with(manager)
        parked = asyncio.Event()

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class CommitThenWait(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                async with tx.scope("rec"):
                    pass

                parked.set()
                await asyncio.sleep(10)
                return args

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class Outer(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                return await reg.resolve("inner", ctx)(args)

        reg = OperationRegistry(
            handlers={"outer": lambda _c: Outer(), "inner": lambda _c: CommitThenWait()}
        ).freeze()
        tracer, _exporter = self._tracer()

        with structlog.testing.capture_logs() as logs:
            await self._cancel_while_parked(reg.resolve("outer", ctx), parked, tracer)

        [entry] = self._ambiguous(logs)
        assert entry["operation"] == "outer"
        reset_commit_started()


class _ReadOnlyAwareTxManager(_RecordingTxManager):
    """Refuses writes in a read-only root it opens, unless it is nested in an outer transaction.

    Nested, the root is a savepoint the backend cannot make read-only, as with Postgres.
    """

    def __init__(self, *, in_outer_transaction: bool = False, commit_delay: float = 0.0) -> None:
        super().__init__(commit_delay=commit_delay)
        self.in_outer_transaction = in_outer_transaction
        self.enforced = False
        self.depth = 0

    def transaction(
        self, *, read_only: bool = False, isolation: IsolationLevel | None = None
    ) -> AbstractAsyncContextManager[None]:
        inner = super().transaction(read_only=read_only, isolation=isolation)

        @asynccontextmanager
        async def _cm() -> AsyncGenerator[None]:
            # Only the root decides; a nested savepoint shares the root's mode.
            if self.depth == 0:
                self.enforced = read_only and not self.in_outer_transaction

            self.depth += 1

            try:
                async with inner:
                    yield

            finally:
                self.depth -= 1

        return _cm()

    def read_only_enforced(self) -> bool:
        return self.enforced


class _TruthyTxManager(_RecordingTxManager):
    """Answers with a truthy value that is not ``True``; only ``True`` may skip the mark."""

    def read_only_enforced(self) -> Any:
        return "yes"


class TestReadOnlyRootCommitPoint:
    """A read-only root marks no commit point only where the backend refuses writes in it."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("manager", "read_only", "marked"),
        [
            pytest.param(_ReadOnlyAwareTxManager(), True, False, id="enforced"),
            pytest.param(
                _ReadOnlyAwareTxManager(in_outer_transaction=True), True, True, id="savepoint"
            ),
            pytest.param(_RecordingTxManager(), True, True, id="unaware"),
            pytest.param(_ReadOnlyAwareTxManager(), False, True, id="writes"),
            pytest.param(_TruthyTxManager(), True, True, id="truthy-not-true"),
        ],
    )
    async def test_the_mark_follows_the_opened_transaction(
        self, manager: TransactionManagerPort, read_only: bool, marked: bool
    ) -> None:
        reset_commit_started()

        async with _tx_with(manager).scope("rec", read_only=read_only):
            pass

        assert commit_started() is marked
        reset_commit_started()

    @staticmethod
    def _read_then_wait(enforced: bool, parked: asyncio.Event) -> OperationRegistry:
        tx = _tx_with(_ReadOnlyAwareTxManager(in_outer_transaction=not enforced))

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class ReadThenWait(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                async with tx.scope("rec", read_only=True):
                    pass

                parked.set()
                await asyncio.sleep(10)
                return args

        reg = OperationRegistry(handlers={"op": lambda _c: ReadThenWait()})

        return reg.bind("op").as_query().finish()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("enforced", "kind", "code"),
        [
            (True, ExceptionKind.TIMEOUT, "deadline_exceeded"),
            (False, ExceptionKind.INTERNAL, "commit_ambiguous"),
        ],
    )
    async def test_a_query_timed_out_after_its_read(
        self, ctx: ExecutionContext, enforced: bool, kind: ExceptionKind, code: str
    ) -> None:
        reset_commit_started()
        resolved = self._read_then_wait(enforced, asyncio.Event()).freeze().resolve("op", ctx)

        with bind_deadline(0.05), pytest.raises(CoreException) as ei:
            await resolved("x")

        assert (ei.value.kind, ei.value.code) == (kind, code)
        reset_commit_started()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(("enforced", "records"), [(True, 0), (False, 1)])
    async def test_a_query_cancelled_after_its_read(
        self, ctx: ExecutionContext, enforced: bool, records: int
    ) -> None:
        reset_commit_started()
        parked = asyncio.Event()
        resolved = self._read_then_wait(enforced, parked).freeze().resolve("op", ctx)
        task = asyncio.create_task(resolved("x"))
        await parked.wait()

        with structlog.testing.capture_logs() as logs:
            task.cancel()

            with pytest.raises(asyncio.CancelledError):
                await task

        assert task.cancelled() is True
        assert len([e for e in logs if e.get("code") == "commit_ambiguous"]) == records

    @pytest.mark.asyncio
    async def test_a_cancel_in_an_enforced_read_only_commit_stays_a_cancel(self) -> None:
        reset_commit_started()
        manager = _ReadOnlyAwareTxManager(commit_delay=10.0)
        tx = _tx_with(manager)

        async def driver() -> None:
            task = asyncio.current_task()
            assert task is not None
            active_operation_var.set(task)

            async with tx.scope("rec", read_only=True):
                pass

        task = asyncio.create_task(driver())
        await manager.in_commit.wait()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert task.cancelled() is True  # nothing can have landed: not converted

    @pytest.mark.asyncio
    async def test_a_nested_scope_on_another_route_keeps_the_commit_point(self) -> None:
        # Another route resolves another manager under the same scope key, so its nested
        # scope is an independent transaction that commits on its own: the read-only root
        # is no longer the only transaction, and a cancel in its commit stays ambiguous.
        reset_commit_started()
        root = _ReadOnlyAwareTxManager(commit_delay=10.0)
        other = _RecordingTxManager()
        tx = TransactionContext()
        tx.lock(lambda route: root if route == "rec" else other)

        async def driver() -> None:
            task = asyncio.current_task()
            assert task is not None
            active_operation_var.set(task)

            async with tx.scope("rec", read_only=True):
                async with tx.scope("other"):
                    pass

        task = asyncio.create_task(driver())
        await root.in_commit.wait()
        task.cancel()

        with pytest.raises(CoreException) as ei:
            await task

        assert ei.value.code == "commit_ambiguous"
        assert other.committed is True
        reset_commit_started()

    @pytest.mark.asyncio
    async def test_a_nested_scope_on_the_same_route_keeps_the_skip(self) -> None:
        # The same route is the same transaction: a savepoint inside the read-only root.
        reset_commit_started()
        tx = _tx_with(_ReadOnlyAwareTxManager())

        async with tx.scope("rec", read_only=True):
            async with tx.scope("rec"):
                pass

        assert commit_started() is False
