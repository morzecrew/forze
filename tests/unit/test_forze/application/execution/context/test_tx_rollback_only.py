"""``rollback_only()`` on a transaction scope: a rollback on purpose, not an error.

The kernel's half, against a recording manager: what the manager is told at exit, what runs
after it, and what is refused. The backends' half (the writes are really gone) is the shared
battery in ``tests/support/tx_rollback_only_conformance.py``.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager, asynccontextmanager

import attrs
import pytest

from forze.application.contracts.transaction import (
    IsolationLevel,
    RollbackSignal,
    TransactionManagerPort,
    TransactionScopeKey,
    TxCapabilities,
)
from forze.application.execution.context.commit_state import (
    commit_started,
    reset_commit_started,
)
from forze.application.execution.context.transaction import TransactionContext
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #

_SCOPE = TransactionScopeKey("fake")


class _Boom(RuntimeError):
    pass


@attrs.define(slots=True)
class _RecordingManager:
    """Records how each level of the nest ended, as ``"<level>:commit"`` or ``"<level>:<error>"``."""

    savepoints: bool = True
    cancel_nested_rollback: bool = False
    """Cancel the task while a nested scope rolls back on request, as a deadline would."""
    events: list[str] = attrs.field(factory=list)
    depth: int = 0

    @property
    def scope_key(self) -> TransactionScopeKey:
        return _SCOPE

    def capabilities(self) -> TxCapabilities:
        return TxCapabilities(
            isolation=frozenset({IsolationLevel.READ_COMMITTED}), savepoints=self.savepoints
        )

    def is_transactionally_enlisted(self) -> bool:
        # One manager is one client: a scope joins only while this one has a transaction open.
        return self.depth > 0

    @asynccontextmanager
    async def transaction(
        self, *, read_only: bool = False, isolation: IsolationLevel | None = None
    ) -> AsyncGenerator[None]:
        level = "nested" if self.depth else "root"
        self.depth += 1

        try:
            yield

        except BaseException as error:
            self.events.append(f"{level}:{type(error).__name__}")

            if (
                self.cancel_nested_rollback
                and level == "nested"
                and isinstance(error, RollbackSignal)
            ):
                task = asyncio.current_task()
                assert task is not None
                task.cancel()
                await asyncio.sleep(0)  # the rollback's round trip, where the cancel lands

            raise

        else:
            self.events.append(f"{level}:commit")

        finally:
            self.depth -= 1


class _NoCapsManager(TransactionManagerPort):
    """A manager that reports no capabilities at all (not ``IsolationAware``)."""

    @property
    def scope_key(self) -> TransactionScopeKey:
        return _SCOPE

    def transaction(self, *, read_only: bool = False) -> AbstractAsyncContextManager[None]:
        @asynccontextmanager
        async def _noop() -> AsyncGenerator[None]:
            yield

        return _noop()


def _tx(manager: object) -> TransactionContext:
    tx = TransactionContext()
    tx.lock(lambda _route: manager)  # type: ignore[arg-type, return-value]
    return tx


def _routes(**managers: object) -> TransactionContext:
    """One manager per route; a manager given twice is one client behind two routes."""

    tx = TransactionContext()
    tx.lock(lambda route: managers[str(route)])  # type: ignore[arg-type, return-value]
    return tx


def _errors(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]


# ....................... #


class TestRoot:
    async def test_rolls_back_and_the_caller_carries_on(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        manager = _RecordingManager()
        tx = _tx(manager)

        async with tx.scope("r") as scope:
            report = "built inside"
            scope.rollback_only()

        assert report == "built inside"
        assert manager.events == ["root:RollbackSignal"]
        assert scope.rollback_requested
        assert tx.depth() == 0
        assert _errors(caplog) == []

    async def test_is_no_commit_point(self) -> None:
        reset_commit_started()

        async with _tx(_RecordingManager()).scope("r") as scope:
            scope.rollback_only()

        assert commit_started() is False

    async def test_after_commit_callbacks_do_not_run(self) -> None:
        tx = _tx(_RecordingManager())
        ran: list[str] = []

        async def callback() -> None:
            ran.append("ran")

        async with tx.scope("r") as scope:
            await tx.run_or_defer(callback)
            scope.rollback_only()

        assert ran == []

    async def test_an_error_after_the_mark_propagates(self) -> None:
        manager = _RecordingManager()

        with pytest.raises(_Boom):
            async with _tx(manager).scope("r") as scope:
                scope.rollback_only()
                raise _Boom

        assert manager.events == ["root:_Boom"]

    async def test_a_second_call_changes_nothing(self) -> None:
        manager = _RecordingManager()

        async with _tx(manager).scope("r") as scope:
            scope.rollback_only()
            scope.rollback_only()

        assert manager.events == ["root:RollbackSignal"]

    async def test_after_the_scope_exited_it_is_refused(self) -> None:
        async with _tx(_RecordingManager()).scope("r") as scope:
            pass

        with pytest.raises(CoreException) as caught:
            scope.rollback_only()

        assert caught.value.kind is ExceptionKind.PRECONDITION
        assert caught.value.code == "tx_scope_ended"

    async def test_an_unmarked_scope_commits(self) -> None:
        manager = _RecordingManager()

        async with _tx(manager).scope("r") as scope:
            pass

        assert not scope.rollback_requested
        assert manager.events == ["root:commit"]


class TestNested:
    async def test_rolls_back_only_its_savepoint(self, caplog: pytest.LogCaptureFixture) -> None:
        manager = _RecordingManager()
        tx = _tx(manager)

        async with tx.scope("r"):
            async with tx.scope("r") as nested:
                nested.rollback_only()

            assert tx.depth() == 1

        assert manager.events == ["nested:RollbackSignal", "root:commit"]
        assert _errors(caplog) == []

    @pytest.mark.parametrize("outer_first", [True, False], ids=["queue-busy", "queue-empty"])
    async def test_drops_only_the_callbacks_it_deferred(self, outer_first: bool) -> None:
        tx = _tx(_RecordingManager())
        ran: list[str] = []

        async def outer() -> None:
            ran.append("outer")

        async def inner() -> None:
            ran.append("inner")

        async with tx.scope("r"):
            if outer_first:
                await tx.run_or_defer(outer)

            async with tx.scope("r") as nested:
                await tx.run_or_defer(inner)
                nested.rollback_only()

            if not outer_first:
                await tx.run_or_defer(outer)

        assert ran == ["outer"]

    async def test_marking_the_outer_scope_from_inside_rolls_back_the_root(self) -> None:
        manager = _RecordingManager()
        tx = _tx(manager)

        async with tx.scope("r") as root, tx.scope("r"):
            root.rollback_only()

        assert manager.events == ["nested:commit", "root:RollbackSignal"]

    @pytest.mark.parametrize(
        "manager",
        [_RecordingManager(savepoints=False), _NoCapsManager()],
        ids=["without-savepoints", "without-capabilities"],
    )
    async def test_without_savepoints_it_is_refused_at_the_call(self, manager: object) -> None:
        tx = _tx(manager)

        with pytest.raises(CoreException) as caught:
            async with tx.scope("r"), tx.scope("r") as nested:
                nested.rollback_only()

        assert caught.value.kind is ExceptionKind.PRECONDITION
        assert caught.value.code == "tx_nested_rollback_unsupported"

        if isinstance(manager, _RecordingManager):
            # A real error from then on: it rolls the whole transaction back unless caught.
            assert manager.events == ["nested:CoreException", "root:CoreException"]

    async def test_a_signal_the_scopes_did_not_raise_is_never_swallowed(self) -> None:
        """Raising it by hand is not a way to ask for a rollback: it leaves every scope."""

        manager = _RecordingManager()
        tx = _tx(manager)
        foreign = RollbackSignal()

        with pytest.raises(RollbackSignal) as caught:
            async with tx.scope("r"), tx.scope("r"):
                raise foreign

        assert caught.value is foreign
        assert manager.events == ["nested:RollbackSignal", "root:RollbackSignal"]


class TestNestedCallbacks:
    async def test_a_cancelled_rollback_cannot_leave_them_queued(self) -> None:
        """A cancel landing in the savepoint rollback, caught, then the outer commit."""

        tx = _tx(_RecordingManager(cancel_nested_rollback=True))
        ran: list[str] = []

        async def outer() -> None:
            ran.append("outer")

        async def inner() -> None:
            ran.append("inner")

        async with tx.scope("r"):
            await tx.run_or_defer(outer)

            with pytest.raises(asyncio.CancelledError):
                async with tx.scope("r") as nested:
                    await tx.run_or_defer(inner)
                    nested.rollback_only()

            task = asyncio.current_task()
            assert task is not None
            task.uncancel()

        assert ran == ["outer"]

    async def test_a_sibling_tasks_outer_callback_survives(self) -> None:
        """A task of the outer scope defers while the nested scope is open: it is not the nested's."""

        tx = _tx(_RecordingManager())
        ran: list[str] = []
        go, deferred = asyncio.Event(), asyncio.Event()

        async def inner() -> None:
            ran.append("inner")

        async def sibling_effect() -> None:
            ran.append("sibling")

        async def sibling() -> None:
            await go.wait()
            await tx.run_or_defer(sibling_effect)
            deferred.set()

        async with tx.scope("r"):
            task = asyncio.create_task(sibling())

            async with tx.scope("r") as nested:
                await tx.run_or_defer(inner)
                go.set()
                await deferred.wait()
                nested.rollback_only()

            await task

        assert ran == ["sibling"]


class TestOtherRoutes:
    """A scope on a route whose manager runs on another client has a transaction of its own."""

    async def test_a_root_marked_first_rolls_it_back(self) -> None:
        a, b = _RecordingManager(), _RecordingManager()
        tx = _routes(a=a, b=b)

        async with tx.scope("a") as root:
            root.rollback_only()

            async with tx.scope("b"):
                pass

        assert b.events == ["root:RollbackSignal"]
        assert a.events == ["root:RollbackSignal"]

    async def test_a_root_marked_while_it_is_open_rolls_it_back(self) -> None:
        a, b = _RecordingManager(), _RecordingManager()
        tx = _routes(a=a, b=b)

        async with tx.scope("a") as root, tx.scope("b"):
            root.rollback_only()

        assert b.events == ["root:RollbackSignal"]
        assert a.events == ["root:RollbackSignal"]

    @pytest.mark.parametrize(
        "other",
        [_RecordingManager(), _NoCapsManager()],
        ids=["separate-client", "cannot-tell"],
    )
    async def test_rollback_only_after_it_committed_is_refused(self, other: object) -> None:
        a = _RecordingManager()
        tx = _routes(a=a, b=other)

        with pytest.raises(CoreException) as caught:
            async with tx.scope("a") as root:
                async with tx.scope("b"):
                    pass

                root.rollback_only()

        assert caught.value.kind is ExceptionKind.PRECONDITION
        assert caught.value.code == "tx_rollback_after_independent_commit"
        # The refusal is an error through the root, which rolls back what it can.
        assert a.events == ["root:CoreException"]

        if isinstance(other, _RecordingManager):
            assert other.events == ["root:commit"]

    async def test_a_route_on_the_same_client_joins_and_is_not_refused(self) -> None:
        a = _RecordingManager()
        tx = _routes(a=a, b=a)

        async with tx.scope("a") as root:
            async with tx.scope("b"):
                pass

            root.rollback_only()

        assert a.events == ["nested:commit", "root:RollbackSignal"]

    async def test_it_rolls_back_alone_without_savepoints(self) -> None:
        a, b = _RecordingManager(), _RecordingManager(savepoints=False)
        tx = _routes(a=a, b=b)

        async with tx.scope("a"), tx.scope("b") as other:
            other.rollback_only()

        assert b.events == ["root:RollbackSignal"]
        assert a.events == ["root:commit"]
