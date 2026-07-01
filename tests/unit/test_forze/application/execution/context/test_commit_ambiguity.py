"""A deadline that tears (or follows) a transaction commit is non-retryable.

If the invocation deadline fires at or after the root transaction's commit point, the
commit may have (or has) landed — the outcome is ambiguous. The engine surfaces a
non-retryable ``commit_ambiguous`` error rather than a retryable ``deadline_exceeded``,
so an at-least-once caller does not blindly retry into a duplicate. A deadline during
the body (before any commit) stays a retryable deadline (nothing committed).
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
