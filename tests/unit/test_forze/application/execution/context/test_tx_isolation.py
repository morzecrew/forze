"""Transaction isolation contract — declared-isolation fail-closed at first resolve.

An operation declares a required :class:`IsolationLevel` (``OperationPlan.bind_tx()
.set_isolation(...)``); the kernel checks it against the route's manager when the root scope
is first entered. A manager that reports the level (``IsolationAware`` with the level in its
``TxCapabilities``) runs it; one that cannot — or reports no capabilities at all — is
rejected with ``exc.configuration`` rather than silently running weaker isolation.
"""

from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import AsyncGenerator

import attrs
import pytest

from forze.application.contracts.execution import Handler
from forze.application.contracts.transaction import (
    IsolationAware,
    IsolationLevel,
    TransactionManagerPort,
    TransactionScopeKey,
)
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.application.execution.context.transaction import TransactionContext
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException
from forze_mock import MockDepsModule
from forze_mock.adapters.tx import (
    MockJournalTxManagerAdapter,
    MockStrictTxManagerAdapter,
    MockTxManagerAdapter,
)
from forze_mock.state import MockState

# ----------------------- #


class _NoCapsManager(TransactionManagerPort):
    """A manager that is *not* ``IsolationAware`` (reports no capabilities)."""

    @property
    def scope_key(self) -> TransactionScopeKey:
        return TransactionScopeKey("mock")

    def transaction(self, *, read_only: bool = False) -> AbstractAsyncContextManager[None]:
        @asynccontextmanager
        async def _noop() -> AsyncGenerator[None]:
            yield

        return _noop()


def _ctx(manager: TransactionManagerPort) -> TransactionContext:
    tx = TransactionContext()
    tx.lock(lambda _route: manager)
    return tx


async def _enter(tx: TransactionContext, **kwargs: object) -> None:
    async with tx.scope("mock", **kwargs):  # type: ignore[arg-type]
        pass


# ....................... #


def test_mock_managers_are_isolation_aware() -> None:
    state = MockState()
    assert isinstance(MockJournalTxManagerAdapter(state=state), IsolationAware)
    assert isinstance(MockStrictTxManagerAdapter(state=state), IsolationAware)
    assert not isinstance(_NoCapsManager(), IsolationAware)


def test_required_isolation_within_capabilities_is_allowed() -> None:
    # The strict manager serializes roots, so it reports every level — serializable runs.
    tx = _ctx(MockStrictTxManagerAdapter(state=MockState()))
    asyncio.run(_enter(tx, isolation=IsolationLevel.SERIALIZABLE))


def test_read_committed_against_journal_is_allowed() -> None:
    tx = _ctx(MockJournalTxManagerAdapter(state=MockState()))
    asyncio.run(_enter(tx, isolation=IsolationLevel.READ_COMMITTED))


def test_isolation_beyond_capabilities_fails_closed() -> None:
    # The no-op manager reports read-committed only; requiring serializable is rejected.
    tx = _ctx(MockTxManagerAdapter(state=MockState()))

    with pytest.raises(CoreException) as excinfo:
        asyncio.run(_enter(tx, isolation=IsolationLevel.SERIALIZABLE))

    assert excinfo.value.code == "tx_isolation_unsupported"


def test_journal_now_supports_serializable() -> None:
    # WS5: the journal manager honors snapshot/serializable via the MVCC overlay.
    tx = _ctx(MockJournalTxManagerAdapter(state=MockState()))
    asyncio.run(_enter(tx, isolation=IsolationLevel.SERIALIZABLE))


def test_isolation_against_non_reporting_manager_fails_closed() -> None:
    # A manager that does not report capabilities cannot guarantee any level → reject.
    tx = _ctx(_NoCapsManager())

    with pytest.raises(CoreException) as excinfo:
        asyncio.run(_enter(tx, isolation=IsolationLevel.READ_COMMITTED))

    assert excinfo.value.code == "tx_isolation_unsupported"


def test_no_declared_isolation_skips_the_check() -> None:
    # No isolation requirement → a non-reporting manager is untouched (zero blast radius).
    tx = _ctx(_NoCapsManager())
    asyncio.run(_enter(tx))


def test_nested_isolation_conflict_is_rejected() -> None:
    tx = _ctx(MockStrictTxManagerAdapter(state=MockState()))

    async def run() -> None:
        async with tx.scope("mock", isolation=IsolationLevel.SERIALIZABLE):
            async with tx.scope("mock", isolation=IsolationLevel.READ_COMMITTED):
                pass

    with pytest.raises(CoreException) as excinfo:
        asyncio.run(run())

    assert excinfo.value.code == "tx_nested_isolation_conflict"


def test_nested_inherits_root_isolation_when_unspecified() -> None:
    # A nested scope that does not request isolation inherits the root's (no conflict).
    tx = _ctx(MockStrictTxManagerAdapter(state=MockState()))

    async def run() -> None:
        async with tx.scope("mock", isolation=IsolationLevel.SERIALIZABLE):
            async with tx.scope("mock"):
                pass

    asyncio.run(run())


# ....................... #
# End-to-end: ``set_isolation`` on a real operation reaches the check via run_operation.


@attrs.define(slots=True, kw_only=True)
class _Noop(Handler[None, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        return None


def _run_op(*, isolation: IsolationLevel, transactions: str) -> None:
    plan = (
        OperationPlan()
        .bind_tx()
        .set_route("mock")
        .set_isolation(isolation)
        .finish(deep=False)
    )
    registry = OperationRegistry(
        handlers={"op": lambda ctx: _Noop(ctx=ctx)},
        plans={"op": plan},
        descriptors={
            "op": OperationDescriptor(
                input_type=None, output_type=None, description="x"
            )
        },
    ).freeze()
    deps = (
        DepsRegistry.from_modules(MockDepsModule(transactions=transactions))
        .freeze()
        .resolve()
    )
    ctx = ExecutionContext(deps=deps)
    asyncio.run(run_operation(registry, "op", None, ctx))


def test_operation_declared_isolation_beyond_caps_fails_closed_end_to_end() -> None:
    # The no-op manager is read-committed only; an op declaring serializable is rejected.
    with pytest.raises(CoreException) as excinfo:
        _run_op(isolation=IsolationLevel.SERIALIZABLE, transactions="none")

    assert excinfo.value.code == "tx_isolation_unsupported"


def test_operation_declared_isolation_within_caps_runs_end_to_end() -> None:
    # The strict manager reports every level, so the same declaration runs.
    _run_op(isolation=IsolationLevel.SERIALIZABLE, transactions="strict")
