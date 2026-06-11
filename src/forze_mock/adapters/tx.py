"""Mock transaction managers: the default no-op and the opt-in strict variant."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, final

import attrs

from forze.application.contracts.transaction import (
    TransactionManagerPort,
    TransactionScopeKey,
)
from forze.base.exceptions import exc

if TYPE_CHECKING:
    from forze_mock.state import MockState

MockTxScopeKey = TransactionScopeKey("mock")
"""Scope key for the in-memory mock transaction manager."""

# ----------------------- #

# Deliberately **module-level** (immortal) ContextVars, per the codebase rule for
# cross-cutting markers (see ``forze.application.execution.context.active_operation``):
# adapter instances are constructed per resolve, so per-instance vars would leak.

_strict_tx_depth: ContextVar[int] = ContextVar(
    "forze_mock_strict_tx_depth",
    default=0,
)
"""Per-task nesting depth of strict mock transaction scopes (``0`` outside)."""

_strict_tx_read_only: ContextVar[bool] = ContextVar(
    "forze_mock_strict_tx_read_only",
    default=False,
)
"""Whether the current task's strict **root** transaction was opened read-only."""


# ....................... #


def mock_tx_is_read_only() -> bool:
    """Return whether the current task is inside a strict read-only mock transaction."""

    return _strict_tx_read_only.get()


# ....................... #


def ensure_mock_tx_writable(*, store: str) -> None:
    """Raise when a participating store is written inside a strict read-only root.

    Mirrors Postgres rejecting writes in a ``BEGIN ... READ ONLY`` transaction
    (``QUERY`` operations open the root read-only). Raised as
    ``exc.precondition(code="read_only_tx")``: the caller violated the root
    transaction's read-only option — the same kind the kernel uses for the
    related ``tx_nested_read_only_conflict`` violation. (The raw Postgres error
    funnels through the generic operational mapping; the mock picks the more
    precise kind on purpose.)

    A no-op outside strict read-only roots, so default (no-op) mock wiring is
    unaffected. Only stores that are DB-backed in production call this — queues,
    blobs, caches, etc. accept writes in a read-only DB transaction in
    production too.
    """

    if _strict_tx_read_only.get():
        raise exc.precondition(
            f"Write to {store!r} inside a read-only transaction.",
            code="read_only_tx",
            details={"store": store},
        )


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockTxManagerAdapter(TransactionManagerPort):
    """No-op transaction manager for mock environments.

    Records each transaction's ``read_only`` flag on the shared state so tests can assert
    a ``QUERY`` operation opened its transaction read-only.

    Writes inside a transaction that later rolls back **persist** under this
    adapter. Opt into :class:`MockStrictTxManagerAdapter` (via
    ``MockDepsModule(strict_tx=True)``) to surface
    "forgot to run it in the same transaction" bugs in tests.
    """

    state: MockState | None = attrs.field(default=None)

    # ....................... #

    @property
    def scope_key(self) -> TransactionScopeKey:
        return MockTxScopeKey

    # ....................... #

    def transaction(
        self, *, read_only: bool = False
    ) -> AbstractAsyncContextManager[None]:
        if self.state is not None:
            self.state.tx_read_only_calls.append(read_only)

        @asynccontextmanager
        async def _noop() -> AsyncGenerator[None]:
            yield

        return _noop()


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockStrictTxManagerAdapter(TransactionManagerPort):
    """Strict mock transaction manager with real rollback semantics.

    Semantics:

    - **Root scope** — snapshots the transaction-participating
      :class:`~forze_mock.state.MockState` stores on entry (see the
      participation classification in its docstring); a clean exit discards the
      snapshot (commit), an escaping exception restores it (rollback).
    - **Nested scope = savepoint** — each nested entry takes its own snapshot;
      an inner rollback restores to the inner snapshot without disturbing outer
      writes.
    - **Serialization** — root transactions on the same state are serialized via
      a per-state :class:`asyncio.Lock`: a global snapshot/restore cannot give
      per-task isolation, so serializing is the honest semantic (real databases
      serialize conflicting writers anyway). Nested scopes on the same task do
      not re-acquire. One :class:`~forze_mock.state.MockState` per transaction
      nest and per event loop is assumed.
    - **Read-only roots** — a root opened with ``read_only=True`` (``QUERY``
      operations) sets a per-task flag; writes to participating stores then
      raise ``exc.precondition(code="read_only_tx")``, matching Postgres
      ``BEGIN ... READ ONLY``. Reads are unaffected.

    Caveat: only mock stores roll back. In-process side effects outside them —
    handler-mutated Python objects, captured lists, etc. — cannot be restored.
    """

    state: MockState

    # ....................... #

    @property
    def scope_key(self) -> TransactionScopeKey:
        return MockTxScopeKey

    # ....................... #

    def transaction(
        self, *, read_only: bool = False
    ) -> AbstractAsyncContextManager[None]:
        self.state.tx_read_only_calls.append(read_only)
        return self._transaction(read_only=read_only)

    # ....................... #

    @asynccontextmanager
    async def _transaction(self, *, read_only: bool) -> AsyncGenerator[None]:
        depth = _strict_tx_depth.get()
        is_root = depth == 0

        if is_root:
            # Serialize root transactions per state (see class docstring).
            await self.state.tx_serializer.acquire()

        try:
            snapshot = self.state.snapshot_tx_stores()
            token_depth = _strict_tx_depth.set(depth + 1)
            # Options are honored at root only (port contract); nested scopes
            # inherit the root's read_only via the still-set ContextVar.
            token_ro = _strict_tx_read_only.set(read_only) if is_root else None

            try:
                yield

            except BaseException:
                self.state.restore_tx_stores(snapshot)
                raise

            finally:
                if token_ro is not None:
                    _strict_tx_read_only.reset(token_ro)
                _strict_tx_depth.reset(token_depth)

        finally:
            if is_root:
                self.state.tx_serializer.release()
