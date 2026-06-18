"""Mock transaction managers: the default no-op and the opt-in strict variant."""

from contextlib import AbstractAsyncContextManager, asynccontextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, AsyncGenerator, final

import attrs

from forze.application.contracts.transaction import (
    IsolationLevel,
    TransactionManagerPort,
    TransactionScopeKey,
    TxCapabilities,
)
from forze.base.exceptions import exc
from forze_mock.adapters._journal import (
    _journal,  # pyright: ignore[reportPrivateUsage]
    undo,
)
from forze_mock.adapters._mvcc import (
    MvccTx,
    _mvcc_tx,  # pyright: ignore[reportPrivateUsage]
)

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

_journal_tx_depth: ContextVar[int] = ContextVar(
    "forze_mock_journal_tx_depth",
    default=0,
)
"""Per-task nesting depth of journal mock transaction scopes (``0`` outside)."""

_mock_tx_read_only: ContextVar[bool] = ContextVar(
    "forze_mock_tx_read_only",
    default=False,
)
"""Whether the current task's **root** mock transaction was opened read-only (strict or
journal manager)."""


# ....................... #


def mock_tx_is_read_only() -> bool:
    """Return whether the current task is inside a read-only mock transaction."""

    return _mock_tx_read_only.get()


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

    if _mock_tx_read_only.get():
        raise exc.precondition(
            f"Write to {store!r} inside a read-only transaction.",
            code="read_only_tx",
            details={"store": store},
        )


# ....................... #


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

    state: "MockState | None" = attrs.field(default=None)

    # ....................... #

    @property
    def scope_key(self) -> TransactionScopeKey:
        return MockTxScopeKey

    # ....................... #

    def capabilities(self) -> TxCapabilities:
        # A no-op manager gives no isolation or atomicity guarantee; it can only honor the
        # weakest declared level (and an explicit stronger requirement fails closed).
        return TxCapabilities(
            isolation=frozenset({IsolationLevel.READ_COMMITTED}),
        )

    # ....................... #

    def transaction(
        self,
        *,
        read_only: bool = False,
        isolation: IsolationLevel | None = None,
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

    state: "MockState"

    # ....................... #

    @property
    def scope_key(self) -> TransactionScopeKey:
        return MockTxScopeKey

    # ....................... #

    def capabilities(self) -> TxCapabilities:
        # Root transactions are globally serialized (per-state lock) and rolled back via a
        # whole-store snapshot, so this manager trivially satisfies every level up to
        # serializable; nested scopes are real savepoints.
        return TxCapabilities(
            isolation=frozenset(IsolationLevel),
        )

    # ....................... #

    def transaction(
        self,
        *,
        read_only: bool = False,
        isolation: IsolationLevel | None = None,
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
            token_ro = _mock_tx_read_only.set(read_only) if is_root else None

            try:
                yield

            except BaseException:
                self.state.restore_tx_stores(snapshot)
                raise

            finally:
                if token_ro is not None:
                    _mock_tx_read_only.reset(token_ro)

                _strict_tx_depth.reset(token_depth)

        finally:
            if is_root:
                self.state.tx_serializer.release()


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockJournalTxManagerAdapter(TransactionManagerPort):
    """Concurrency-preserving atomicity via a per-transaction undo journal — the default.

    Writes to participating stores go through immediately (write-through), but each records an
    undo thunk in a per-task journal (see :mod:`forze_mock.adapters._journal`); an escaping
    exception replays the journal in reverse, undoing **only this transaction's** writes — so
    a failed operation leaves no partial writes, while concurrent transactions interleave
    freely (no global serialization, unlike :class:`MockStrictTxManagerAdapter`). Coverage
    spans every participating store: documents and the outbox/inbox per-entry, identity via a
    coarse deep snapshot (it is mutated in place). Write-write conflicts on documents are
    caught by the row ``rev`` (optimistic concurrency). A read-only root rejects writes to
    participating stores (``QUERY`` operations), like Postgres ``BEGIN ... READ ONLY``.

    Read-committed by default (write-through, so a concurrent transaction can observe a
    not-yet-committed row — a rollback then undoes it). Snapshot / serializable isolation is
    honored for the document store via the MVCC overlay (see :mod:`forze_mock.adapters._mvcc`).

    Faithful enough to make DST findings trustworthy: it rolls back partial writes (no false
    "double effect" from an aborted transaction) yet preserves the interleavings DST explores.
    """

    state: "MockState"

    # ....................... #

    @property
    def scope_key(self) -> TransactionScopeKey:
        return MockTxScopeKey

    # ....................... #

    def capabilities(self) -> TxCapabilities:
        # Read-committed by default (write-through journal + row-``rev`` OCC); snapshot and
        # serializable are honored via the MVCC buffered overlay (see ``adapters._mvcc``).
        return TxCapabilities(
            isolation=frozenset(IsolationLevel),
        )

    # ....................... #

    def transaction(
        self,
        *,
        read_only: bool = False,
        isolation: IsolationLevel | None = None,
    ) -> AbstractAsyncContextManager[None]:
        self.state.tx_read_only_calls.append(read_only)
        return self._transaction(read_only=read_only, isolation=isolation)

    # ....................... #

    @asynccontextmanager
    async def _transaction(
        self, *, read_only: bool, isolation: IsolationLevel | None
    ) -> AsyncGenerator[None]:
        depth = _journal_tx_depth.get()
        is_root = depth == 0
        token_depth = _journal_tx_depth.set(depth + 1)

        # Snapshot / serializable additionally use the MVCC buffered overlay for the DOCUMENT
        # store (reads from an as-of-begin snapshot, writes buffered, validated + published at
        # commit). The undo journal is ALWAYS active at the root: under read-committed it
        # covers document writes too (write-through JournalingStore); under MVCC, documents go
        # through the overlay instead, so the journal then covers only the non-document
        # participating stores (outbox `list`, inbox `set`, via `record_undo`). Identity is
        # mutated in place, so it is reverted by a coarse deep-snapshot rather than the
        # journal. Only the root sets these up; nested scopes (savepoints) share them —
        # savepoint-level partial rollback is not modelled.
        mvcc_enabled = isolation in (
            IsolationLevel.SNAPSHOT,
            IsolationLevel.SERIALIZABLE,
        )

        mvcc = (
            MvccTx.begin(
                self.state, serializable=isolation is IsolationLevel.SERIALIZABLE
            )
            if is_root and mvcc_enabled
            else None
        )
        token_mvcc = _mvcc_tx.set(mvcc) if mvcc is not None else None
        token_journal = _journal.set([]) if is_root else None
        token_ro = _mock_tx_read_only.set(read_only) if is_root else None
        identity_snapshot = self.state.snapshot_identity() if is_root else None

        try:
            yield

            # Validate + publish the overlay on the success path (a serialization conflict
            # raises, falling through to the abort branch which discards the overlay).
            if mvcc is not None:
                mvcc.validate(self.state)
                mvcc.commit(self.state)

        except BaseException:
            if is_root:
                # Revert this transaction's writes across every participating store: the undo
                # journal (documents under read-committed; outbox/inbox always) and the
                # identity deep-snapshot. The MVCC document overlay needs no undo — its
                # buffered writes never reached the live store.
                if journal := _journal.get():
                    undo(journal)

                if identity_snapshot is not None:
                    self.state.restore_identity(identity_snapshot)

            raise

        finally:
            if mvcc is not None:
                mvcc.finish(self.state)

            if token_mvcc is not None:
                _mvcc_tx.reset(token_mvcc)

            if token_ro is not None:
                _mock_tx_read_only.reset(token_ro)

            if token_journal is not None:
                _journal.reset(token_journal)

            _journal_tx_depth.reset(token_depth)
