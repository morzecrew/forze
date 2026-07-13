"""Transaction manager and scoped port contracts."""

from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from enum import IntEnum
from typing import (
    Protocol,
    final,
    runtime_checkable,
)

import attrs

# ----------------------- #


@final
class IsolationLevel(IntEnum):
    """Transaction isolation level, ordered weakest → strongest.

    Intent-named (the *guarantee*, not a SQL keyword): an adapter maps each to its backend's
    spelling in :meth:`IsolationAware.capabilities` (e.g. ``SNAPSHOT`` → Postgres ``REPEATABLE
    READ``). Ordered so ``required <= supplied`` and "at least as strong as" comparisons are
    direct.

    - ``READ_COMMITTED`` — each statement sees the latest committed data.
    - ``SNAPSHOT`` — the whole transaction reads a single consistent snapshot (no
      non-repeatable reads / phantoms), but write-skew is permitted.
    - ``SERIALIZABLE`` — the outcome is equivalent to some serial order (write-skew rejected).
    """

    READ_COMMITTED = 1
    SNAPSHOT = 2
    SERIALIZABLE = 3


# ....................... #


@runtime_checkable
class TransactionallyEnlistable(Protocol):
    """A resource that can report whether its writes commit in the *ambient* transaction.

    A store (e.g. an inbox) is *enlisted* when its writes go through the same connection the
    active :meth:`~forze.application.execution.context.transaction.TransactionContext.scope`
    opened its transaction on — so they commit atomically with the rest of the unit of work.
    A store bound to a **different** client/pool is not enlisted: its write commits on its own
    connection, silently breaking any "commit atomically" guarantee. See
    :meth:`~forze.application.execution.context.transaction.TransactionContext.assert_enlisted`.
    """

    def is_transactionally_enlisted(self) -> bool:
        """Whether this resource's writes participate in the ambient transaction."""
        ...  # pragma: no cover


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TxCapabilities:
    """What a transaction manager supports — its reported, fail-closed contract.

    A required isolation not in :attr:`isolation` is rejected at first resolve (see
    :meth:`~forze.application.execution.context.transaction.TransactionContext.scope`); a
    manager that does not report capabilities at all (not :class:`IsolationAware`) cannot
    satisfy *any* explicit isolation requirement.
    """

    isolation: frozenset[IsolationLevel]
    """Isolation levels this manager can honor.

    The capability report intentionally covers only what the kernel **enforces** at resolve
    (the declared isolation). Other transaction features (savepoints / partial rollback,
    read-only enforcement) are honored by the adapter at runtime, not advertised here — add
    a field only when the kernel grows a fail-closed check that consults it."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class TransactionScopeKey:
    """Identifier for a transaction scope (e.g. database vs cache)."""

    name: str
    """Scope name used to match ports with the active transaction."""


# ....................... #


@attrs.define(slots=True, frozen=True)
class TransactionHandle:
    """Opaque capability token for transactional execution."""

    scope: TransactionScopeKey
    """The scope of the transaction."""

    read_only: bool = attrs.field(default=False, kw_only=True)
    """Whether the root transaction was opened read-only (nested scopes inherit it)."""

    isolation: "IsolationLevel | None" = attrs.field(default=None, kw_only=True)
    """Isolation level of the root transaction, if one was requested (nested scopes inherit
    it). ``None`` leaves the manager's default."""


# ....................... #


@runtime_checkable
class AfterCommitPort(Protocol):
    """Run async side effects after a successful DB commit when in a transaction.

    Implementations align with :meth:`forze.application.execution.ExecutionContext.transaction`
    (e.g. defer until commit, or run immediately when no transaction is active).
    """

    def __call__(self, cb: Callable[[], Awaitable[None]]) -> Awaitable[None]:
        """Await *cb* now if outside a transaction; else run it after commit."""
        ...


# ....................... #


@runtime_checkable
class TransactionManagerPort(Protocol):
    """Transaction manager port."""

    @property
    def scope_key(self) -> TransactionScopeKey:
        """Return the key used to scope the transaction."""
        ...

    def transaction(self, *, read_only: bool = False) -> AbstractAsyncContextManager[None]:
        """Return an async context manager that scopes a transaction.

        On entry, begins a transaction; on exit, commits or rolls back
        according to implementation policy. When ``read_only`` is true the backend opens a
        read-only transaction where supported (e.g. Postgres ``BEGIN ... READ ONLY``), so
        the database rejects writes — used for ``QUERY`` operations.

        Nesting contract: a nested call (one issued while a transaction on the same
        scope key is already active) opens a **savepoint**, not a new transaction.
        Transaction options (isolation, ``read_only``) are honored only at the root —
        the kernel never forwards ``read_only`` to nested calls, and implementations
        must not attempt mid-transaction option changes.

        Isolation is opt-in: a manager that can honor an explicit
        :class:`IsolationLevel` implements :class:`IsolationAware` (reporting its
        :class:`TxCapabilities` and accepting an ``isolation`` argument). Managers that do
        not implement it run at their default isolation, and any operation declaring an
        explicit isolation against such a manager is rejected at first resolve (fail-closed).
        """
        ...


# ....................... #


@runtime_checkable
class IsolationAware(Protocol):
    """Opt-in extension for transaction managers that can honor an explicit isolation level.

    Kept separate from :class:`TransactionManagerPort` so adding isolation does not force
    every existing manager to change: a manager implements this only when it can report and
    honor isolation. The kernel checks an operation's required isolation against
    :meth:`capabilities` at first resolve and passes ``isolation`` to :meth:`transaction`
    only for managers that implement this protocol.
    """

    def capabilities(self) -> TxCapabilities:
        """Report the isolation levels and features this manager supports."""
        ...

    def transaction(
        self,
        *,
        read_only: bool = False,
        isolation: "IsolationLevel | None" = None,
    ) -> AbstractAsyncContextManager[None]:
        """Open a transaction at *isolation* (honored at root only; nested inherits)."""
        ...
