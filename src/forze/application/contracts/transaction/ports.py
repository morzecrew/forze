"""Transaction manager and scoped port contracts."""

from typing import (
    AsyncContextManager,
    Awaitable,
    Callable,
    Protocol,
    final,
    runtime_checkable,
)

import attrs

# ----------------------- #


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

    def transaction(self, *, read_only: bool = False) -> AsyncContextManager[None]:
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
        """
        ...
