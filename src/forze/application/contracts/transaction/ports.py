"""Transaction manager and scoped port contracts."""

from typing import (
    AsyncContextManager,
    Awaitable,
    Callable,
    Protocol,
    final,
    runtime_checkable,
)
from uuid import UUID

import attrs

from forze.base.primitives import uuid7

# ----------------------- #
#! TODO: get rid of redundant things


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

    id: UUID = attrs.field(factory=uuid7, init=False)  #!? not necessary ?
    """The unique identifier of the transaction."""


# ....................... #
#! TODO: rename to deferrable* or so


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

    def transaction(self) -> AsyncContextManager[None]:
        """Return an async context manager that scopes a transaction.

        On entry, begins a transaction; on exit, commits or rolls back
        according to implementation policy.
        """
        ...
