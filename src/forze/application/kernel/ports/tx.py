from typing import AsyncContextManager, Protocol, runtime_checkable
from uuid import UUID

import attrs

from forze.base.primitives import uuid7

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class TxScopeKey:
    name: str


# ....................... #


@attrs.define(slots=True, frozen=True)
class TxHandle:
    """Opaque capability token for transactional execution."""

    scope: TxScopeKey
    """The scope of the transaction."""

    id: UUID = attrs.field(factory=uuid7, init=False)
    """The unique identifier of the transaction."""


# ....................... #


@runtime_checkable
class TxScopedPort(Protocol):
    tx_scope: TxScopeKey


# ....................... #


@runtime_checkable
class TxManagerPort(Protocol):
    """Transactional boundary for the current execution context.

    Implementations define what "transaction" means for the primary persistence
    layer. Nested transactions may be supported via savepoints, but callers
    must not assume a specific strategy unless documented by the implementation.
    """

    def scope_key(self) -> TxScopeKey:
        """Return the key used to scope the transaction."""
        ...

    def transaction(self) -> AsyncContextManager[None]:
        """Return an async context manager that scopes a transaction."""
        ...
