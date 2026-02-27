from typing import (
    TYPE_CHECKING,
    AsyncContextManager,
    Protocol,
    final,
    runtime_checkable,
)
from uuid import UUID

import attrs

from forze.base.primitives import uuid7

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
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

# @attrs.define(slots=True, frozen=True)
# class TxOptions: ...


# #! ^^^ And how to make it abstract ????

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

    #! we should add tx options to this protocol method
    def transaction(self) -> AsyncContextManager[None]:
        """Return an async context manager that scopes a transaction."""
        ...


# ....................... #


@runtime_checkable
class TxContextScopedPort(Protocol):
    """Port that requires a transaction scope to be matched with the current execution context."""

    ctx: "ExecutionContext"
    tx_scope: TxScopeKey
