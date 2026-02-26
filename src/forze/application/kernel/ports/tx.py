from typing import AsyncContextManager, Protocol, runtime_checkable

import attrs

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class TxScopeKey:
    name: str


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
