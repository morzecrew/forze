from typing import AsyncContextManager, Protocol, final, runtime_checkable

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True)
class TxScopeKey:
    name: str


# ....................... #


@attrs.define(slots=True, frozen=True)
class TxOptions: ...


#! ^^^ And how to make it abstract ????

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
