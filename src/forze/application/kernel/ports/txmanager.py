from typing import AsyncContextManager, Protocol, runtime_checkable

# ----------------------- #


@runtime_checkable
class TxManagerPort(Protocol):
    """Transactional boundary for the current execution context.

    Implementations define what "transaction" means for the primary persistence
    layer. Nested transactions may be supported via savepoints, but callers
    must not assume a specific strategy unless documented by the implementation.
    """

    def transaction(self) -> AsyncContextManager[None]:
        """Return an async context manager that scopes a transaction."""
        ...
