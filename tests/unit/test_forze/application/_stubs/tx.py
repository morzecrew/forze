"""In-memory stub for TxManagerPort."""

from contextlib import asynccontextmanager
from typing import AsyncContextManager, final

from forze.application.contracts._ports.tx import TxManagerPort, TxScopeKey

# ----------------------- #


@final
class InMemoryTxManagerPort(TxManagerPort):
    """No-op transaction manager for unit tests. No real transactions."""

    def __init__(self, scope_key: TxScopeKey | None = None) -> None:
        self._scope_key = scope_key or TxScopeKey(name="stub")

    def scope_key(self) -> TxScopeKey:
        return self._scope_key

    def transaction(self) -> AsyncContextManager[None]:
        @asynccontextmanager
        async def _noop():
            yield

        return _noop()
