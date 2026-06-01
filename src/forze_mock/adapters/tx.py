"""No-op mock transaction manager."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import (
    final,
)
import attrs
from forze.application.contracts.transaction import (
    TransactionManagerPort,
    TransactionScopeKey,
)

MockTxScopeKey = TransactionScopeKey("mock")
"""Scope key for the in-memory mock transaction manager."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockTxManagerAdapter(TransactionManagerPort):
    """No-op transaction manager for mock environments."""

    # ....................... #

    @property
    def scope_key(self) -> TransactionScopeKey:
        return MockTxScopeKey

    # ....................... #

    def transaction(self):  # type: ignore[no-untyped-def]
        @asynccontextmanager
        async def _noop():  # type: ignore[no-untyped-def]
            yield

        return _noop()
