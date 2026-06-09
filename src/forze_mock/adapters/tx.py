"""No-op mock transaction manager."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import TYPE_CHECKING, final

import attrs

from forze.application.contracts.transaction import (
    TransactionManagerPort,
    TransactionScopeKey,
)

if TYPE_CHECKING:
    from forze_mock.state import MockState

MockTxScopeKey = TransactionScopeKey("mock")
"""Scope key for the in-memory mock transaction manager."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockTxManagerAdapter(TransactionManagerPort):
    """No-op transaction manager for mock environments.

    Records each transaction's ``read_only`` flag on the shared state so tests can assert
    a ``QUERY`` operation opened its transaction read-only.
    """

    state: MockState | None = attrs.field(default=None)

    # ....................... #

    @property
    def scope_key(self) -> TransactionScopeKey:
        return MockTxScopeKey

    # ....................... #

    def transaction(
        self, *, read_only: bool = False
    ) -> AbstractAsyncContextManager[None]:
        if self.state is not None:
            self.state.tx_read_only_calls.append(read_only)

        @asynccontextmanager
        async def _noop() -> AsyncGenerator[None]:
            yield

        return _noop()
