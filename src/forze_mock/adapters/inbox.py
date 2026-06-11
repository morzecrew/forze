"""In-memory inbox (consumer-side dedup) adapter."""

from __future__ import annotations

from typing import final

import attrs

from forze.application.contracts.inbox import InboxPort
from forze_mock.adapters.tx import ensure_mock_tx_writable
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin, partition_namespace

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockInboxAdapter(MockTenancyMixin, InboxPort):
    """In-memory consumer-side dedup adapter."""

    state: MockState
    namespace: str

    # ....................... #

    def _key(self, inbox: str, message_id: str) -> tuple[str, str, str]:
        ns = partition_namespace(self.require_tenant_if_aware(), self.namespace)
        return ns, inbox, message_id

    # ....................... #

    async def mark_if_unseen(self, inbox: str, message_id: str) -> bool:
        # Inbox marks are DB rows in production: writing one inside a strict
        # read-only root raises, like Postgres ``BEGIN ... READ ONLY`` would.
        ensure_mock_tx_writable(store=f"inbox:{self.namespace}")

        with self.state.lock:
            key = self._key(inbox, message_id)

            if key in self.state.inbox:
                return False

            self.state.inbox.add(key)
            return True
