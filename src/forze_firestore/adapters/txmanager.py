"""Firestore transaction manager implementing :class:`~forze.application.contracts.tx.TxManagerPort`."""

from forze_firestore._compat import require_firestore

require_firestore()

# ....................... #

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import final

import attrs

from forze.application.contracts.transaction import (
    IsolationLevel as CoreIsolationLevel,
)
from forze.application.contracts.transaction import (
    TransactionManagerPort,
    TransactionScopeKey,
    TxCapabilities,
)

from ..kernel.client import FirestoreClientPort
from ._logger import logger

# ----------------------- #

FirestoreTxScopeKey = TransactionScopeKey("firestore")

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class FirestoreTxManagerAdapter(TransactionManagerPort):
    """Firestore-backed transaction manager."""

    client: FirestoreClientPort
    """Client instance."""

    # ....................... #

    @property
    def scope_key(self) -> TransactionScopeKey:
        return FirestoreTxScopeKey

    # ....................... #

    def capabilities(self) -> TxCapabilities:
        # Firestore transactions are always serializable, which satisfies any requested level.
        return TxCapabilities(isolation=frozenset(CoreIsolationLevel))

    # ....................... #

    def is_transactionally_enlisted(self) -> bool:
        """Whether a scope opened now joins a transaction already open on this client.

        ``False`` on a client with none open: the scope then begins a transaction of its own,
        which commits at its exit even inside another route's root (see
        :class:`~forze.application.contracts.transaction.TransactionallyEnlistable`).
        """

        return self.client.is_in_transaction()

    # ....................... #

    @asynccontextmanager
    async def transaction(
        self,
        *,
        read_only: bool = False,
        isolation: CoreIsolationLevel | None = None,
    ) -> AsyncGenerator[None]:
        # ``read_only`` accepted for interface parity; Firestore transactions have no
        # read-only mode. ``isolation`` is accepted and always satisfied — Firestore
        # transactions are serializable. The Phase-1 port guard still blocks writes in a QUERY.
        logger.debug("Starting Firestore transaction (read_only=%s)", read_only)

        async with self.client.transaction():
            try:
                yield

            except Exception:
                logger.debug("Firestore transaction rolled back")
                raise

            else:
                logger.debug("Firestore transaction committed")
