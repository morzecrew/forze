"""Firestore transaction manager implementing :class:`~forze.application.contracts.tx.TxManagerPort`."""

from forze_firestore._compat import require_firestore

require_firestore()

# ....................... #

from contextlib import asynccontextmanager
from typing import AsyncGenerator, final

import attrs

from forze.application.contracts.transaction import (
    TransactionManagerPort,
    TransactionScopeKey,
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

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None]:
        logger.debug("Starting Firestore transaction")

        async with self.client.transaction():
            try:
                yield

            except Exception:
                logger.debug("Firestore transaction rolled back")
                raise

            else:
                logger.debug("Firestore transaction committed")
