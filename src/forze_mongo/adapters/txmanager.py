"""Mongo transaction manager implementing :class:`~forze.application.contracts.tx.TxManagerPort`."""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from contextlib import asynccontextmanager
from typing import AsyncGenerator, final

import attrs

from forze.application.contracts.transaction import (
    TransactionManagerPort,
    TransactionScopeKey,
)

from ..kernel.client import MongoClientPort, MongoTransactionOptions
from ._logger import logger

# ----------------------- #

MongoTxScopeKey = TransactionScopeKey("mongo")
"""Key used to scope the Mongo transaction."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoTxManagerAdapter(TransactionManagerPort):
    """Mongo-backed :class:`TxManagerPort` that delegates to :meth:`MongoClient.transaction`."""

    client: MongoClientPort
    """Client instance instance."""

    options: MongoTransactionOptions = attrs.field(factory=MongoTransactionOptions)
    """Transaction options forwarded to the Mongo session."""

    # ....................... #

    @property
    def scope_key(self) -> TransactionScopeKey:
        return MongoTxScopeKey

    # ....................... #

    @asynccontextmanager
    async def transaction(self, *, read_only: bool = False) -> AsyncGenerator[None]:
        """Open Mongo transaction for the duration of the context.

        ``read_only`` is accepted for interface parity but not enforced — Mongo
        multi-document transactions have no read-only mode (a read-only intent is better
        expressed via a read preference / secondary). The Phase-1 port guard still prevents
        a ``QUERY`` operation from acquiring a write port.
        """

        logger.debug("Starting transaction (read_only=%s)", read_only)

        async with self.client.transaction(options=self.options):
            try:
                yield

            #! Hmmm.. should it be like that?
            except Exception:
                logger.debug("Transaction rolled back")
                raise

            else:
                logger.debug("Transaction committed")
