"""Mongo transaction manager implementing :class:`~forze.application.contracts.tx.TxManagerPort`."""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from contextlib import asynccontextmanager
from typing import AsyncIterator, final

import attrs

from forze.application.contracts.tx import TxManagerPort, TxScopeKey

from ..kernel.platform import MongoClient, MongoTransactionOptions
from ._logger import logger

# ----------------------- #

MongoTxScopeKey = TxScopeKey("mongo")
"""Key used to scope the Mongo transaction."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoTxManagerAdapter(TxManagerPort):
    """Mongo-backed :class:`TxManagerPort` that delegates to :meth:`MongoClient.transaction`."""

    client: MongoClient
    """Client instance instance."""

    options: MongoTransactionOptions = attrs.field(factory=MongoTransactionOptions)
    """Transaction options forwarded to the Mongo session."""

    # Non initable fields
    scope_key: TxScopeKey = attrs.field(default=MongoTxScopeKey, init=False)
    """The key used to scope the transaction."""

    # ....................... #

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        """Open Mongo transaction for the duration of the context."""

        #! TODO: log options
        logger.debug("Starting transaction")

        async with self.client.transaction(options=self.options):
            try:
                yield

            #! Hmmm.. should it be like that?
            except Exception:
                logger.debug("Transaction rolled back")
                raise

            else:
                logger.debug("Transaction committed")
