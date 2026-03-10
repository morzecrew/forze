"""Mongo transaction manager implementing :class:`~forze.application.contracts.tx.TxManagerPort`."""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from contextlib import asynccontextmanager
from typing import AsyncIterator, final

import attrs

from forze.application.contracts.tx import TxManagerPort, TxScopeKey

from ..kernel.platform import MongoClient, MongoTransactionOptions

# ----------------------- #

MongoTxScopeKey = TxScopeKey("mongo")
"""Key used to scope the Mongo transaction."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoTxManagerAdapter(TxManagerPort):
    """Mongo adapter for managing transactions through :class:`MongoClient`.

    Wraps the client's :meth:`~MongoClient.transaction` context manager and
    exposes the :data:`MongoTxScopeKey` so the execution plan can group
    Mongo-scoped operations under a single transaction boundary.
    """

    client: MongoClient
    """Shared :class:`MongoClient` instance."""

    options: MongoTransactionOptions = attrs.field(factory=MongoTransactionOptions)
    """Transaction options forwarded to the Mongo session."""

    # ....................... #

    def scope_key(self) -> TxScopeKey:
        """Return the Mongo transaction scope key."""

        return MongoTxScopeKey

    # ....................... #

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        """Open a Mongo transaction for the duration of the context."""

        async with self.client.transaction(options=self.options):
            yield
