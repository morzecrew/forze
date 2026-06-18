"""Mongo transaction manager implementing :class:`~forze.application.contracts.tx.TxManagerPort`."""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from contextlib import asynccontextmanager
from typing import AsyncGenerator, final

import attrs
from pymongo.read_concern import ReadConcern

from forze.application.contracts.transaction import (
    IsolationLevel as CoreIsolationLevel,
)
from forze.application.contracts.transaction import (
    TransactionManagerPort,
    TransactionScopeKey,
    TxCapabilities,
)

from ..kernel.client import MongoClientPort, MongoTransactionOptions
from ._logger import logger

# ----------------------- #

MongoTxScopeKey = TransactionScopeKey("mongo")
"""Key used to scope the Mongo transaction."""

_MONGO_ISOLATION = frozenset(
    {CoreIsolationLevel.READ_COMMITTED, CoreIsolationLevel.SNAPSHOT}
)
"""Mongo multi-document transactions provide snapshot isolation (and the weaker
read-committed); they have no serializable mode, so requiring SERIALIZABLE fails closed."""

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

    def capabilities(self) -> TxCapabilities:
        return TxCapabilities(isolation=_MONGO_ISOLATION)

    # ....................... #

    @asynccontextmanager
    async def transaction(
        self,
        *,
        read_only: bool = False,
        isolation: CoreIsolationLevel | None = None,
    ) -> AsyncGenerator[None]:
        """Open Mongo transaction for the duration of the context.

        ``read_only`` is accepted for interface parity but not enforced — Mongo
        multi-document transactions have no read-only mode (a read-only intent is better
        expressed via a read preference / secondary). The Phase-1 port guard still prevents
        a ``QUERY`` operation from acquiring a write port.

        ``isolation=SNAPSHOT`` (validated against :meth:`capabilities`) sets the transaction's
        read concern to ``snapshot``; ``READ_COMMITTED`` leaves the configured default.
        """

        options = self.options

        if isolation is CoreIsolationLevel.SNAPSHOT:
            options = attrs.evolve(options, read_concern=ReadConcern("snapshot"))

        logger.debug("Starting transaction (read_only=%s)", read_only)

        async with self.client.transaction(options=options):
            try:
                yield

            #! Hmmm.. should it be like that?
            except Exception:
                logger.debug("Transaction rolled back")
                raise

            else:
                logger.debug("Transaction committed")
