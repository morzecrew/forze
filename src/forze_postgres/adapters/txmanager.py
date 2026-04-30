"""Postgres adapter implementing the transaction manager port."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from contextlib import asynccontextmanager
from typing import AsyncIterator, final

import attrs

from forze.application.contracts.tx import TxManagerPort, TxScopeKey

from ..kernel.platform import PostgresClientPort, PostgresTransactionOptions
from ._logger import logger

# ----------------------- #

PostgresTxScopeKey = TxScopeKey("postgres")
"""Key used to scope the Postgres transaction."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresTxManagerAdapter(TxManagerPort):
    """Postgres-backed :class:`TxManagerPort` that delegates to :meth:`PostgresClient.transaction`."""

    client: PostgresClientPort
    """Client instance."""

    options: PostgresTransactionOptions = attrs.field(
        factory=PostgresTransactionOptions
    )
    """Transaction options forwarded to the Postgres client."""

    # Non initable fields
    scope_key: TxScopeKey = attrs.field(default=PostgresTxScopeKey, init=False)
    """The key used to scope the transaction."""

    # ....................... #

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        """Open Postgres transaction for the duration of the context."""

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
