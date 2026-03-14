"""Postgres adapter implementing the transaction manager port."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from contextlib import asynccontextmanager
from typing import AsyncIterator, final

import attrs

from forze.application.contracts.tx import TxManagerPort, TxScopeKey
from forze.base.logging import getLogger

from ..kernel.platform import PostgresClient, PostgresTransactionOptions

# ----------------------- #

logger = getLogger(__name__).bind(scope="postgres.txmanager")

# ....................... #
PostgresTxScopeKey = TxScopeKey("postgres")
"""Key used to scope the Postgres transaction."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresTxManagerAdapter(TxManagerPort):
    """Postgres-backed :class:`TxManagerPort` that delegates to :meth:`PostgresClient.transaction`."""

    client: PostgresClient
    options: PostgresTransactionOptions = attrs.field(
        factory=PostgresTransactionOptions
    )

    # ....................... #

    def scope_key(self) -> TxScopeKey:
        return PostgresTxScopeKey

    # ....................... #

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        logger.debug("Starting transaction")

        async with self.client.transaction(options=self.options):
            yield
