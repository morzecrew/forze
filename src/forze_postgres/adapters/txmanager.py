"""Postgres adapter implementing the transaction manager port."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from contextlib import asynccontextmanager
from typing import AsyncGenerator, final

import attrs

from forze.application.contracts.transaction import (
    TransactionManagerPort,
    TransactionScopeKey,
)

from ..kernel.client import PostgresClientPort, PostgresTransactionOptions
from ._logger import logger

# ----------------------- #

PostgresTxScopeKey = TransactionScopeKey("postgres")
"""Key used to scope the Postgres transaction."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresTxManagerAdapter(TransactionManagerPort):
    """Postgres-backed :class:`TxManagerPort` that delegates to :meth:`PostgresClient.transaction`."""

    client: PostgresClientPort
    """Client instance."""

    options: PostgresTransactionOptions = attrs.field(
        factory=PostgresTransactionOptions
    )
    """Transaction options forwarded to the Postgres client."""

    # ....................... #

    @property
    def scope_key(self) -> TransactionScopeKey:
        return PostgresTxScopeKey

    # ....................... #

    @asynccontextmanager
    async def transaction(self, *, read_only: bool = False) -> AsyncGenerator[None]:
        """Open Postgres transaction for the duration of the context.

        ``read_only`` (set for ``QUERY`` operations) opens the transaction with
        ``BEGIN ... READ ONLY`` so the database rejects writes. A route configured
        read-only at construction stays read-only regardless (restrictive OR).
        """

        options = (
            attrs.evolve(self.options, read_only=True)
            if read_only and not self.options.read_only
            else self.options
        )

        logger.debug("Starting transaction (read_only=%s)", options.read_only)

        async with self.client.transaction(options=options):
            try:
                yield

            #! Hmmm.. should it be like that?
            except Exception:
                logger.debug("Transaction rolled back")
                raise

            else:
                logger.debug("Transaction committed")
