"""Postgres adapter implementing the transaction manager port."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from contextlib import asynccontextmanager
from typing import AsyncGenerator, final

import attrs

from forze.application.contracts.transaction import (
    IsolationLevel as CoreIsolationLevel,
)
from forze.application.contracts.transaction import (
    TransactionManagerPort,
    TransactionScopeKey,
    TxCapabilities,
)

from ..kernel.client import PostgresClientPort, PostgresTransactionOptions
from ..kernel.client.types import IsolationLevel as PgIsolationLevel
from ._logger import logger

# ----------------------- #

PostgresTxScopeKey = TransactionScopeKey("postgres")
"""Key used to scope the Postgres transaction."""

_PG_ISOLATION: dict[CoreIsolationLevel, PgIsolationLevel] = {
    CoreIsolationLevel.READ_COMMITTED: "read_committed",
    CoreIsolationLevel.SNAPSHOT: "repeatable_read",  # Postgres snapshot isolation
    CoreIsolationLevel.SERIALIZABLE: "serializable",
}
"""Map the kernel's intent-named isolation to the Postgres level (Postgres can do all three)."""

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

    def capabilities(self) -> TxCapabilities:
        # Postgres honors every level natively (snapshot via REPEATABLE READ).
        return TxCapabilities(isolation=frozenset(CoreIsolationLevel))

    # ....................... #

    @asynccontextmanager
    async def transaction(
        self,
        *,
        read_only: bool = False,
        isolation: CoreIsolationLevel | None = None,
    ) -> AsyncGenerator[None]:
        """Open Postgres transaction for the duration of the context.

        ``read_only`` (set for ``QUERY`` operations) opens the transaction with
        ``BEGIN ... READ ONLY`` so the database rejects writes. A route configured
        read-only at construction stays read-only regardless (restrictive OR).

        ``isolation`` (a kernel :class:`IsolationLevel`, validated against
        :meth:`capabilities` before this runs) maps to the Postgres level on the ``BEGIN``.
        """

        options = self.options

        if read_only and not options.read_only:
            options = attrs.evolve(options, read_only=True)

        if isolation is not None:
            options = attrs.evolve(options, isolation=_PG_ISOLATION[isolation])

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
