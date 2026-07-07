"""Neo4j transaction manager implementing
:class:`~forze.application.contracts.transaction.TransactionManagerPort` /
:class:`~forze.application.contracts.transaction.IsolationAware`."""

from forze_neo4j._compat import require_neo4j

require_neo4j()

# ....................... #

from contextlib import asynccontextmanager
from typing import AsyncGenerator, final

import attrs

from forze.application.contracts.transaction import (
    IsolationLevel,
    TransactionManagerPort,
    TransactionScopeKey,
    TxCapabilities,
)

from ..kernel.client import Neo4jClientPort
from ._logger import logger

# ----------------------- #

Neo4jTxScopeKey = TransactionScopeKey("neo4j")

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class Neo4jTxManagerAdapter(TransactionManagerPort):
    """Neo4j-backed transaction manager.

    Enlists the client's explicit transaction into the framework transaction scope so a
    handler's graph writes commit or roll back as a unit (a rollback on any exception). Neo4j
    runs at READ COMMITTED and the level is not configurable, so only that level is
    advertised — a stronger requirement is rejected at first resolve (fail closed).

    **Not co-transactional with other backends.** Neo4j is a separate database from the
    Postgres outbox (or any other store): there is no cross-database two-phase commit, so a
    graph write and an outbox write are not atomic with each other. This scope only makes a
    handler's *graph* statements atomic among themselves.
    """

    client: Neo4jClientPort
    """Client instance (may be a routed per-tenant client)."""

    # ....................... #

    @property
    def scope_key(self) -> TransactionScopeKey:
        return Neo4jTxScopeKey

    # ....................... #

    def capabilities(self) -> TxCapabilities:
        # Neo4j transactions are READ COMMITTED and the level is not selectable.
        return TxCapabilities(isolation=frozenset({IsolationLevel.READ_COMMITTED}))

    # ....................... #

    @asynccontextmanager
    async def transaction(
        self,
        *,
        read_only: bool = False,
        isolation: IsolationLevel | None = None,
    ) -> AsyncGenerator[None]:
        # ``isolation`` only ever arrives as READ_COMMITTED (stronger levels are rejected at
        # resolve against ``capabilities``); accepted for interface parity. ``read_only`` is
        # likewise accepted for parity — Neo4j has no read-only transaction mode wired here,
        # and the Phase-1 port guard still blocks writes in a QUERY.
        _ = isolation
        logger.debug("Starting Neo4j transaction (read_only=%s)", read_only)

        async with self.client.transaction():
            try:
                yield

            except BaseException:
                # BaseException (not just Exception) so a cancelled/interrupted transaction also
                # logs the rollback the client will perform as the exception propagates.
                logger.debug("Neo4j transaction rolled back")
                raise

            else:
                logger.debug("Neo4j transaction committed")
