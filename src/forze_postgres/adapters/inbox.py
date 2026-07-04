"""Postgres consumer-side dedup (inbox) store."""

from __future__ import annotations

from typing import final

import attrs
from psycopg import sql

from forze.application.contracts.inbox import InboxPort, InboxSpec
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.primitives import utcnow
from forze_postgres.execution.deps.configs.inbox import PostgresInboxConfig
from forze_postgres.kernel.client import PostgresClientPort
from forze_postgres.kernel.gateways.base import PostgresQualifiedName
from forze_postgres.kernel.relation import resolve_postgres_qname

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresInboxStore(TenancyMixin, InboxPort):
    """Postgres-backed consumer-side dedup store.

    Marks a message processed in the caller's transaction via
    ``INSERT ... ON CONFLICT DO NOTHING`` — so the mark and the handler's writes
    commit atomically (exactly-once effect). The table is provided by the
    application; expected schema::

        CREATE TABLE <relation> (
            inbox_route  text NOT NULL,
            message_id   text NOT NULL,
            processed_at timestamptz NOT NULL,
            PRIMARY KEY (inbox_route, message_id)
        );
    """

    client: PostgresClientPort
    spec: InboxSpec
    config: PostgresInboxConfig

    # ....................... #

    async def _table(self) -> PostgresQualifiedName:
        tenant_id = self.require_tenant_if_aware()
        return await resolve_postgres_qname(self.config.relation, tenant_id)

    # ....................... #

    def is_transactionally_enlisted(self) -> bool:
        """Whether the dedup mark commits in the ambient transaction.

        ``True`` only when this store's own client is inside a transaction — i.e. the
        surrounding scope opened it on *this* client. A client bound to a different pool is
        not enlisted, so the mark would commit on its own connection (breaking exactly-once).
        """

        return self.client.is_in_transaction()

    # ....................... #

    async def mark_if_unseen(self, inbox: str, message_id: str) -> bool:
        table = await self._table()
        stmt = sql.SQL(
            """
            INSERT INTO {table} (inbox_route, message_id, processed_at)
            VALUES ({route}, {mid}, {at})
            ON CONFLICT (inbox_route, message_id) DO NOTHING
            """
        ).format(
            table=table.ident(),
            route=sql.Placeholder(),
            mid=sql.Placeholder(),
            at=sql.Placeholder(),
        )

        rowcount = await self.client.execute(
            stmt,
            [inbox, message_id, utcnow()],
            return_rowcount=True,
        )

        return bool(rowcount)
