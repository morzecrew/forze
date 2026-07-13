"""Postgres HLC high-water-mark store — co-located, in-transaction advance."""

from __future__ import annotations

from typing import final

import attrs
from psycopg import sql

from forze.application.contracts.hlc import HlcCheckpointPort
from forze.base.primitives import HlcTimestamp
from forze_postgres.execution.deps.configs.hlc_checkpoint import (
    PostgresHlcCheckpointConfig,
)
from forze_postgres.kernel.client import PostgresClientPort
from forze_postgres.kernel.gateways.base import PostgresQualifiedName
from forze_postgres.kernel.relation import resolve_postgres_qname

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresHlcCheckpointStore(HlcCheckpointPort):
    """Postgres store for a node's HLC high-water mark.

    :meth:`advance` runs on the caller's transaction connection — the outbox flush invokes
    it inside the business transaction — so the mark and the HLC-stamped rows commit
    atomically: a committed stamp is never durable without a mark covering it, and a
    rolled-back flush does not advance the mark. :meth:`load` reads the max across all node
    rows at startup, so a restart resumes above the whole deployment's emissions.

    The mark is a monotonic max (``GREATEST``), so out-of-order or concurrent writers never
    lower it. The table is provided by the application; expected schema::

        CREATE TABLE <relation> (
            node_key text   NOT NULL,
            hlc      bigint NOT NULL,   -- packed HlcTimestamp: physical_ms << 16 | logical
            PRIMARY KEY (node_key)
        );
    """

    client: PostgresClientPort
    config: PostgresHlcCheckpointConfig

    # ....................... #

    async def _table(self) -> PostgresQualifiedName:
        # Node-global (not tenant-partitioned): resolve the bare relation.
        return await resolve_postgres_qname(self.config.relation, None)

    # ....................... #

    async def load(self) -> HlcTimestamp | None:
        table = await self._table()
        stmt = sql.SQL("SELECT max(hlc) FROM {table}").format(table=table.ident())

        row = await self.client.fetch_one(stmt, {}, row_factory="tuple")

        if row is None or row[0] is None:
            return None

        return HlcTimestamp.unpack(int(row[0]))

    # ....................... #

    async def advance(self, mark: HlcTimestamp) -> None:
        table = await self._table()

        # Monotonic max upsert on the node's row, inside the business transaction so the
        # mark commits with the rows it stamps. GREATEST keeps it from ever going backwards.
        stmt = sql.SQL(
            "INSERT INTO {table} (node_key, hlc) VALUES ({node_key}, {hlc}) "
            "ON CONFLICT (node_key) DO UPDATE "
            "SET hlc = GREATEST({table}.hlc, EXCLUDED.hlc)"
        ).format(
            table=table.ident(),
            node_key=sql.Placeholder("node_key"),
            hlc=sql.Placeholder("hlc"),
        )

        await self.client.execute(stmt, {"node_key": self.config.node_key, "hlc": mark.pack()})
