"""Postgres principal epochs — raised and held on the caller's transaction."""

from __future__ import annotations

from typing import final
from uuid import UUID

import attrs
from psycopg import sql

from forze.application.contracts.authn import PrincipalEpochPort
from forze_postgres.execution.deps.configs.principal_epoch import PostgresPrincipalEpochConfig
from forze_postgres.kernel.client import PostgresClientPort
from forze_postgres.kernel.gateways.base import PostgresQualifiedName
from forze_postgres.kernel.relation import resolve_postgres_qname

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresPrincipalEpochStore(PrincipalEpochPort):
    """Each principal's epoch, on the caller's transaction connection.

    :meth:`advance` is one ``INSERT … ON CONFLICT DO UPDATE``: it locks the principal's row,
    or claims its key when there is none, until the transaction ends. :meth:`hold` creates the
    row when absent and reads it ``FOR SHARE``. So a raise and an issue serialize, whichever
    creates the row, and a revocation that raises before it scans the sessions finds every
    session issued under the old epoch. The table is provided by the application; expected
    schema::

        CREATE TABLE <relation> (
            principal_id uuid   PRIMARY KEY,
            epoch        bigint NOT NULL
        );

    Never delete a row while a token might carry its epoch: a missing row reads as ``0``.
    """

    client: PostgresClientPort
    config: PostgresPrincipalEpochConfig

    # ....................... #

    async def _table(self) -> PostgresQualifiedName:
        # Principal-global (not tenant-partitioned): resolve the bare relation.
        return await resolve_postgres_qname(self.config.relation, None)

    # ....................... #

    async def current(self, principal_id: UUID) -> int:
        table = await self._table()
        stmt = sql.SQL("SELECT epoch FROM {table} WHERE principal_id = {principal}").format(
            table=table.ident(),
            principal=sql.Placeholder("principal"),
        )

        row = await self.client.fetch_one(stmt, {"principal": principal_id}, row_factory="tuple")

        return 0 if row is None else int(row[0])

    # ....................... #

    async def hold(self, principal_id: UUID) -> int:
        self.client.require_transaction()
        table = await self._table()
        params = {"principal": principal_id}

        await self.client.execute(
            sql.SQL(
                "INSERT INTO {table} (principal_id, epoch) VALUES ({principal}, 0) "
                "ON CONFLICT (principal_id) DO NOTHING"
            ).format(table=table.ident(), principal=sql.Placeholder("principal")),
            params,
        )

        row = await self.client.fetch_one(
            sql.SQL("SELECT epoch FROM {table} WHERE principal_id = {principal} FOR SHARE").format(
                table=table.ident(), principal=sql.Placeholder("principal")
            ),
            params,
            row_factory="tuple",
        )

        return 0 if row is None else int(row[0])

    # ....................... #

    async def advance(self, principal_id: UUID) -> int:
        self.client.require_transaction()
        table = await self._table()

        row = await self.client.fetch_one(
            sql.SQL(
                "INSERT INTO {table} (principal_id, epoch) VALUES ({principal}, 1) "
                "ON CONFLICT (principal_id) DO UPDATE SET epoch = {table}.epoch + 1 "
                "RETURNING epoch"
            ).format(table=table.ident(), principal=sql.Placeholder("principal")),
            {"principal": principal_id},
            row_factory="tuple",
        )

        return 0 if row is None else int(row[0])
