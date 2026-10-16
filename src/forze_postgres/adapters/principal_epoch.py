"""Postgres principal epochs — raised and held on the caller's transaction."""

from __future__ import annotations

from typing import final
from uuid import UUID

import attrs
from psycopg import sql

from forze.application.contracts.authn import PrincipalEpochPort
from forze.base.exceptions import exc
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

    The exclusion needs the raise at read committed (the Postgres default), where the
    revocation's scan, a later statement, sees the session an issue committed while the raise
    waited. At repeatable read or serializable the scan reads the snapshot taken before the
    wait and misses it — serializable isolation checks only against other serializable
    transactions, and an issue need not be one — so a raise there is refused.
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
                "RETURNING epoch, current_setting('transaction_isolation')"
            ).format(table=table.ident(), principal=sql.Placeholder("principal")),
            {"principal": principal_id},
            row_factory="tuple",
        )

        if row is None:
            return 0

        # Refused before the scan, so the whole revocation rolls back with it. Postgres runs
        # read uncommitted as read committed.
        if row[1] not in ("read committed", "read uncommitted"):
            raise exc.configuration(
                f"A principal's epoch cannot be raised at {row[1]}: the revocation's session "
                "scan would read a snapshot older than a session issued meanwhile. Run "
                "revocations at read committed.",
                code="authn_epoch_isolation",
            )

        return int(row[0])
