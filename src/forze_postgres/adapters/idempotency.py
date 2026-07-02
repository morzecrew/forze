"""Postgres co-located idempotency store — atomic in-transaction result commit."""

from __future__ import annotations

from typing import final

import attrs
from psycopg import sql

from forze.application.contracts.idempotency import (
    IdempotencyPort,
    IdempotencyRecord,
    IdempotencySpec,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.exceptions import exc
from forze_postgres.execution.deps.configs.idempotency import PostgresIdempotencyConfig
from forze_postgres.kernel.client import PostgresClientPort
from forze_postgres.kernel.gateways.base import PostgresQualifiedName
from forze_postgres.kernel.relation import resolve_postgres_qname

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresIdempotencyStore(TenancyMixin, IdempotencyPort):
    """Postgres-backed co-located idempotency store (``commits_in_transaction``).

    :meth:`commit` runs on the caller's transaction connection — the auto-injected
    ``on_success`` hook invokes it inside the business transaction — so the result record
    and the business writes commit atomically, closing the crash window an
    out-of-transaction store leaves open. :meth:`begin` and :meth:`fail` run out of
    transaction (auto-committed by the client), so a pending claim is immediately visible
    to a concurrent duplicate. Expired rows (past ``IdempotencySpec.ttl``) are re-claimable.

    The table is provided by the application; expected schema::

        CREATE TABLE <relation> (
            op           text        NOT NULL,
            idem_key     text        NOT NULL,
            payload_hash text        NOT NULL,
            status       text        NOT NULL,   -- 'pending' | 'done'
            result       bytea,                   -- NULL while pending
            expires_at   timestamptz NOT NULL,
            PRIMARY KEY (op, idem_key)
        );
    """

    client: PostgresClientPort
    spec: IdempotencySpec
    config: PostgresIdempotencyConfig

    # ....................... #

    @property
    def commits_in_transaction(self) -> bool:
        """Always ``True``: :meth:`commit` writes on the caller's transaction connection."""

        return True

    # ....................... #

    async def _table(self) -> PostgresQualifiedName:
        tenant_id = self.require_tenant_if_aware()
        return await resolve_postgres_qname(self.config.relation, tenant_id)

    # ....................... #

    async def begin(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
    ) -> IdempotencyRecord | None:
        if not key:
            return None

        table = await self._table()

        # Claim a fresh key or re-claim an expired one (``WHERE ... expires_at <= now()``),
        # and read back the outcome in a single statement so claim-and-read share one
        # snapshot (no window for a concurrent commit/fail to slip between them, unlike a
        # separate INSERT then SELECT). The data-modifying CTE returns a row iff we now own
        # a pending claim (fresh insert or expired reclaim); otherwise a live row exists and
        # the ``UNION ALL`` branch reads it. Out of transaction -> auto-committed, so a
        # pending claim is immediately visible to a concurrent duplicate.
        stmt = sql.SQL(
            """
            WITH ins AS (
                INSERT INTO {table} (op, idem_key, payload_hash, status, result, expires_at)
                VALUES ({op}, {key}, {hash}, 'pending', NULL, now() + {ttl})
                ON CONFLICT (op, idem_key) DO UPDATE
                  SET payload_hash = EXCLUDED.payload_hash,
                      status = 'pending',
                      result = NULL,
                      expires_at = EXCLUDED.expires_at
                  WHERE {table}.expires_at <= now()
                RETURNING status, payload_hash, result
            )
            SELECT status, payload_hash, result, true AS claimed FROM ins
            UNION ALL
            SELECT status, payload_hash, result, false AS claimed
            FROM {table}
            WHERE op = {op} AND idem_key = {key} AND expires_at > now()
              AND NOT EXISTS (SELECT 1 FROM ins)
            """
        ).format(
            table=table.ident(),
            op=sql.Placeholder("op"),
            key=sql.Placeholder("key"),
            hash=sql.Placeholder("hash"),
            ttl=sql.Placeholder("ttl"),
        )

        row = await self.client.fetch_one(
            stmt,
            {"op": op, "key": key, "hash": payload_hash, "ttl": self.spec.ttl},
            row_factory="tuple",
        )

        if row is None:
            # No claim and no live row (a concurrent fail/expiry emptied it): in-progress.
            raise exc.conflict("Idempotency is in progress")

        status, existing_hash, result, claimed = row

        if claimed:
            return None  # fresh or reclaimed pending claim

        if existing_hash != payload_hash:
            raise exc.conflict("Payload hash mismatch")

        if status != "done" or result is None:
            raise exc.conflict("Idempotency is in progress")

        return IdempotencyRecord(result=bytes(result))

    # ....................... #

    async def commit(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
        record: IdempotencyRecord,
    ) -> None:
        if not key:
            return

        table = await self._table()

        # Runs inside the business transaction (the on_success hook) -> the record commits
        # atomically with the business writes; a rollback reverts it.
        stmt = sql.SQL(
            "UPDATE {table} SET status = 'done', result = {result}, expires_at = now() + {ttl} "
            "WHERE op = {op} AND idem_key = {key} AND payload_hash = {hash} "
            "AND status = 'pending'"
        ).format(
            table=table.ident(),
            result=sql.Placeholder(),
            ttl=sql.Placeholder(),
            op=sql.Placeholder(),
            key=sql.Placeholder(),
            hash=sql.Placeholder(),
        )

        rowcount = await self.client.execute(
            stmt,
            [record.result, self.spec.ttl, op, key, payload_hash],
            return_rowcount=True,
        )

        if rowcount == 0:
            # No matching pending claim: fail closed so the business transaction rolls back
            # rather than committing an effect with no idempotency record.
            raise exc.conflict("Idempotency commit failed (missing or non-pending claim)")

    # ....................... #

    async def fail(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
    ) -> None:
        if not key:
            return

        table = await self._table()

        # Only release our own pending claim: a completed record or a claim for a different
        # payload hash is left untouched.
        stmt = sql.SQL(
            "DELETE FROM {table} WHERE op = {op} AND idem_key = {key} "
            "AND payload_hash = {hash} AND status = 'pending'"
        ).format(
            table=table.ident(),
            op=sql.Placeholder(),
            key=sql.Placeholder(),
            hash=sql.Placeholder(),
        )

        await self.client.execute(stmt, [op, key, payload_hash])
