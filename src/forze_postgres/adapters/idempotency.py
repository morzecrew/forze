"""Postgres co-located idempotency store — atomic in-transaction result commit."""

from __future__ import annotations

from typing import Any, Final, final

import attrs
from psycopg import sql

from forze.application.contracts.idempotency import (
    ClaimOwnerMixin,
    IdempotencyPort,
    IdempotencyRecord,
    IdempotencySpec,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.exceptions import exc
from forze.base.primitives import CacheLane
from forze_postgres.execution.deps.configs.idempotency import PostgresIdempotencyConfig
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client import PostgresClientPort
from forze_postgres.kernel.gateways.base import PostgresQualifiedName
from forze_postgres.kernel.relation import resolve_postgres_qname

from ._logger import logger

# ----------------------- #

_OWNER_COLUMN: Final[str] = "owner"

_RECHECK_AFTER: Final[float] = 60.0
"""Seconds before a relation reported without the ``owner`` column is probed again.

The column can be added to a running deployment, so an absent one is a fact with a
shelf life. Long enough that an un-migrated table costs one catalog row a minute,
short enough that "run the migration" does not silently mean "and restart".
"""

_MAX_TRACKED_RELATIONS: Final[int] = 2048
"""Cap on both lanes below.

A relation name is not a bounded quantity: table-per-tenant and database-per-tenant
deployments mint one key per tenant, so an unbounded dict here would grow with the tenant
list for the life of the process. Evicting the oldest costs one extra probe or one repeated
warning, which is the cheapest possible consequence.
"""

_ProbeKey = tuple[str, str]
"""``(cache partition, qualified relation)`` — the introspector's own cache identity.

Keyed by the partition too, because the answer being cached is per *database*: with
database-per-tenant routing a relation name alone would let one tenant's probe stand in for
another's, and a tenant that had just migrated would keep running unfenced until a cooldown
it never started elapsed.
"""

_PROBE_COOLDOWN: Final[CacheLane[_ProbeKey, bool]] = CacheLane(
    max_entries=_MAX_TRACKED_RELATIONS,
    ttl_seconds=_RECHECK_AFTER,
)
"""Relations probed recently and still without the column. Expiry *is* the cooldown."""

_REPORTED_UNFENCED: Final[CacheLane[_ProbeKey, bool]] = CacheLane(
    max_entries=_MAX_TRACKED_RELATIONS,
)
"""Relations already reported as lacking the ``owner`` column.

Process-wide so the operator hears about an un-migrated table once rather than on every
operation, and capped for the same reason the cooldown is.
"""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresIdempotencyStore(TenancyMixin, ClaimOwnerMixin, IdempotencyPort):
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
            owner        uuid,                    -- optional; see below
            PRIMARY KEY (op, idem_key)
        );

    ``owner`` is **optional and additive**. With it, :meth:`commit` and :meth:`fail` refuse
    a claim another invocation reclaimed — the failure two duplicates of one request can
    otherwise produce, since their ``op``, key and payload hash are identical. Without it
    the store keeps its previous behaviour, which is what lets a deployment upgrade before
    it migrates: the column is detected through the catalog, because a statement naming a
    column the table lacks does not execute at all, so no conditional predicate could
    rescue it. A relation running unfenced is logged once per process rather than left
    silent, and re-probed on a cooldown, so applying the migration to a running deployment
    turns the fence on without a restart (see :meth:`_has_owner_column`).

    ``ALTER TABLE <relation> ADD COLUMN owner uuid;`` is the whole migration — nullable, so
    existing rows need no backfill: they expire inside the dedup window, and a row with a
    NULL owner is still completable by design.
    """

    client: PostgresClientPort
    spec: IdempotencySpec
    config: PostgresIdempotencyConfig

    introspector: PostgresIntrospector | None = None
    """Catalog access used to detect the optional ``owner`` column.

    ``None`` — a direct construction rather than the wired factory — means no detection is
    possible, so the store keeps the legacy statements and no fencing. Wiring always
    supplies one; this default exists so tooling and tests can build the store without a
    dep registry, not as a supported production configuration.
    """

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

    async def _has_owner_column(self, table: PostgresQualifiedName) -> bool:
        """Whether *table* carries the optional ``owner`` column.

        Reads the catalog through the introspector, whose per-relation cache makes a
        *present* column free after the first query. An **absent** one is re-probed on a
        cooldown instead of being trusted for the life of the process: the migration is
        documented as something a running deployment may apply, and a cached "no such
        column" would keep every process it reached unfenced until a restart nobody was
        told to perform. The re-probe is one catalog row at most once per
        :data:`_RECHECK_AFTER`, and the introspector's entry is invalidated exactly once —
        when the column appears — rather than on a timer, so the caches it shares with the
        document and search planes are not swept on every check.

        The answer selects between two statement shapes for this call: a column the table
        does not have cannot be named in SQL at all, which is why detection exists instead
        of a predicate that tolerates its absence.
        """

        if self.introspector is None:
            return False

        columns = await self.introspector.get_column_types(
            schema=table.schema,
            relation=table.name,
        )

        if _OWNER_COLUMN in columns:
            return True

        key = (self._cache_partition(), table.string())

        if not self._recheck_due(key):
            return False

        if not await self._probe_owner_column(table):
            self._report_unfenced(key)
            return False

        # The column arrived under a running process: drop the stale entry so every later
        # call takes the cached fast path again, drop the cooldown this relation no longer
        # needs, and let this one through fenced.
        self.introspector.invalidate_relation(schema=table.schema, relation=table.name)
        _PROBE_COOLDOWN.invalidate(key)

        return True

    # ....................... #

    def _cache_partition(self) -> str:
        """The routing identity the introspector caches under, or ``""`` when unrouted.

        Read from the introspector rather than tracked separately, so the cooldown and the
        cache it defers to can never disagree about which database an answer belongs to.
        Reached only after :meth:`_has_owner_column` has already queried through the same
        introspector, which refuses a partition callable that yields nothing — so the
        ``or ""`` is a type narrowing, not a silent merge of two tenants into one key.
        """

        if self.introspector is None or self.introspector.cache_partition_key is None:
            return ""

        return self.introspector.cache_partition_key() or ""

    # ....................... #

    @staticmethod
    def _recheck_due(key: _ProbeKey) -> bool:
        """Whether *key* is due another probe for a late-arriving ``owner`` column.

        The lane's TTL is the cooldown: an entry present means one ran recently, and its
        expiry is what makes the next one due. Its cap means a deployment with more
        relations than the cap re-probes some of them sooner, which costs a catalog row.
        """

        if _PROBE_COOLDOWN.lookup(key) is not None:
            return False

        _PROBE_COOLDOWN.store(key, True)

        return True

    # ....................... #

    async def _probe_owner_column(self, table: PostgresQualifiedName) -> bool:
        """Ask the catalog directly whether the column exists now, bypassing every cache.

        Matches schema, relation and column by exact name — the introspector's own shape —
        rather than parsing an identifier through ``to_regclass``, which would lowercase a
        mixed-case relation and answer about a table that does not exist.
        """

        stmt = sql.SQL(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = {schema}
                  AND c.relname = {relation}
                  AND a.attname = {column}
                  AND a.attnum > 0
                  AND NOT a.attisdropped
            )
            """
        ).format(
            schema=sql.Placeholder(),
            relation=sql.Placeholder(),
            column=sql.Placeholder(),
        )

        found = await self.client.fetch_value(
            stmt,
            [table.schema, table.name, _OWNER_COLUMN],
            default=False,
        )

        return bool(found)

    # ....................... #

    @staticmethod
    def _report_unfenced(key: _ProbeKey) -> None:
        """Say once per relation that it is running without the fence, and how to fix it."""

        if _REPORTED_UNFENCED.lookup(key) is not None:
            return

        _REPORTED_UNFENCED.store(key, True)
        qualified = key[1]
        logger.warning(
            "Idempotency table %s has no 'owner' column: a claim reclaimed by a "
            "duplicate can still be committed by the operation it displaced. "
            "ALTER TABLE %s ADD COLUMN owner uuid;",
            qualified,
            qualified,
        )

    # ....................... #

    def _owner_insert(
        self,
        has_column: bool,
    ) -> tuple[sql.Composable, sql.Composable, sql.Composable]:
        """The column, value and conflict-update fragments that record the claim's owner.

        Empty on a table without the column. The value is bound even when it is ``NULL``,
        unlike the predicate in :meth:`_owner_predicate`: writing NULL states "this claim
        has no owner", which is exactly what a reclaim must overwrite, whereas *reading*
        NULL by equality matches nothing.
        """

        if not has_column:
            return sql.SQL(""), sql.SQL(""), sql.SQL("")

        column = sql.Identifier(_OWNER_COLUMN)

        return (
            sql.SQL(", {column}").format(column=column),
            sql.SQL(", {owner}").format(owner=sql.Placeholder("owner")),
            sql.SQL(", {column} = EXCLUDED.{column}").format(column=column),
        )

    # ....................... #

    def _owner_predicate(self, has_column: bool) -> tuple[sql.Composable, list[Any]]:
        """The ownership fence for :meth:`commit` / :meth:`fail`, and its parameters.

        Omitted — not bound to ``NULL`` — when there is no owner to compare: ``owner =
        NULL`` is ``UNKNOWN`` in SQL, so binding it would match no owned row at all while
        looking like a working predicate, which is the opposite of degrading to the
        previous behaviour. The ``IS NULL`` arm is what keeps rows written before the
        column (or by a store with no provider) completable by their own caller.
        """

        owner = self.claim_owner() if has_column else None

        if owner is None:
            return sql.SQL(""), []

        return sql.SQL("AND ({column} IS NULL OR {column} = {owner})").format(
            column=sql.Identifier(_OWNER_COLUMN),
            owner=sql.Placeholder(),
        ), [owner]

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
        owner_column, owner_value, owner_update = self._owner_insert(
            await self._has_owner_column(table)
        )

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
                INSERT INTO {table} (
                    op, idem_key, payload_hash, status, result, expires_at{owner_column}
                )
                VALUES ({op}, {key}, {hash}, 'pending', NULL, now() + {ttl}{owner_value})
                ON CONFLICT (op, idem_key) DO UPDATE
                  SET payload_hash = EXCLUDED.payload_hash,
                      status = 'pending',
                      result = NULL,
                      expires_at = EXCLUDED.expires_at{owner_update}
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
            owner_column=owner_column,
            owner_value=owner_value,
            owner_update=owner_update,
        )

        row = await self.client.fetch_one(
            stmt,
            {
                "op": op,
                "key": key,
                "hash": payload_hash,
                "ttl": self.spec.ttl,
                # Ignored by the legacy statement, which names no owner placeholder.
                "owner": self.claim_owner(),
            },
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
        owner_sql, owner_params = self._owner_predicate(await self._has_owner_column(table))

        # Runs inside the business transaction (the on_success hook) -> the record commits
        # atomically with the business writes; a rollback reverts it.
        stmt = sql.SQL(
            "UPDATE {table} SET status = 'done', result = {result}, expires_at = now() + {ttl} "
            "WHERE op = {op} AND idem_key = {key} AND payload_hash = {hash} "
            "AND status = 'pending' {owner}"
        ).format(
            table=table.ident(),
            result=sql.Placeholder(),
            ttl=sql.Placeholder(),
            op=sql.Placeholder(),
            key=sql.Placeholder(),
            hash=sql.Placeholder(),
            owner=owner_sql,
        )

        rowcount = await self.client.execute(
            stmt,
            [record.result, self.spec.ttl, op, key, payload_hash, *owner_params],
            return_rowcount=True,
        )

        if rowcount == 0:
            # No matching pending claim of our own: fail closed so the business transaction
            # rolls back rather than committing an effect with no idempotency record — or,
            # where a duplicate reclaimed the key, rather than replacing that operation's
            # live claim with this one's result.
            raise exc.conflict(
                "Idempotency commit failed (claim missing, non-pending, or reclaimed)"
            )

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
        owner_sql, owner_params = self._owner_predicate(await self._has_owner_column(table))

        # Only release our own pending claim: a completed record, a claim for a different
        # payload hash, or one another invocation reclaimed is left untouched.
        stmt = sql.SQL(
            "DELETE FROM {table} WHERE op = {op} AND idem_key = {key} "
            "AND payload_hash = {hash} AND status = 'pending' {owner}"
        ).format(
            table=table.ident(),
            op=sql.Placeholder(),
            key=sql.Placeholder(),
            hash=sql.Placeholder(),
            owner=owner_sql,
        )

        await self.client.execute(stmt, [op, key, payload_hash, *owner_params])
