"""Postgres transactional outbox store."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any, final
from uuid import UUID

import attrs
from psycopg import sql
from psycopg.types.json import Jsonb
from pydantic import BaseModel

from forze.application.contracts.outbox import (
    OutboxAdminPort,
    OutboxClaim,
    OutboxDepth,
    OutboxQueryPort,
    OutboxSpec,
    OutboxStatus,
    StagedOutboxEntry,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.exceptions import exc
from forze.base.primitives import HlcTimestamp, utcnow, uuid7
from forze_postgres.execution.deps.configs.outbox import PostgresOutboxConfig
from forze_postgres.kernel.client import PostgresClientPort
from forze_postgres.kernel.gateways.base import PostgresQualifiedName
from forze_postgres.kernel.relation import resolve_postgres_qname

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresOutboxStore[M: BaseModel](TenancyMixin, OutboxQueryPort, OutboxAdminPort):
    """Postgres-backed outbox persistence, query, and admin port.

    **Delivery model — at-least-once.** :meth:`claim_pending` moves rows to ``processing``
    under ``FOR UPDATE SKIP LOCKED`` and there is no fence token: a relay that is merely
    slow can have its rows reclaimed by :meth:`reclaim_stale_processing` and both may
    publish. This is intentional — the consumer's inbox dedup makes the *effect*
    exactly-once, so a duplicate publish is tolerated rather than fenced.

    **Ordering.** ``ordering_key`` is *not* guaranteed FIFO under concurrent relays:
    ``SKIP LOCKED`` hands disjoint row sets to different relays, so two messages sharing an
    ``ordering_key`` can publish out of order. Per-key order is preserved only with a single
    relay (or a deployment that pins each ``ordering_key`` to one relay, e.g. partition
    affinity). Within one relay's claim, rows are emitted in ``hlc``/``created_at`` order.

    **Recommended index.** Back :meth:`claim_pending` / :meth:`reclaim_stale_processing`
    (``WHERE outbox_route = … AND status = 'pending' AND (available_at IS NULL OR
    available_at <= now()) [AND tenant_id = …] ORDER BY [hlc NULLS LAST,] created_at`` under
    ``FOR UPDATE SKIP LOCKED``) — pick the shape matching the route's config::

        -- base
        CREATE INDEX ON <relation> (outbox_route, status, created_at);
        -- with hlc_ordering enabled (the ORDER BY leads with hlc)
        CREATE INDEX ON <relation> (outbox_route, status, hlc, created_at);
        -- on a shared tagged table (the claim filters tenant_id) — lead with it
        CREATE INDEX ON <relation> (tenant_id, outbox_route, status, created_at);
    """

    client: PostgresClientPort
    spec: OutboxSpec[M]
    config: PostgresOutboxConfig

    # ....................... #

    async def _table(self) -> PostgresQualifiedName:
        tenant_id = self.require_tenant_if_aware()
        return await resolve_postgres_qname(self.config.relation, tenant_id)

    # ....................... #

    async def persist_rows(self, rows: Sequence[StagedOutboxEntry]) -> int:
        if not rows:
            return 0

        if len(rows) > self.config.max_flush_rows:
            raise exc.internal(
                f"Outbox flush exceeds max_flush_rows ({self.config.max_flush_rows})."
            )

        table = await self._table()
        created_at = utcnow()
        hlc_ordering = self.config.hlc_ordering
        propagate_trace = self.config.propagate_trace
        cols = (
            "id",
            "outbox_route",
            "event_id",
            "event_type",
            "tenant_id",
            "execution_id",
            "correlation_id",
            "causation_id",
            "occurred_at",
            "payload",
            "status",
            "created_at",
            "attempts",
            "available_at",
            "ordering_key",
            *(("hlc",) if hlc_ordering else ()),
            *(("traceparent",) if propagate_trace else ()),
        )
        col_idents = [sql.Identifier(c) for c in cols]
        row_template = (
            sql.SQL("(") + sql.SQL(", ").join(sql.Placeholder() for _ in cols) + sql.SQL(")")
        )
        value_parts = [row_template] * len(rows)
        flat_params: list[Any] = []

        for entry in rows:
            event = entry.event
            flat_params.extend(
                [
                    uuid7(),
                    entry.outbox_route,
                    event.event_id,
                    event.event_type,
                    event.tenant_id,
                    event.execution_id,
                    event.correlation_id,
                    event.causation_id,
                    event.occurred_at,
                    Jsonb(entry.payload_json),
                    OutboxStatus.PENDING.value,
                    created_at,
                    0,
                    None,
                    event.ordering_key,
                    *(
                        (event.hlc.pack() if event.hlc is not None else None,)
                        if hlc_ordering
                        else ()
                    ),
                    *((event.traceparent,) if propagate_trace else ()),
                ]
            )

        stmt = sql.SQL(
            """
            INSERT INTO {table} ({cols})
            VALUES {vals}
            ON CONFLICT (outbox_route, event_id) DO NOTHING
            """
        ).format(
            table=table.ident(),
            cols=sql.SQL(", ").join(col_idents),
            vals=sql.SQL(", ").join(value_parts),
        )

        rowcount = await self.client.execute(stmt, flat_params, return_rowcount=True)
        return int(rowcount or 0)

    # ....................... #

    async def claim_pending(
        self,
        *,
        limit: int | None = None,
    ) -> Sequence[OutboxClaim]:
        table = await self._table()
        max_n = limit if limit is not None else self.config.max_claim_rows
        route = str(self.spec.name)
        tenant_id = self.require_tenant_if_aware()

        tenant_filter = sql.SQL("")
        now = utcnow()
        params: dict[str, Any] = {
            "route": route,
            "limit": max_n,
            "pending": OutboxStatus.PENDING.value,
            "processing": OutboxStatus.PROCESSING.value,
            "processing_at": now,
            "now": now,
        }

        if tenant_id is not None:
            tenant_filter = sql.SQL("AND tenant_id = %(tenant_id)s")
            params["tenant_id"] = tenant_id

        # HLC ordering: claim in causal order, with the time-ordered uuid7 ``id``
        # as a deterministic tiebreaker; legacy null-hlc rows fall back to
        # ``created_at``. Off keeps the size-only created_at order, byte for byte.
        hlc_ordering = self.config.hlc_ordering
        propagate_trace = self.config.propagate_trace
        order_by = sql.SQL("hlc NULLS LAST, created_at, id" if hlc_ordering else "created_at")
        hlc_returning = sql.SQL(", t.hlc") if hlc_ordering else sql.SQL("")
        trace_returning = sql.SQL(", t.traceparent") if propagate_trace else sql.SQL("")

        stmt = sql.SQL(
            """
            WITH picked AS (
                SELECT id
                FROM {table}
                WHERE outbox_route = %(route)s
                  AND status = %(pending)s
                  AND (available_at IS NULL OR available_at <= %(now)s)
                  {tenant_filter}
                ORDER BY {order_by}
                LIMIT %(limit)s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE {table} AS t
            SET status = %(processing)s,
                processing_at = %(processing_at)s
            FROM picked
            WHERE t.id = picked.id
            RETURNING
                t.id, t.outbox_route, t.event_id, t.event_type, t.payload,
                t.tenant_id, t.execution_id, t.correlation_id, t.causation_id,
                t.occurred_at, t.attempts, t.ordering_key, t.created_at{hlc_returning}{trace_returning}
            """
        ).format(
            table=table.ident(),
            tenant_filter=tenant_filter,
            order_by=order_by,
            hlc_returning=hlc_returning,
            trace_returning=trace_returning,
        )

        rows = await self.client.fetch_all(stmt, params)

        # ``UPDATE … RETURNING`` does not preserve the picked CTE's ``ORDER BY``
        # (Postgres decouples the two), so re-apply the claim order in Python.
        if hlc_ordering:
            rows = sorted(
                rows,
                key=lambda r: (
                    r.get("hlc") is None,  # NULLS LAST
                    r.get("hlc") or 0,
                    r["created_at"],
                    r["id"],
                ),
            )

        else:
            rows = sorted(rows, key=lambda r: (r["created_at"], r["id"]))

        return [
            OutboxClaim(
                id=row["id"],
                outbox_route=row["outbox_route"],
                event_id=row["event_id"],
                event_type=row["event_type"],
                payload=dict(row["payload"]),
                tenant_id=row.get("tenant_id"),
                execution_id=row.get("execution_id"),
                correlation_id=row.get("correlation_id"),
                causation_id=row.get("causation_id"),
                occurred_at=row.get("occurred_at"),
                attempts=int(row.get("attempts") or 0),
                ordering_key=row.get("ordering_key"),
                hlc=(
                    HlcTimestamp.unpack(row["hlc"])
                    if hlc_ordering and row.get("hlc") is not None
                    else None
                ),
                traceparent=row.get("traceparent") if propagate_trace else None,
            )
            for row in rows
        ]

    async def mark_published(self, ids: Sequence[UUID]) -> int:
        return await self._mark(ids, OutboxStatus.PUBLISHED)

    async def mark_failed(
        self,
        ids: Sequence[UUID],
        *,
        error: str | None = None,
    ) -> int:
        return await self._mark(ids, OutboxStatus.FAILED, error=error)

    async def _mark(
        self,
        ids: Sequence[UUID],
        status: OutboxStatus,
        *,
        error: str | None = None,
    ) -> int:
        if not ids:
            return 0

        table = await self._table()
        now = utcnow()
        params: dict[str, Any] = {
            "ids": list(ids),
            "status": status.value,
            "processing": OutboxStatus.PROCESSING.value,
            "published_at": now if status == OutboxStatus.PUBLISHED else None,
            "last_error": error,
        }

        stmt = sql.SQL(
            """
            UPDATE {table}
            SET status = %(status)s,
                published_at = COALESCE(%(published_at)s, published_at),
                last_error = %(last_error)s
            WHERE id = ANY(%(ids)s::uuid[])
              AND status = %(processing)s
            """
        ).format(table=table.ident())

        rowcount = await self.client.execute(stmt, params, return_rowcount=True)
        return int(rowcount or 0)

    async def mark_retry(
        self,
        ids: Sequence[UUID],
        *,
        attempts: int,
        available_at: datetime,
        error: str | None = None,
    ) -> int:
        if not ids:
            return 0

        table = await self._table()
        params: dict[str, Any] = {
            "ids": list(ids),
            "pending": OutboxStatus.PENDING.value,
            "processing": OutboxStatus.PROCESSING.value,
            "attempts": attempts,
            "available_at": available_at,
            "last_error": error,
        }

        stmt = sql.SQL(
            """
            UPDATE {table}
            SET status = %(pending)s,
                processing_at = NULL,
                attempts = %(attempts)s,
                available_at = %(available_at)s,
                last_error = %(last_error)s
            WHERE id = ANY(%(ids)s::uuid[])
              AND status = %(processing)s
            """
        ).format(table=table.ident())

        rowcount = await self.client.execute(stmt, params, return_rowcount=True)
        return int(rowcount or 0)

    async def reclaim_stale_processing(
        self,
        *,
        older_than: datetime,
    ) -> int:
        table = await self._table()
        route = str(self.spec.name)
        tenant_id = self.require_tenant_if_aware()

        tenant_filter = sql.SQL("")
        params: dict[str, Any] = {
            "route": route,
            "older_than": older_than,
            "pending": OutboxStatus.PENDING.value,
            "processing": OutboxStatus.PROCESSING.value,
        }

        if tenant_id is not None:
            tenant_filter = sql.SQL("AND tenant_id = %(tenant_id)s")
            params["tenant_id"] = tenant_id

        stmt = sql.SQL(
            """
            UPDATE {table}
            SET status = %(pending)s,
                processing_at = NULL
            WHERE outbox_route = %(route)s
              AND status = %(processing)s
              AND processing_at IS NOT NULL
              AND processing_at < %(older_than)s
              {tenant_filter}
            """
        ).format(table=table.ident(), tenant_filter=tenant_filter)

        rowcount = await self.client.execute(stmt, params, return_rowcount=True)
        return int(rowcount or 0)

    async def requeue_failed(self, ids: Sequence[UUID]) -> int:
        if not ids:
            return 0

        table = await self._table()
        params: dict[str, Any] = {
            "ids": list(ids),
            "pending": OutboxStatus.PENDING.value,
            "failed": OutboxStatus.FAILED.value,
        }

        stmt = sql.SQL(
            """
            UPDATE {table}
            SET status = %(pending)s,
                processing_at = NULL,
                published_at = NULL,
                last_error = NULL,
                attempts = 0,
                available_at = NULL
            WHERE id = ANY(%(ids)s::uuid[])
              AND status = %(failed)s
            """
        ).format(table=table.ident())

        rowcount = await self.client.execute(stmt, params, return_rowcount=True)
        return int(rowcount or 0)

    # ....................... #
    # Admin (observability) port

    def _admin_scope(self) -> tuple[sql.SQL, dict[str, Any]]:
        """Route + tenant predicate shared by every admin probe."""

        params: dict[str, Any] = {
            "route": str(self.spec.name),
            "pending": OutboxStatus.PENDING.value,
            "processing": OutboxStatus.PROCESSING.value,
            "failed": OutboxStatus.FAILED.value,
        }
        tenant_id = self.require_tenant_if_aware()
        tenant_filter = sql.SQL("")

        if tenant_id is not None:
            tenant_filter = sql.SQL("AND tenant_id = %(tenant_id)s")
            params["tenant_id"] = tenant_id

        return tenant_filter, params

    # ....................... #

    async def has_undrained(self) -> bool:
        table = await self._table()
        tenant_filter, params = self._admin_scope()

        # EXISTS, not count(*): one index seek and stop at the first hit, so a quiesce loop
        # can poll this without paying for the table's published history. Statuses are an
        # IN-list rather than `status <> 'published'` — an inequality cannot seek a btree,
        # so the negated form would scan every row ever emitted.
        stmt = sql.SQL(
            """
            SELECT EXISTS (
                SELECT 1
                FROM {table}
                WHERE outbox_route = %(route)s
                  AND status IN (%(pending)s, %(processing)s)
                  {tenant_filter}
            )
            """
        ).format(table=table.ident(), tenant_filter=tenant_filter)

        return bool(await self.client.fetch_value(stmt, params, default=False))

    # ....................... #

    async def depth(self) -> OutboxDepth:
        table = await self._table()
        tenant_filter, params = self._admin_scope()

        # Undrained buckets only. `published` is excluded deliberately: nothing prunes it, so
        # it grows with every event ever emitted and counting it would scan the whole table.
        stmt = sql.SQL(
            """
            SELECT status, count(*) AS n
            FROM {table}
            WHERE outbox_route = %(route)s
              AND status IN (%(pending)s, %(processing)s, %(failed)s)
              {tenant_filter}
            GROUP BY status
            """
        ).format(table=table.ident(), tenant_filter=tenant_filter)

        rows = await self.client.fetch_all(stmt, params)
        counts = {str(row["status"]): int(row["n"]) for row in rows}

        return OutboxDepth(
            pending=counts.get(OutboxStatus.PENDING.value, 0),
            processing=counts.get(OutboxStatus.PROCESSING.value, 0),
            failed=counts.get(OutboxStatus.FAILED.value, 0),
        )

    # ....................... #

    async def oldest_pending_age(self) -> timedelta | None:
        table = await self._table()
        tenant_filter, params = self._admin_scope()

        # Keyed on created_at, which every route has — `hlc` exists only under
        # `hlc_ordering`, so ordering by it would make this probe config-dependent.
        stmt = sql.SQL(
            """
            SELECT min(created_at) AS oldest
            FROM {table}
            WHERE outbox_route = %(route)s
              AND status = %(pending)s
              {tenant_filter}
            """
        ).format(table=table.ident(), tenant_filter=tenant_filter)

        oldest = await self.client.fetch_value(stmt, params)

        return None if oldest is None else utcnow() - oldest
