"""Postgres durable-run store (run instances + lease-based crash recovery claims)."""

from __future__ import annotations

from collections.abc import Collection, Sequence
from datetime import datetime, timedelta
from typing import Any, final
from uuid import UUID

import attrs
from psycopg import sql
from psycopg.types.json import Jsonb

from forze.application.contracts.crypto import BytesCipherPort
from forze.application.contracts.durable.function import (
    PURGEABLE_STATUSES,
    DurableLeaseRenewal,
    DurableRunAdminPort,
    DurableRunControlAware,
    DurableRunControlCapabilities,
    DurableRunPage,
    DurableRunRecord,
    DurableRunStatus,
    DurableRunStorePort,
    build_run_page,
    check_enqueue_keys,
    check_purge,
    decode_run_cursor,
    requeue_failed_unnamed_error,
    requeue_unscoped_error,
    scope_tenant_key,
    unscope_tenant_key,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.application.integrations.crypto.payload import (
    decrypt_payload,
    encrypt_payload,
    is_payload_cipher_missing,
)
from forze.application.integrations.durable import (
    is_passing_fault,
    quarantines_unopenable,
    unopenable_hold,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, utcnow, uuid7
from forze_postgres.adapters._logger import logger
from forze_postgres.adapters.durable.function_step import DURABLE_PAYLOAD_DOMAIN
from forze_postgres.execution.deps.configs.durable import (
    PostgresDurableRunConfig,
    PostgresDurableStepConfig,
)
from forze_postgres.kernel.client import PostgresClientPort
from forze_postgres.kernel.gateways.base import PostgresQualifiedName
from forze_postgres.kernel.relation import resolve_postgres_qname

# ----------------------- #

_COLUMNS = (
    "run_id, name, status, idempotency_key, input, output, error, tenant_id, "
    "attempts, available_at, created_at, cancel_requested_at, cancel_refused_at"
)
"""Row projection for plain single-table SELECTs."""

_T_COLUMNS = (
    "t.run_id, t.name, t.status, t.idempotency_key, t.input, t.output, "
    "t.error, t.tenant_id, t.attempts, t.available_at, t.created_at, "
    "t.cancel_requested_at, t.cancel_refused_at"
)
"""Row projection for RETURNING out of an ``UPDATE ... FROM picked`` join (qualified to
resolve the ``run_id`` shared with the ``picked`` CTE); column names stay unqualified."""


def _scope_idem(idempotency_key: str | None, tenant_id: UUID | None) -> str | None:
    """Namespace an idempotency key under its tenant for the stored ``idempotency_key``.

    A shared **tagged** table has one global ``UNIQUE (idempotency_key)``; the
    :func:`scope_tenant_key` form scopes convergence per tenant, so two tenants reusing one
    key (e.g. a scheduler's ``{schedule_id}:{fire_epoch}``) stay distinct runs, and an
    untagged key never meets a tenant's. A ``None`` key is never namespaced (no idempotency;
    ``NULL`` stays globally distinct).
    """
    return None if idempotency_key is None else scope_tenant_key(idempotency_key, tenant_id)


def _unscope_idem(stored: str | None, tenant_id: UUID | None) -> str | None:
    """Strip the prefix :func:`_scope_idem` added, so a record surfaces the key the caller
    passed."""
    return None if stored is None else unscope_tenant_key(stored, tenant_id)


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresDurableRunStore(
    TenancyMixin,
    DurableRunStorePort,
    DurableRunAdminPort,
    DurableRunControlAware,
):
    """Postgres-backed durable-run store.

    Records run instances and hands out claims for execution and crash recovery. A crashed
    run is left ``RUNNING`` with an expired lease; :meth:`claim_abandoned` re-claims it with
    ``FOR UPDATE SKIP LOCKED`` (single-leader-safe, concurrent-scanner-safe). Re-submits
    under one ``idempotency_key`` converge on a single run (``ON CONFLICT DO NOTHING``);
    the key is stored **tenant-scoped**, so on a shared tagged table two tenants reusing one
    key (e.g. a scheduler's ``{schedule_id}:{fire_epoch}``) stay distinct runs.

    **Tenancy.** The table is resolved under the bound tenant, so a static ``relation`` is a
    shared **tagged** table (``tenant_id`` column) and a per-tenant ``relation`` resolver is a
    **namespace** table. Recovery either runs unbound over a tagged table (claims every
    tenant's runs; the runner re-binds each run's tenant to execute it) or per-tenant over a
    namespace table (the scanner binds each tenant in turn). An unbound scan never fails, and
    a bound scan claims only that tenant's runs.

    What a bound caller reaches splits by verb. ``begin``, ``renew``, ``load``, ``mark_retry``
    and every terminal write reach that tenant's runs and untagged ones (see
    :meth:`_worker_scope`); ``claim_abandoned``, ``list_runs``, ``request_cancel``,
    ``requeue_quarantined``, ``requeue_failed``, ``purge_finished`` and ``refuse_cancel``
    match the tenant exactly. ``enqueue`` resolves one effective tenant for the table, the tag and the scoped
    key together, and refuses an explicit tenant that contradicts the binding.

    The table is provided by the application, with its migrations in
    ``pages/docs/reference/durable-schema.md``; expected schema::

        CREATE TABLE <relation> (
            run_id              text        NOT NULL,
            name                text        NOT NULL,
            status              text        NOT NULL,
            idempotency_key     text,
            coalesce_key        text,
            input               jsonb,
            output              jsonb,
            error               text,
            tenant_id           uuid,
            attempts            integer     NOT NULL DEFAULT 0,
            leased_until        timestamptz,
            available_at        timestamptz,
            created_at          timestamptz NOT NULL,
            updated_at          timestamptz NOT NULL,
            cancel_requested_at timestamptz,
            cancel_refused_at   timestamptz,
            PRIMARY KEY (run_id),
            UNIQUE (idempotency_key)
        );

        -- Needed by enqueue(coalesce_key=...): one unclaimed pending run per key. An
        -- existing table adds both before that call is used; nothing else reads the column:
        --   ALTER TABLE <relation> ADD COLUMN coalesce_key text;
        CREATE UNIQUE INDEX ON <relation> (coalesce_key) WHERE status = 'pending' AND attempts = 0;

        -- Both cancel columns are nullable with no default, so an existing deployment
        -- migrates with two plain ADD COLUMNs and no table rewrite:
        --   ALTER TABLE <relation> ADD COLUMN cancel_requested_at timestamptz,
        --                          ADD COLUMN cancel_refused_at   timestamptz;
        --
        -- RUN THE MIGRATION BEFORE DEPLOYING THIS CODE. `begin`, the recovery scan
        -- (`claim_abandoned`) and every read return both columns, so against the old schema
        -- each one raises `UndefinedColumn` and no body starts. A new `enqueue` still
        -- inserts, so its run waits `pending` until the migration lands and the recovery
        -- scan claims it; a re-submit of an existing key fails like a read.

        -- Recommended: back the recovery scan (claim_abandoned), which filters on
        -- status/available_at/leased_until and orders by created_at under
        -- FOR UPDATE SKIP LOCKED. Without it the claim seq-scans + sorts under lock as
        -- the table grows.
        CREATE INDEX ON <relation> (status, created_at);
        -- On a shared tagged table claim_abandoned also filters `tenant_id = …` for a bound
        -- tenant, so lead the index with tenant_id to skip other tenants' rows:
        CREATE INDEX ON <relation> (tenant_id, status, created_at);

        -- Recommended where runs are purged: purge_finished filters on status and
        -- updated_at (when the run landed) and deletes the oldest landings first.
        CREATE INDEX ON <relation> (status, updated_at);

        -- Recommended: back the admin listing (list_runs), which orders newest-first by
        -- (created_at DESC, run_id DESC) and keyset-seeks on (created_at, run_id). Without a
        -- matching index it seq-scans + sorts the whole relation. On a namespace (per-tenant)
        -- table the ordering columns suffice:
        CREATE INDEX ON <relation> (created_at DESC, run_id DESC);
        -- On a shared tagged table list_runs filters `tenant_id = …` for a bound tenant, so
        -- lead with tenant_id (an optional `status`/`name` filter is an extra residual
        -- predicate on top of this keyset order):
        CREATE INDEX ON <relation> (tenant_id, created_at DESC, run_id DESC);

    ``attempts`` doubles as the fence token (advances under a row lock on each claim);
    ``available_at`` delays when a ``PENDING`` run may be claimed. Concurrent scanners are
    safe (``FOR UPDATE SKIP LOCKED``) and a terminal write can be fenced against a
    reclaimed lease — so the store is multi-worker-safe, not just single-leader.
    """

    client: PostgresClientPort
    config: PostgresDurableRunConfig
    cipher: BytesCipherPort | None = None

    step_config: PostgresDurableStepConfig | None = None
    """The step journal :meth:`purge_finished` deletes a purged run's steps from, in the same
    statement. ``PostgresDepsModule`` passes its ``durable_step``."""

    journal: bool = True
    """Whether runs on this table journal steps. While ``True`` and no :attr:`step_config` is
    given, :meth:`purge_finished` refuses rather than leave each purged run's steps behind.
    ``PostgresDepsModule`` sets it from whether ``durable_step`` is wired."""

    # ....................... #

    async def _table(self, tenant_id: UUID | None = None) -> PostgresQualifiedName:
        """Resolve the relation, under *tenant_id* when the caller has already settled it.

        A verb that accepts an explicit tenant passes the effective one here so the row
        lands in the relation its tag says it belongs to; every other verb resolves from
        the binding, as before.
        """

        if tenant_id is None:
            tenant_id = self._tenant_id_for_resolve()

        return await resolve_postgres_qname(self.config.relation, tenant_id)

    # ....................... #

    def _worker_scope(self) -> tuple[sql.Composable, dict[str, Any]]:
        """Tenant predicate for the verbs reached with a run id: ``begin``, ``renew``,
        ``load`` and every terminal write.

        Bound, a caller reaches its own tenant's runs and the untagged ones; unbound — the
        recovery and single-tenant role — it reaches everything, which is what lets one
        sweep serve every tenant.

        The ``IS NULL`` arm is load-bearing rather than lax. A run tagged with no tenant
        belongs to none, and the runner leaves the ambient binding alone for such a run, so
        an exact match would make its terminal write hit nothing: the run would sit
        ``running`` until its lease expired, be reclaimed, and re-run forever. Tightening
        this re-opens that loop.
        """

        tenant_id = self._tenant_id_for_resolve()

        if tenant_id is None:
            return sql.SQL(""), {}

        return (
            sql.SQL(" AND (tenant_id = {scope} OR tenant_id IS NULL)").format(
                scope=sql.Placeholder("scope_tenant"),
            ),
            {"scope_tenant": tenant_id},
        )

    # ....................... #

    async def enqueue(
        self,
        name: str,
        *,
        input_json: JsonDict | None,
        idempotency_key: str | None = None,
        tenant_id: UUID | None = None,
        available_at: datetime | None = None,
        coalesce_key: str | None = None,
    ) -> DurableRunRecord:
        check_enqueue_keys(idempotency_key, coalesce_key)

        # One effective tenant for the row's column, its scoped idempotency key *and* the
        # relation it lands in — resolving the relation from the binding while tagging from
        # the argument is how a row ends up where nobody looks for it.
        tenant_id = self._effective_tenant(tenant_id)
        table = await self._table(tenant_id)
        run_id = str(uuid7())
        now = utcnow()
        stored_input = await self._seal(input_json, run_id, "input", tenant_id)
        stored_idem = _scope_idem(idempotency_key, tenant_id)

        key_column = sql.SQL("idempotency_key")
        conflict = sql.SQL("ON CONFLICT (idempotency_key) DO NOTHING")
        returning = sql.SQL("run_id")
        key = stored_idem

        if coalesce_key is not None:
            # The partial unique index admits one unclaimed pending run per key. DO UPDATE
            # (a no-op write) hands back that run under its row lock, atomically: if a claim
            # takes it first, the row leaves the index and this insert goes ahead instead.
            # Only this path names the column, so a table without it serves every other.
            key_column = sql.SQL("coalesce_key")
            conflict = sql.SQL(
                "ON CONFLICT (coalesce_key) WHERE status = 'pending' AND attempts = 0 "
                "DO UPDATE SET coalesce_key = EXCLUDED.coalesce_key"
            )
            returning = sql.SQL(_COLUMNS)
            key = scope_tenant_key(coalesce_key, tenant_id)

        row = await self.client.fetch_one(
            sql.SQL(
                """
                INSERT INTO {table}
                    (run_id, name, status, {key_column}, input, tenant_id,
                     attempts, leased_until, available_at, created_at, updated_at)
                VALUES
                    ({run_id}, {name}, 'pending', {key}, {input}, {tenant_id},
                     0, NULL, {available_at}, {now}, {now})
                {conflict}
                RETURNING {returning}
                """
            ).format(
                table=table.ident(),
                key_column=key_column,
                run_id=sql.Placeholder("run_id"),
                name=sql.Placeholder("name"),
                key=sql.Placeholder("key"),
                input=sql.Placeholder("input"),
                tenant_id=sql.Placeholder("tenant_id"),
                available_at=sql.Placeholder("available_at"),
                now=sql.Placeholder("now"),
                conflict=conflict,
                returning=returning,
            ),
            {
                "run_id": run_id,
                "name": name,
                "key": key,
                "input": None if stored_input is None else Jsonb(stored_input),
                "tenant_id": tenant_id,
                "available_at": available_at,
                "now": now,
            },
        )

        if row is not None and row["run_id"] != run_id:
            # Coalesced onto the run already waiting for this key.
            return await self._record_from_row(row)

        if row is not None:
            return DurableRunRecord(
                run_id=run_id,
                name=name,
                status=DurableRunStatus.PENDING,
                idempotency_key=idempotency_key,
                input_json=input_json,
                tenant_id=tenant_id,
                attempts=0,
                available_at=available_at,
                created_at=now,
            )

        # A run already exists for this idempotency key: converge on it. Look up by the
        # tenant-scoped stored key so convergence never crosses tenants (``_record_from_row``
        # unscopes it back to the caller's key).
        existing = await self._load_by_idempotency(table, stored_idem)

        if existing is None:  # pragma: no cover — the conflicting row must exist
            raise exc.internal(
                "Durable run enqueue conflicted on idempotency_key but the existing "
                "run could not be loaded.",
            )

        return existing

    # ....................... #

    async def begin(
        self,
        run_id: str,
        *,
        lease_for: timedelta,
    ) -> DurableRunRecord | None:
        table = await self._table()
        scope_clause, scope_params = self._worker_scope()

        row = await self.client.fetch_one(
            sql.SQL(
                """
                WITH picked AS (
                    SELECT run_id FROM {table}
                    WHERE run_id = {run_id} AND status = 'pending'{scope}
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE {table} AS t
                SET status = 'running',
                    attempts = t.attempts + 1,
                    leased_until = now() + {lease},
                    updated_at = now()
                FROM picked
                WHERE t.run_id = picked.run_id
                RETURNING {columns}
                """
            ).format(
                table=table.ident(),
                run_id=sql.Placeholder("run_id"),
                lease=sql.Placeholder("lease"),
                columns=sql.SQL(_T_COLUMNS),
                scope=scope_clause,
            ),
            {"run_id": run_id, "lease": lease_for, **scope_params},
        )

        if row is None:
            return None

        return await self._record_from_row(row)

    # ....................... #

    async def renew(
        self,
        run_id: str,
        *,
        lease_for: timedelta,
        fence: int,
    ) -> DurableLeaseRenewal:
        table = await self._table()
        scope_clause, scope_params = self._worker_scope()

        # Fenced on ``attempts = fence`` (and ``status = 'running'``), mirroring ``_finish``:
        # the lease is pushed forward only while this worker is still the current claim
        # holder. If a recovery scan reclaimed the run its ``attempts`` advanced, no row
        # matches, nothing is returned, and the caller learns it must stop.
        #
        # The cancel stamp rides back on the same RETURNING, so observing a cancel request
        # costs the heartbeat it already makes rather than a second polling loop.
        row = await self.client.fetch_one(
            sql.SQL(
                """
                UPDATE {table}
                SET leased_until = now() + {lease}, updated_at = now()
                WHERE run_id = {run_id} AND status = 'running' AND attempts = {fence}{scope}
                RETURNING (cancel_requested_at IS NOT NULL) AS cancel_requested
                """
            ).format(
                table=table.ident(),
                lease=sql.Placeholder("lease"),
                run_id=sql.Placeholder("run_id"),
                fence=sql.Placeholder("fence"),
                scope=scope_clause,
            ),
            {"lease": lease_for, "run_id": run_id, "fence": fence, **scope_params},
        )

        if row is None:
            return DurableLeaseRenewal(held=False)

        return DurableLeaseRenewal(held=True, cancel_requested=bool(row["cancel_requested"]))

    # ....................... #

    async def claim_abandoned(
        self,
        *,
        limit: int,
        lease_for: timedelta,
    ) -> Sequence[DurableRunRecord]:
        table = await self._table()
        params: dict[str, object] = {"limit": limit, "lease": lease_for}

        # Scope the scan to the bound tenant when one is bound (per-tenant recovery on a
        # tagged table); unbound, it recovers every tenant's runs (the runner re-binds each
        # run's tenant to execute it). On a namespace table the resolved table is already
        # per-tenant, so the filter is a redundant no-op.
        tenant_id = self._tenant_id_for_resolve()
        tenant_filter = sql.SQL("")

        if tenant_id is not None:
            tenant_filter = sql.SQL("AND tenant_id = %(tenant_id)s")
            params["tenant_id"] = tenant_id

        rows = await self.client.fetch_all(
            sql.SQL(
                """
                WITH picked AS (
                    SELECT run_id FROM {table}
                    WHERE (
                        (status = 'pending'
                         AND (available_at IS NULL OR available_at <= now()))
                        OR (status = 'running'
                            AND (leased_until IS NULL OR leased_until <= now()))
                    ) {tenant_filter}
                    ORDER BY created_at
                    LIMIT {limit}
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE {table} AS t
                SET status = 'running',
                    attempts = t.attempts + 1,
                    leased_until = now() + {lease},
                    updated_at = now()
                FROM picked
                WHERE t.run_id = picked.run_id
                RETURNING {columns}
                """
            ).format(
                tenant_filter=tenant_filter,
                table=table.ident(),
                limit=sql.Placeholder("limit"),
                lease=sql.Placeholder("lease"),
                columns=sql.SQL(_T_COLUMNS),
            ),
            params,
        )

        return await self._open_claimed(rows, lease_for)

    # ....................... #

    async def _open_claimed(
        self,
        rows: Sequence[Any],
        lease_for: timedelta,
    ) -> list[DurableRunRecord]:
        # One by one: the claim has already leased every row, so a run whose payload cannot
        # be opened must not fail the runs claimed with it. One whose key is gone is held
        # out of the scan for longer than a lease, or it would be back at the head of the
        # next one, and quarantined once that hold has doubled to its cap; one that met a
        # passing fault keeps its lease. A missing cipher is the deployment's, not the
        # run's, and fails the scan.
        records: list[DurableRunRecord] = []

        for row in rows:
            try:
                records.append(await self._record_from_row(row))

            except Exception as error:
                if is_payload_cipher_missing(error):
                    raise

                if quarantines_unopenable(error, lease_for=lease_for, attempts=row["attempts"]):
                    logger.exception(
                        "Durable run %s could not be opened on its claim %d; quarantined, "
                        "recovery will not claim it again until it is requeued",
                        row["run_id"],
                        row["attempts"],
                    )
                    await self._quarantine(row["run_id"], fence=row["attempts"])
                    continue

                hold = unopenable_hold(error, lease_for=lease_for, attempts=row["attempts"])
                logger.exception(
                    "Durable run %s was claimed for recovery but could not be opened; %s",
                    row["run_id"],
                    "retried once its lease lapses"
                    if hold is None
                    else f"held out of recovery for {hold}",
                )

                if hold is None:
                    continue

                await self.client.execute(
                    sql.SQL(
                        """
                        UPDATE {table}
                        SET leased_until = now() + {hold}, updated_at = now()
                        WHERE run_id = {run_id} AND status = 'running' AND attempts = {fence}
                        """
                    ).format(
                        table=(await self._table()).ident(),
                        hold=sql.Placeholder("hold"),
                        run_id=sql.Placeholder("run_id"),
                        fence=sql.Placeholder("fence"),
                    ),
                    {"hold": hold, "run_id": row["run_id"], "fence": row["attempts"]},
                )

        return records

    # ....................... #

    async def _quarantine(self, run_id: str, *, fence: int) -> None:
        # Fenced like the hold: only the claim that failed to open the run moves it, and
        # only while it is still running under that claim.
        await self.client.execute(
            sql.SQL(
                """
                UPDATE {table}
                SET status = 'quarantined', leased_until = NULL, updated_at = now()
                WHERE run_id = {run_id} AND status = 'running' AND attempts = {fence}
                """
            ).format(
                table=(await self._table()).ident(),
                run_id=sql.Placeholder("run_id"),
                fence=sql.Placeholder("fence"),
            ),
            {"run_id": run_id, "fence": fence},
        )

    # ....................... #

    async def requeue_quarantined(self, *, run_ids: Sequence[str] | None = None) -> int:
        table = await self._table()
        params: dict[str, object] = {}
        clauses: list[sql.Composable] = [sql.SQL("status = 'quarantined'")]

        # Scoped like ``list_runs``. A quarantined run has no lease and no holder, so there
        # is nothing to fence against. It is made due at once: ``begin`` takes a run before
        # its ``available_at``, so one can reach quarantine still delayed.
        tenant_id = self._tenant_id_for_resolve()

        if tenant_id is not None:
            clauses.append(sql.SQL("tenant_id = %(tenant_id)s"))
            params["tenant_id"] = tenant_id

        if run_ids is not None:
            clauses.append(sql.SQL("run_id = ANY(%(run_ids)s)"))
            params["run_ids"] = list(run_ids)

        elif tenant_id is None:
            # Unbound and unnamed, every tenant's runs would come back ahead of the fresh
            # ones, those whose key is gone for good among them. Refused while any is
            # tagged; the update then takes untagged runs only, so a run tagged and
            # quarantined after this check stays where it is.
            tagged = await self.client.fetch_one(
                sql.SQL(
                    "SELECT 1 FROM {table} "
                    "WHERE status = 'quarantined' AND tenant_id IS NOT NULL LIMIT 1"
                ).format(table=table.ident()),
                row_factory="tuple",
            )

            if tagged is not None:
                raise requeue_unscoped_error()

            clauses.append(sql.SQL("tenant_id IS NULL"))

        return await self._requeue(table, clauses, params)

    # ....................... #

    async def requeue_failed(
        self,
        *,
        run_ids: Sequence[str] | None = None,
        name: str | None = None,
    ) -> int:
        if run_ids is None and name is None:
            raise requeue_failed_unnamed_error()

        table = await self._table()
        params: dict[str, object] = {}
        clauses: list[sql.Composable] = [
            sql.SQL("status IN ('failed', 'timed_out')"),
            # An unanswered cancel ask is honoured: such a run stays where it landed.
            sql.SQL("(cancel_requested_at IS NULL OR cancel_refused_at IS NOT NULL)"),
        ]

        # Scoped like ``list_runs``; a landed run has no lease and no holder to fence against.
        tenant_id = self._tenant_id_for_resolve()

        if tenant_id is not None:
            clauses.append(sql.SQL("tenant_id = %(tenant_id)s"))
            params["tenant_id"] = tenant_id

        if run_ids is not None:
            clauses.append(sql.SQL("run_id = ANY(%(run_ids)s)"))
            params["run_ids"] = list(run_ids)

        if name is not None:
            clauses.append(sql.SQL("name = %(name)s"))
            params["name"] = name

        return await self._requeue(table, clauses, params)

    # ....................... #

    async def _requeue(
        self,
        table: PostgresQualifiedName,
        clauses: list[sql.Composable],
        params: dict[str, object],
    ) -> int:
        # Due at once, keeping attempts (the fence), payloads and cancel stamps.
        return await self.client.execute(
            sql.SQL(
                "UPDATE {table} SET status = 'pending', available_at = NULL, "
                "updated_at = now() WHERE {where}"
            ).format(table=table.ident(), where=sql.SQL(" AND ").join(clauses)),
            params,
            return_rowcount=True,
        )

    # ....................... #

    async def complete(
        self,
        run_id: str,
        *,
        output_json: JsonDict | None,
        fence: int | None = None,
    ) -> None:
        # Seal under the run's *stored* tenant, read here rather than taken from the
        # binding: ``_record_from_row`` unseals with the column, and the two must agree or a
        # completed run is one nothing can open. They differ for exactly the case
        # ``_worker_scope`` keeps reachable — an untagged run finished under a binding.
        stored = await self._seal(output_json, run_id, "output", await self._stored_tenant(run_id))
        await self._finish(
            run_id,
            status=DurableRunStatus.COMPLETED,
            output=None if stored is None else Jsonb(stored),
            error=None,
            fence=fence,
        )

    # ....................... #

    async def fail(self, run_id: str, *, error: str, fence: int | None = None) -> None:
        await self._finish(
            run_id,
            status=DurableRunStatus.FAILED,
            output=None,
            error=error,
            fence=fence,
        )

    # ....................... #

    async def mark_retry(
        self,
        run_id: str,
        *,
        error: str,
        available_at: datetime,
        fence: int | None = None,
    ) -> None:
        await self._finish(
            run_id,
            status=DurableRunStatus.PENDING,
            output=None,
            error=error,
            fence=fence,
            available_at=available_at,
        )

    # ....................... #

    async def mark_forward_incomplete(
        self, run_id: str, *, error: str, fence: int | None = None
    ) -> None:
        await self._finish(
            run_id,
            status=DurableRunStatus.FORWARD_INCOMPLETE,
            output=None,
            error=error,
            fence=fence,
        )

    # ....................... #

    async def mark_cancelled(
        self, run_id: str, *, error: str | None = None, fence: int | None = None
    ) -> None:
        await self._finish(
            run_id,
            status=DurableRunStatus.CANCELLED,
            output=None,
            error=error,
            fence=fence,
        )

    # ....................... #

    async def mark_timed_out(self, run_id: str, *, error: str, fence: int | None = None) -> None:
        await self._finish(
            run_id,
            status=DurableRunStatus.TIMED_OUT,
            output=None,
            error=error,
            fence=fence,
        )

    # ....................... #

    async def refuse_cancel(self, run_id: str, *, fence: int | None = None) -> None:
        table = await self._table()
        params: dict[str, object] = {"run_id": run_id}

        # Fenced but NOT guarded on ``status = 'running'`` (unlike ``_finish``): a refusal
        # records what happened to the ask, and the run it describes may already have landed
        # by the time the holder writes it down. ``COALESCE`` keeps the first refusal's
        # instant, so a repeated write is a no-op.
        fence_clause = sql.SQL("")

        if fence is not None:
            fence_clause = sql.SQL(" AND attempts = %(fence)s")
            params["fence"] = fence

        # Tenant-scoped like ``request_cancel``. Losing the ``status = 'running'`` guard makes
        # this the widest write on the port — it can stamp a run in *any* state — and
        # ``attempts`` is a small integer that collides freely across tenants, so the fence
        # alone is thin protection here. The ask and the refusal are read together by an
        # operator, so they get the same isolation.
        tenant_id = self._tenant_id_for_resolve()
        tenant_filter = sql.SQL("")

        if tenant_id is not None:
            tenant_filter = sql.SQL(" AND tenant_id = %(tenant_id)s")
            params["tenant_id"] = tenant_id

        await self.client.execute(
            sql.SQL(
                """
                UPDATE {table}
                SET cancel_refused_at = COALESCE(cancel_refused_at, now()),
                    updated_at = now()
                WHERE run_id = {run_id}{fence_clause}{tenant_filter}
                """
            ).format(
                table=table.ident(),
                run_id=sql.Placeholder("run_id"),
                fence_clause=fence_clause,
                tenant_filter=tenant_filter,
            ),
            params,
        )

    # ....................... #

    async def request_cancel(self, run_id: str) -> bool:
        table = await self._table()
        params: dict[str, object] = {"run_id": run_id}

        # Scoped like ``list_runs``: an operator bound to one tenant must not stop a run it
        # could not have listed. On a namespace table the resolved table is already
        # per-tenant, so the filter is a redundant no-op.
        tenant_id = self._tenant_id_for_resolve()
        tenant_filter = sql.SQL("")

        if tenant_id is not None:
            tenant_filter = sql.SQL(" AND tenant_id = %(tenant_id)s")
            params["tenant_id"] = tenant_id

        # One statement covers every live state. A PENDING run never claimed has no holder
        # to wait for and no journal to decide by, so it lands CANCELLED here and the
        # recovery scan never picks it up. Every other live run only gets the stamp: a
        # RUNNING one's holder lands the terminal state under its fence, and a QUARANTINED
        # one, or a PENDING one requeued from it, may be a saga past its pivot, decided once
        # a claim can read it. ``COALESCE`` makes a repeated ask keep the first instant. A
        # terminal run matches no row and the caller is told nothing happened.
        row = await self.client.fetch_one(
            sql.SQL(
                """
                UPDATE {table}
                SET status = CASE WHEN status = 'pending' AND attempts = 0
                                  THEN 'cancelled' ELSE status END,
                    leased_until = CASE WHEN status = 'pending' AND attempts = 0
                                        THEN NULL ELSE leased_until END,
                    cancel_requested_at = COALESCE(cancel_requested_at, now()),
                    updated_at = now()
                WHERE run_id = {run_id}
                  AND status IN ('pending', 'running', 'quarantined'){tenant_filter}
                RETURNING run_id
                """
            ).format(
                table=table.ident(),
                run_id=sql.Placeholder("run_id"),
                tenant_filter=tenant_filter,
            ),
            params,
            row_factory="tuple",
        )

        return row is not None

    # ....................... #

    async def purge_finished(
        self,
        older_than: timedelta,
        *,
        statuses: Collection[DurableRunStatus] = PURGEABLE_STATUSES,
        name: str | None = None,
        limit: int = 1000,
    ) -> int:
        check_purge(older_than, statuses, limit)

        if self.journal and self.step_config is None:
            raise exc.configuration(
                "Purging durable runs would leave their step journal behind: pass the run "
                "store its step_config, or journal=False if its runs journal no steps.",
            )

        table = await self._table()
        params: dict[str, object] = {
            "statuses": [str(status) for status in statuses],
            "older_than": older_than,
            "limit": limit,
        }
        # ``updated_at`` is the landing: every terminal write sets it from the database
        # clock, so the window starts when the run finished, not when it was submitted.
        clauses: list[sql.Composable] = [
            sql.SQL("status = ANY(%(statuses)s)"),
            sql.SQL("updated_at < now() - %(older_than)s"),
        ]

        # Scoped like ``list_runs``.
        tenant_id = self._tenant_id_for_resolve()

        if tenant_id is not None:
            clauses.append(sql.SQL("tenant_id = %(tenant_id)s"))
            params["tenant_id"] = tenant_id

        if name is not None:
            clauses.append(sql.SQL("name = %(name)s"))
            params["name"] = name

        journal: sql.Composable = sql.SQL("")

        if self.step_config is not None:
            steps = await resolve_postgres_qname(self.step_config.relation, tenant_id)
            journal = sql.SQL(
                ", journal AS (DELETE FROM {steps} AS s USING gone WHERE s.run_id = gone.run_id)"
            ).format(steps=steps.ident())

        # One statement locks the batch, deletes it and its journal: a run requeued meanwhile
        # holds its row lock, so SKIP LOCKED leaves it and its steps, and no crash between
        # the two deletes can orphan a journal. Oldest landing first.
        row = await self.client.fetch_one(
            sql.SQL(
                """
                WITH doomed AS (
                    SELECT run_id FROM {table} WHERE {where}
                    ORDER BY updated_at
                    LIMIT %(limit)s
                    FOR UPDATE SKIP LOCKED
                ), gone AS (
                    DELETE FROM {table} AS t USING doomed
                    WHERE t.run_id = doomed.run_id
                    RETURNING t.run_id
                ){journal}
                SELECT count(*) FROM gone
                """
            ).format(
                table=table.ident(),
                where=sql.SQL(" AND ").join(clauses),
                journal=journal,
            ),
            params,
            row_factory="tuple",
        )

        return 0 if row is None else int(row[0])

    # ....................... #

    def control_capabilities(self) -> DurableRunControlCapabilities:
        return DurableRunControlCapabilities(supports_cancel=True)

    # ....................... #

    async def load(self, run_id: str) -> DurableRunRecord | None:
        table = await self._table()
        scope_clause, scope_params = self._worker_scope()

        row = await self.client.fetch_one(
            sql.SQL("SELECT {columns} FROM {table} WHERE run_id = {run_id}{scope}").format(
                columns=sql.SQL(_COLUMNS),
                table=table.ident(),
                run_id=sql.Placeholder("run_id"),
                scope=scope_clause,
            ),
            {"run_id": run_id, **scope_params},
        )

        return None if row is None else await self._record_from_row(row)

    # ....................... #

    async def list_runs(
        self,
        *,
        status: DurableRunStatus | None = None,
        name: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> DurableRunPage:
        if limit < 1:
            raise exc.validation("Durable run list limit must be >= 1.")

        table = await self._table()
        params: dict[str, object] = {"limit": limit + 1}
        clauses: list[sql.Composable] = []

        # Scope to the bound tenant when one is bound (per-tenant ops view over a tagged
        # table); unbound, list every tenant's runs. On a namespace table the resolved table
        # is already per-tenant, so the filter is a redundant no-op.
        tenant_id = self._tenant_id_for_resolve()

        if tenant_id is not None:
            clauses.append(sql.SQL("tenant_id = %(tenant_id)s"))
            params["tenant_id"] = tenant_id

        if status is not None:
            clauses.append(sql.SQL("status = %(status)s"))
            params["status"] = str(status)

        if name is not None:
            clauses.append(sql.SQL("name = %(name)s"))
            params["name"] = name

        if cursor is not None:
            # Keyset seek past the last row of the previous page: a row-value comparison keeps
            # the same (created_at DESC, run_id DESC) order the ORDER BY imposes.
            cursor_ts, cursor_id = decode_run_cursor(cursor)
            clauses.append(sql.SQL("(created_at, run_id) < (%(cursor_ts)s, %(cursor_id)s)"))
            params["cursor_ts"] = cursor_ts
            params["cursor_id"] = cursor_id

        where: sql.Composable = sql.SQL("")

        if clauses:
            where = sql.SQL("WHERE ") + sql.SQL(" AND ").join(clauses)

        # Over-fetch one row so build_run_page can tell whether an older page follows without
        # a second round trip.
        rows = await self.client.fetch_all(
            sql.SQL(
                "SELECT {columns} FROM {table} {where} "
                "ORDER BY created_at DESC, run_id DESC LIMIT %(limit)s"
            ).format(
                columns=sql.SQL(_COLUMNS),
                table=table.ident(),
                where=where,
            ),
            params,
        )

        records = [await self._listed(row) for row in rows]

        return build_run_page(records, limit)

    # ....................... #

    async def _finish(
        self,
        run_id: str,
        *,
        status: DurableRunStatus,
        output: Jsonb | None,
        error: str | None,
        fence: int | None = None,
        available_at: datetime | None = None,
    ) -> None:
        table = await self._table()
        scope_clause, scope_params = self._worker_scope()

        # Guarded on ``status = 'running'`` so a terminal state is not overwritten and a
        # duplicate/late completion is a no-op (idempotent under recovery re-invocation).
        # When *fence* is given, also require it to match ``attempts`` so a stale worker
        # whose lease was reclaimed (attempts advanced) cannot finish the run.
        fence_clause = (
            sql.SQL(" AND attempts = {fence}").format(fence=sql.Placeholder("fence"))
            if fence is not None
            else sql.SQL("")
        )

        # A retry goes back to PENDING due at *available_at*; a terminal write leaves
        # ``available_at`` as it is.
        retry = status is DurableRunStatus.PENDING
        due_clause = (
            sql.SQL(", available_at = {due}").format(due=sql.Placeholder("due"))
            if retry
            else sql.SQL("")
        )

        await self.client.execute(
            sql.SQL(
                """
                UPDATE {table}
                SET status = {status}, output = {output}, error = {error},
                    leased_until = NULL, updated_at = now(){due_clause}
                WHERE run_id = {run_id} AND status = 'running'{fence_clause}{scope}
                """
            ).format(
                table=table.ident(),
                status=sql.Placeholder("status"),
                output=sql.Placeholder("output"),
                error=sql.Placeholder("error"),
                due_clause=due_clause,
                run_id=sql.Placeholder("run_id"),
                fence_clause=fence_clause,
                scope=scope_clause,
            ),
            {
                "status": str(status),
                "output": output,
                "error": error,
                "run_id": run_id,
                **({"fence": fence} if fence is not None else {}),
                **({"due": available_at} if retry else {}),
                **scope_params,
            },
        )

    # ....................... #

    async def _stored_tenant(self, run_id: str) -> UUID | None:
        """The tenant column of a run this caller can reach, or ``None``.

        Scoped like the write that follows it, so a run outside the caller's reach reads as
        untagged and the terminal write it feeds then matches nothing — the same silence an
        unreachable run already answers with, rather than a seal against a tenant whose key
        this caller does not hold.
        """

        table = await self._table()
        scope_clause, scope_params = self._worker_scope()

        row = await self.client.fetch_one(
            sql.SQL("SELECT tenant_id FROM {table} WHERE run_id = {run_id}{scope}").format(
                table=table.ident(),
                run_id=sql.Placeholder("run_id"),
                scope=scope_clause,
            ),
            {"run_id": run_id, **scope_params},
        )

        return None if row is None else row["tenant_id"]

    # ....................... #

    async def _load_by_idempotency(
        self,
        table: PostgresQualifiedName,
        idempotency_key: str | None,
    ) -> DurableRunRecord | None:
        if idempotency_key is None:
            return None

        row = await self.client.fetch_one(
            sql.SQL("SELECT {columns} FROM {table} WHERE idempotency_key = {idem}").format(
                columns=sql.SQL(_COLUMNS),
                table=table.ident(),
                idem=sql.Placeholder("idem"),
            ),
            {"idem": idempotency_key},
        )

        return None if row is None else await self._record_from_row(row)

    # ....................... #

    async def _listed(self, row: Any) -> DurableRunRecord:
        # A listing is how an operator finds a run to cancel, so one whose payloads cannot
        # be decrypted is listed without them rather than failing the page.
        try:
            return await self._record_from_row(row)

        except Exception as error:
            # A passing fault would cost one KMS call per row, each to its own timeout,
            # before the page came back stripped; it fails the page instead.
            if is_payload_cipher_missing(error) or is_passing_fault(error):
                raise

            logger.warning(
                "Durable run %s is listed without its payloads, which could not be decrypted (%s)",
                row["run_id"],
                type(error).__name__,
            )

            return await self._record_from_row(row, open_payloads=False)

    # ....................... #

    async def _record_from_row(
        self,
        row: dict[str, Any],
        *,
        open_payloads: bool = True,
    ) -> DurableRunRecord:
        tenant_id = row["tenant_id"]
        run_id = row["run_id"]
        input_json = output_json = None

        if open_payloads:
            input_json = await self._unseal(row["input"], run_id, "input", tenant_id)
            output_json = await self._unseal(row["output"], run_id, "output", tenant_id)

        return DurableRunRecord(
            run_id=run_id,
            name=row["name"],
            status=DurableRunStatus(row["status"]),
            idempotency_key=_unscope_idem(row["idempotency_key"], tenant_id),
            input_json=input_json,
            output_json=output_json,
            error=row["error"],
            tenant_id=tenant_id,
            attempts=row["attempts"],
            available_at=row["available_at"],
            created_at=row["created_at"],
            cancel_requested_at=row["cancel_requested_at"],
            cancel_refused_at=row["cancel_refused_at"],
            payloads_unreadable=not open_payloads,
        )

    # ....................... #

    async def _seal(
        self,
        payload: JsonDict | None,
        run_id: str,
        slot: str,
        tenant_id: UUID | None,
    ) -> JsonDict | None:
        if payload is None or self.cipher is None:
            return payload

        return await encrypt_payload(
            self.cipher,
            payload,
            domain=DURABLE_PAYLOAD_DOMAIN,
            tenant_id=tenant_id,
            record_id=f"{run_id}:{slot}",
        )

    # ....................... #

    async def _unseal(
        self,
        raw: JsonDict | None,
        run_id: str,
        slot: str,
        tenant_id: UUID | None,
    ) -> JsonDict | None:
        if raw is None:
            return None

        return await decrypt_payload(
            self.cipher,
            raw,
            domain=DURABLE_PAYLOAD_DOMAIN,
            tenant_id=tenant_id,
            record_id=f"{run_id}:{slot}",
        )
