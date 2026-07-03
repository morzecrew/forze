"""Postgres durable-run store (run instances + lease-based crash recovery claims)."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Sequence, final
from uuid import UUID

import attrs
from psycopg import sql
from psycopg.types.json import Jsonb

from forze.application.contracts.crypto import BytesCipherPort
from forze.application.contracts.durable.function import (
    DurableRunRecord,
    DurableRunStatus,
    DurableRunStorePort,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.application.integrations.crypto.payload import (
    decrypt_payload,
    encrypt_payload,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, utcnow, uuid7
from forze_postgres.adapters.durable.function_step import DURABLE_PAYLOAD_DOMAIN
from forze_postgres.execution.deps.configs.durable import PostgresDurableRunConfig
from forze_postgres.kernel.client import PostgresClientPort
from forze_postgres.kernel.gateways.base import PostgresQualifiedName
from forze_postgres.kernel.relation import resolve_postgres_qname

# ----------------------- #

_COLUMNS = (
    "run_id, name, status, idempotency_key, input, output, error, tenant_id, "
    "attempts, available_at"
)
"""Row projection for plain single-table SELECTs."""

_T_COLUMNS = (
    "t.run_id, t.name, t.status, t.idempotency_key, t.input, t.output, "
    "t.error, t.tenant_id, t.attempts, t.available_at"
)
"""Row projection for RETURNING out of an ``UPDATE ... FROM picked`` join (qualified to
resolve the ``run_id`` shared with the ``picked`` CTE); column names stay unqualified."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresDurableRunStore(TenancyMixin, DurableRunStorePort):
    """Postgres-backed durable-run store.

    Records run instances and hands out claims for execution and crash recovery. A crashed
    run is left ``RUNNING`` with an expired lease; :meth:`claim_abandoned` re-claims it with
    ``FOR UPDATE SKIP LOCKED`` (single-leader-safe, concurrent-scanner-safe). Re-submits
    under one ``idempotency_key`` converge on a single run (``ON CONFLICT DO NOTHING``).

    **Tenancy.** The table is resolved under the bound tenant, so a static ``relation`` is a
    shared **tagged** table (``tenant_id`` column) and a per-tenant ``relation`` resolver is a
    **namespace** table. Recovery either runs unbound over a tagged table (claims every
    tenant's runs; the runner re-binds each run's tenant to execute it) or per-tenant over a
    namespace table (the scanner binds each tenant in turn). Non-enforcing: an unbound scan
    never fails, and a bound scan claims only that tenant's runs. The table is provided by the
    application; expected schema::

        CREATE TABLE <relation> (
            run_id          text        NOT NULL,
            name            text        NOT NULL,
            status          text        NOT NULL,
            idempotency_key text,
            input           jsonb,
            output          jsonb,
            error           text,
            tenant_id       uuid,
            attempts        integer     NOT NULL DEFAULT 0,
            leased_until    timestamptz,
            available_at    timestamptz,
            created_at      timestamptz NOT NULL,
            updated_at      timestamptz NOT NULL,
            PRIMARY KEY (run_id),
            UNIQUE (idempotency_key)
        );

    ``attempts`` doubles as the fence token (advances under a row lock on each claim);
    ``available_at`` delays when a ``PENDING`` run may be claimed. Concurrent scanners are
    safe (``FOR UPDATE SKIP LOCKED``) and a terminal write can be fenced against a
    reclaimed lease — so the store is multi-worker-safe, not just single-leader.
    """

    client: PostgresClientPort
    config: PostgresDurableRunConfig
    cipher: BytesCipherPort | None = None

    # ....................... #

    async def _table(self) -> PostgresQualifiedName:
        return await resolve_postgres_qname(
            self.config.relation, self._tenant_id_for_resolve()
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
    ) -> DurableRunRecord:
        # Default the tenant column to the bound tenant so a run enqueued under a namespace
        # binding still tags its tenant (the recovery filter matches on it).
        tenant_id = tenant_id if tenant_id is not None else self._tenant_id_for_resolve()
        table = await self._table()
        run_id = str(uuid7())
        now = utcnow()
        stored_input = await self._seal(input_json, run_id, "input", tenant_id)

        row = await self.client.fetch_one(
            sql.SQL(
                """
                INSERT INTO {table}
                    (run_id, name, status, idempotency_key, input, tenant_id,
                     attempts, leased_until, available_at, created_at, updated_at)
                VALUES
                    ({run_id}, {name}, 'pending', {idem}, {input}, {tenant_id},
                     0, NULL, {available_at}, {now}, {now})
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING run_id
                """
            ).format(
                table=table.ident(),
                run_id=sql.Placeholder("run_id"),
                name=sql.Placeholder("name"),
                idem=sql.Placeholder("idem"),
                input=sql.Placeholder("input"),
                tenant_id=sql.Placeholder("tenant_id"),
                available_at=sql.Placeholder("available_at"),
                now=sql.Placeholder("now"),
            ),
            {
                "run_id": run_id,
                "name": name,
                "idem": idempotency_key,
                "input": None if stored_input is None else Jsonb(stored_input),
                "tenant_id": tenant_id,
                "available_at": available_at,
                "now": now,
            },
            row_factory="tuple",
        )

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
            )

        # A run already exists for this idempotency key: converge on it.
        existing = await self._load_by_idempotency(table, idempotency_key)

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

        row = await self.client.fetch_one(
            sql.SQL(
                """
                WITH picked AS (
                    SELECT run_id FROM {table}
                    WHERE run_id = {run_id} AND status = 'pending'
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
            ),
            {"run_id": run_id, "lease": lease_for},
        )

        if row is None:
            return None

        return await self._record_from_row(row)

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

        return [await self._record_from_row(row) for row in rows]

    # ....................... #

    async def complete(
        self,
        run_id: str,
        *,
        output_json: JsonDict | None,
        fence: int | None = None,
    ) -> None:
        stored = await self._seal(output_json, run_id, "output", None)
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

    async def load(self, run_id: str) -> DurableRunRecord | None:
        table = await self._table()

        row = await self.client.fetch_one(
            sql.SQL("SELECT {columns} FROM {table} WHERE run_id = {run_id}").format(
                columns=sql.SQL(_COLUMNS),
                table=table.ident(),
                run_id=sql.Placeholder("run_id"),
            ),
            {"run_id": run_id},
        )

        return None if row is None else await self._record_from_row(row)

    # ....................... #

    async def _finish(
        self,
        run_id: str,
        *,
        status: DurableRunStatus,
        output: Jsonb | None,
        error: str | None,
        fence: int | None = None,
    ) -> None:
        table = await self._table()

        # Guarded on ``status = 'running'`` so a terminal state is not overwritten and a
        # duplicate/late completion is a no-op (idempotent under recovery re-invocation).
        # When *fence* is given, also require it to match ``attempts`` so a stale worker
        # whose lease was reclaimed (attempts advanced) cannot finish the run.
        fence_clause = (
            sql.SQL(" AND attempts = {fence}").format(fence=sql.Placeholder("fence"))
            if fence is not None
            else sql.SQL("")
        )

        await self.client.execute(
            sql.SQL(
                """
                UPDATE {table}
                SET status = {status}, output = {output}, error = {error},
                    leased_until = NULL, updated_at = now()
                WHERE run_id = {run_id} AND status = 'running'{fence_clause}
                """
            ).format(
                table=table.ident(),
                status=sql.Placeholder("status"),
                output=sql.Placeholder("output"),
                error=sql.Placeholder("error"),
                run_id=sql.Placeholder("run_id"),
                fence_clause=fence_clause,
            ),
            {
                "status": str(status),
                "output": output,
                "error": error,
                "run_id": run_id,
                **({"fence": fence} if fence is not None else {}),
            },
        )

    # ....................... #

    async def _load_by_idempotency(
        self,
        table: PostgresQualifiedName,
        idempotency_key: str | None,
    ) -> DurableRunRecord | None:
        if idempotency_key is None:
            return None

        row = await self.client.fetch_one(
            sql.SQL(
                "SELECT {columns} FROM {table} WHERE idempotency_key = {idem}"
            ).format(
                columns=sql.SQL(_COLUMNS),
                table=table.ident(),
                idem=sql.Placeholder("idem"),
            ),
            {"idem": idempotency_key},
        )

        return None if row is None else await self._record_from_row(row)

    # ....................... #

    async def _record_from_row(self, row: dict[str, Any]) -> DurableRunRecord:
        tenant_id = row["tenant_id"]
        run_id = row["run_id"]
        input_json = await self._unseal(row["input"], run_id, "input", tenant_id)
        output_json = await self._unseal(row["output"], run_id, "output", tenant_id)

        return DurableRunRecord(
            run_id=run_id,
            name=row["name"],
            status=DurableRunStatus(row["status"]),
            idempotency_key=row["idempotency_key"],
            input_json=input_json,
            output_json=output_json,
            error=row["error"],
            tenant_id=tenant_id,
            attempts=row["attempts"],
            available_at=row["available_at"],
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
