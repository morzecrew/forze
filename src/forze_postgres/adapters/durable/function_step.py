"""Postgres durable-function step-memo journal (DBOS-style memoized steps)."""

from __future__ import annotations

from typing import Awaitable, Callable, cast, final
from uuid import UUID

import attrs
import orjson
from psycopg import sql
from psycopg.types.json import Jsonb

from forze.application.contracts.crypto import BytesCipherPort
from forze.application.contracts.durable.function import (
    DurableFunctionStepPort,
    require_durable_run,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.application.integrations.crypto.payload import (
    decrypt_payload,
    encrypt_payload,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, utcnow
from forze_postgres.execution.deps.configs.durable import PostgresDurableStepConfig
from forze_postgres.kernel.client import PostgresClientPort
from forze_postgres.kernel.gateways.base import PostgresQualifiedName
from forze_postgres.kernel.relation import resolve_postgres_qname

# ----------------------- #

DURABLE_PAYLOAD_DOMAIN = "durable"
"""AAD domain binding a journaled durable payload to its ``(run_id, step_id)``."""

_MISSING = object()
"""Sentinel distinguishing "no journal row" from a journaled falsy result (``None``/``0``)."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresDurableFunctionStepAdapter(TenancyMixin, DurableFunctionStepPort):
    """Memoize durable-function step results in a Postgres journal.

    On the first execution of ``(run_id, step_id)`` the step body runs and its result is
    journaled; on replay (crash recovery / re-invocation) the journaled **result** is
    returned and the body is **not** re-run. The guarantee is exactly-once for the recorded
    result — the body may still run more than once if a worker is reclaimed (its run lease
    expired mid-body) or crashes before the result is journaled, so keep step bodies
    idempotent for exactly-once external effects. The active ``run_id`` is read from the
    ambient :class:`~forze.application.contracts.durable.function.DurableRunContext` bound by
    the runner, so it need not thread through every ``step.run`` call.

    Results are journaled as JSON, so a step must return a JSON-serializable value (the
    durable saga executor encodes its context before journaling); a value comes back as its
    JSON projection on replay (e.g. a tuple returns as a list). A configured keyring seals
    the journaled result at rest.

    The table is provided by the application; expected schema::

        CREATE TABLE <relation> (
            run_id     text        NOT NULL,
            step_id    text        NOT NULL,
            result     jsonb       NOT NULL,
            tenant_id  uuid,
            created_at timestamptz NOT NULL,
            PRIMARY KEY (run_id, step_id)
        );
    """

    client: PostgresClientPort
    config: PostgresDurableStepConfig
    cipher: BytesCipherPort | None = None

    # ....................... #

    async def _table(self) -> PostgresQualifiedName:
        tenant_id = self._tenant_id_for_resolve()
        return await resolve_postgres_qname(self.config.relation, tenant_id)

    # ....................... #

    async def run[T](
        self,
        step_id: str,
        fn: Callable[[], Awaitable[T]],
    ) -> T:
        run = require_durable_run()
        tenant_id = self._tenant_id_for_resolve()
        table = await self._table()

        memoized = await self._read(table, run.run_id, step_id, tenant_id)

        if memoized is not _MISSING:
            return cast("T", memoized)

        result = await fn()
        stored = await self._encode(result, step_id, run.run_id, tenant_id)

        rowcount = await self.client.execute(
            sql.SQL(
                """
                INSERT INTO {table} (run_id, step_id, result, tenant_id, created_at)
                VALUES ({run_id}, {step_id}, {result}, {tenant_id}, {created_at})
                ON CONFLICT (run_id, step_id) DO NOTHING
                """
            ).format(
                table=table.ident(),
                run_id=sql.Placeholder("run_id"),
                step_id=sql.Placeholder("step_id"),
                result=sql.Placeholder("result"),
                tenant_id=sql.Placeholder("tenant_id"),
                created_at=sql.Placeholder("created_at"),
            ),
            {
                "run_id": run.run_id,
                "step_id": step_id,
                "result": Jsonb(stored),
                "tenant_id": tenant_id,
                "created_at": utcnow(),
            },
            return_rowcount=True,
        )

        if not rowcount:
            # A concurrent/duplicate runner journaled this step first: converge on the
            # winner's memoized result rather than this attempt's (so every caller agrees on
            # one result; both bodies still ran — an at-least-once effect).
            winner = await self._read(table, run.run_id, step_id, tenant_id)

            if winner is not _MISSING:
                return cast("T", winner)

        return result

    # ....................... #

    async def _read(
        self,
        table: PostgresQualifiedName,
        run_id: str,
        step_id: str,
        tenant_id: UUID | None,
    ) -> object:
        row = await self.client.fetch_one(
            sql.SQL(
                "SELECT result FROM {table} "
                "WHERE run_id = {run_id} AND step_id = {step_id}"
            ).format(
                table=table.ident(),
                run_id=sql.Placeholder("run_id"),
                step_id=sql.Placeholder("step_id"),
            ),
            {"run_id": run_id, "step_id": step_id},
            row_factory="tuple",
        )

        if row is None:
            return _MISSING

        (raw,) = row
        envelope = await decrypt_payload(
            self.cipher,
            cast("JsonDict", raw),
            domain=DURABLE_PAYLOAD_DOMAIN,
            tenant_id=tenant_id,
            record_id=f"{run_id}:{step_id}",
        )

        return envelope["value"]

    # ....................... #

    async def _encode(
        self,
        result: object,
        step_id: str,
        run_id: str,
        tenant_id: UUID | None,
    ) -> JsonDict:
        envelope: JsonDict = {"value": result}

        try:
            orjson.dumps(envelope)
        except TypeError as error:
            raise exc.validation(
                f"Durable step {step_id!r} returned a non-JSON-serializable result; "
                "durable step results must be JSON-serializable to be journaled.",
            ) from error

        if self.cipher is None:
            return envelope

        return await encrypt_payload(
            self.cipher,
            envelope,
            domain=DURABLE_PAYLOAD_DOMAIN,
            tenant_id=tenant_id,
            record_id=f"{run_id}:{step_id}",
        )
