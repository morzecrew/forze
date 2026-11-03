"""Postgres procedures adapter — governed parametrized command/compute.

Thin wrapper over :class:`~forze_postgres.kernel.client.PostgresClient`: binds the typed params
(sealing encrypted fields and the ``%(tenant)s`` floor), runs the registered statement, and
dispatches the result on the spec's declared cardinality.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, cast, final
from uuid import UUID

import attrs
from psycopg import AsyncConnection
from psycopg.abc import QueryNoTemplate
from pydantic import BaseModel

from forze.application.contracts.procedure import (
    ExecResult,
    ProcedurePort,
    ProcedureSpec,
)
from forze.application.contracts.resolution import resolve_scoped_namespace
from forze.application.contracts.tenancy import TenantProviderPort, soft_tenant_id
from forze.application.integrations.tenancy_sql import bind_tenant_param
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, OnceCell
from forze.base.serialization import default_model_codec
from forze_postgres.execution.deps.configs import PostgresProcedureConfig
from forze_postgres.kernel.client import PostgresClientPort, scoped_local_settings

from .._logger import logger

# ----------------------- #

CANCEL_GRACE = timedelta(seconds=5)
"""How long a timed-out autocommit statement gets to stop after the server is asked to cancel
it, before its connection is closed instead."""

# ....................... #


async def _stop_statement(statement: asyncio.Future[Any], conn: AsyncConnection) -> None:
    """Cancel *statement* on the server; close *conn* if that does not finish in time.

    Cancelling the task makes psycopg send the server a cancel request and then drain the
    connection, with no bound on the drain. Only a drain that ends within
    :data:`CANCEL_GRACE` leaves the connection usable. Otherwise a second cancel cuts the
    drain short, or the drain failed, and the connection's state is unknown: it is closed,
    and the pool discards a closed connection rather than lend it out again.
    """

    statement.cancel()
    done, _ = await asyncio.wait({statement}, timeout=CANCEL_GRACE.total_seconds())
    clean = bool(done) and (statement.cancelled() or statement.exception() is None)

    if not done:
        statement.cancel()
        done, _ = await asyncio.wait({statement}, timeout=CANCEL_GRACE.total_seconds())

        if not done:
            logger.warning("A canceled autocommit procedure did not stop; closing its connection")

    if not clean:
        await conn.close()


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresProcedureAdapter[In: BaseModel, Out](ProcedurePort[In, Out]):
    """A single governed procedure backed by PostgreSQL (command-only)."""

    client: PostgresClientPort
    spec: ProcedureSpec[In, Out]
    config: PostgresProcedureConfig
    tenant_provider: TenantProviderPort | None = None
    """Tenant context for ``%(tenant)s`` binding and per-tenant ``query_schema`` resolution."""

    _query_schema_cell: OnceCell[str] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )

    # ....................... #

    def _tenant_id_for_resolve(self) -> UUID | None:
        # Fail closed on a tenant-aware route with no bound tenant — mirrors ``bind_tenant_param``
        # and ``TenancyMixin._tenant_id_for_resolve`` — so a dynamic ``query_schema`` resolver is
        # never invoked with ``None`` ahead of the authentication error.
        if self.config.tenant_aware:
            if self.tenant_provider is None:
                raise exc.configuration(
                    "Tenant provider is required for a tenant-aware procedure route.",
                    code="procedures_tenant_provider_missing",
                )

            tenant = self.tenant_provider()

            if tenant is None:
                raise exc.authentication("Tenant ID is required", code="tenant_required")

            return tenant.tenant_id

        return soft_tenant_id(self.tenant_provider)

    # ....................... #

    async def _query_schema(self) -> str | None:
        spec = self.config.query_schema

        if spec is None:
            return None

        return await resolve_scoped_namespace(
            spec,
            tenant_id=self._tenant_id_for_resolve(),
            cell=self._query_schema_cell,
        )

    # ....................... #

    async def _bound_params(self, params: In) -> dict[str, object]:
        if not isinstance(params, self.spec.params):
            raise exc.precondition(
                f"Procedure {self.spec.name!r} params must be a "
                f"{self.spec.params.__name__} instance."
            )

        # Always bind through the resolved params codec so a custom codec's field names/values are
        # honored; an encrypting codec (wrapped by the factory via ``resolve_procedure_codecs_spec``)
        # also needs its cipher warmed first, hence the ``prepare_encrypt`` pre-pass when present.
        codec = self.spec.resolved_params_codec
        prepare_encrypt = getattr(codec, "prepare_encrypt", None)

        if prepare_encrypt is not None:
            await prepare_encrypt()

        data: JsonDict = codec.encode_persistence_mapping(params)

        bound = bind_tenant_param(
            data,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=self.tenant_provider,
            subject="procedures",
        )

        return dict(bound)

    # ....................... #

    async def _execute_autocommit(self, fn: Callable[[], Awaitable[Any]]) -> Any:
        """Run *fn* out of transaction on its own connection, bounded by the route's timeout.

        ``REFRESH ... CONCURRENTLY`` and some maintenance cannot run in a transaction, so there
        is no ``SET LOCAL`` to scope (the config forbids ``query_schema``). The connection is
        checked out before the clock starts, so the timeout covers the statement and not the
        wait for a connection, which the pool's acquire timeout bounds. When it passes, the
        statement is canceled on the server (:func:`_stop_statement`).

        :raises CoreException: ``infrastructure``, "Database query canceled (timeout)", the
            error a server-side ``statement_timeout`` maps to on the transactional path.
        """

        async with self.client.detached(), self.client.bound_connection() as conn:
            await conn.set_autocommit(True)
            statement = asyncio.ensure_future(fn())

            try:
                done, _ = await asyncio.wait(
                    {statement},
                    timeout=self.config.statement_timeout.total_seconds(),
                )

                if not done:
                    await _stop_statement(statement, conn)

            except BaseException:
                # Our caller was canceled: the statement must not outlive it on a pooled
                # connection.
                await _stop_statement(statement, conn)
                raise

            finally:
                # The pool's reset callback clears a leaked autocommit flag as well.
                if not conn.closed:
                    with contextlib.suppress(Exception):
                        await conn.set_autocommit(False)

        finished = statement.done() and not statement.cancelled()

        # In time, or just as the timeout passed: a committed effect is reported either way.
        if done or (finished and statement.exception() is None):
            return statement.result()

        cause = statement.exception() if finished else None

        raise exc.infrastructure(
            "Database query canceled (timeout).",
            details={"route": str(self.spec.name)},
        ) from cause

    # ....................... #

    async def _execute(self, fn: Callable[[], Awaitable[Any]]) -> Any:
        if not self.config.in_transaction:
            return await self._execute_autocommit(fn)

        schema = await self._query_schema()

        # Inside a caller transaction (the normal command-handler case) ours is a savepoint, and
        # ``SET LOCAL`` merges into the *outer* transaction when the savepoint is released — so
        # the procedure's per-tenant search_path and statement_timeout would outlive it and apply
        # to every subsequent statement of the caller's transaction. Capture the caller's values
        # first and put them back after (the same scope the analytics adapter runs its SET LOCALs
        # in). At the root there is nothing to restore: the transaction ends with the statement,
        # taking the settings with it.
        in_caller_transaction = self.client.is_in_transaction()

        async with (
            self.client.transaction(),
            scoped_local_settings(
                self.client,
                statement_timeout=self.config.statement_timeout,
                search_path=schema,
                restore=in_caller_transaction,
            ),
        ):
            return await fn()

    # ....................... #

    async def run(self, params: In) -> ExecResult[Out]:
        bound = await self._bound_params(params)
        query = cast(QueryNoTemplate, self.config.sql)

        async def _dispatch() -> ExecResult[Out]:
            if self.spec.returns_scalar:
                value = await self.client.fetch_value(query, bound)
                return ExecResult(value=cast("Out | None", self.spec.coerce_scalar(value)))

            if self.spec.returns_row:
                row = await self.client.fetch_one(query, bound)

                if row is None:
                    return ExecResult(value=None)

                result_type = cast("type[BaseModel]", self.spec.result)
                decoded = default_model_codec(result_type).decode_mapping(row)
                return ExecResult(value=cast("Out", decoded))

            # Side-effect (result=None): surface the statement rowcount (rows affected by DML /
            # CALL). A function that *returns* a count must declare a scalar result instead —
            # `SELECT my_fn(...)` yields one row, so its rowcount here is 1, not the value.
            count = await self.client.execute(query, bound, return_rowcount=True)
            return ExecResult(affected_count=count)

        return await self._execute(_dispatch)
