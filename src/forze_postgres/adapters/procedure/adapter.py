"""Postgres procedures adapter — governed parametrized command/compute.

Thin wrapper over :class:`~forze_postgres.kernel.client.PostgresClient`: binds the typed params
(sealing encrypted fields and the ``%(tenant)s`` floor), runs the registered statement, and
dispatches the result on the spec's declared cardinality.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, cast, final
from uuid import UUID

import attrs
from psycopg import sql
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
from forze_postgres.kernel.client import PostgresClientPort

# ----------------------- #


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
                raise exc.authentication(
                    "Tenant ID is required", code="tenant_required"
                )

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

    async def _execute(self, fn: Callable[[], Awaitable[Any]]) -> Any:
        if not self.config.in_transaction:
            # Autocommit path: REFRESH ... CONCURRENTLY and some maintenance cannot run in a
            # transaction. The config forbids query_schema / statement_timeout here, so there is
            # no SET LOCAL to scope.
            return await fn()

        schema = await self._query_schema()
        timeout = self.config.statement_timeout

        async with self.client.transaction():
            if timeout is not None:
                await self.client.execute(
                    sql.SQL("SET LOCAL statement_timeout = {}").format(
                        sql.Literal(int(timeout.total_seconds() * 1000)),
                    ),
                )

            if schema is not None:
                # Tenant schema first (its tables shadow ``public``), then ``public`` so
                # unqualified extension objects and shared lookups stay reachable.
                await self.client.execute(
                    sql.SQL("SET LOCAL search_path TO {}, {}").format(
                        sql.Identifier(schema),
                        sql.Identifier("public"),
                    ),
                )

            return await fn()

    # ....................... #

    async def run(self, params: In) -> ExecResult[Out]:
        bound = await self._bound_params(params)
        query = cast(QueryNoTemplate, self.config.sql)

        async def _dispatch() -> ExecResult[Out]:
            if self.spec.returns_scalar:
                value = await self.client.fetch_value(query, bound)
                return ExecResult(value=cast("Out | None", value))

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
