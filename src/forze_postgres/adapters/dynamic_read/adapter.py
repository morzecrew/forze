"""Postgres governed dynamic-read adapter — runtime-authored statements, engine-enforced limits.

The shared shell (:class:`~forze.application.integrations.dynamic_read.DynamicReadAdapter`) has
already applied every ceiling by the time this adapter runs; what is left is the part only
Postgres can do, and each line of it is a refusal the *engine* makes:

.. code-block:: text

    BEGIN READ ONLY                       -- sticky for the transaction; survives role games
    SET LOCAL statement_timeout = …       -- always on
    SET LOCAL search_path = …             -- when query_schema is configured
    SET LOCAL ROLE …                      -- when role is configured
    <statement>  via the extended protocol -- one command, server-enforced

No SQL is parsed, matched or rewritten anywhere in this file. That is the plane's central
decision: a parser the framework writes is a parser a statement outgrows, so ``INSERT`` is
refused because the transaction is read-only, ``'SELECT 1; DROP …'`` is refused because the
extended protocol carries one command, and a cross-schema read is refused because the role
lacks the grant.
"""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import Any, cast, final

import attrs
from psycopg import AsyncConnection, capabilities, errors, sql
from psycopg.abc import QueryNoTemplate
from psycopg.rows import dict_row

from forze.application.contracts.resolution import resolve_scoped_namespace
from forze.application.integrations.dynamic_read import (
    DynamicReadAdapter,
    DynamicReadRequest,
    role_unavailable,
)
from forze.base.primitives import JsonDict, OnceCell
from forze_postgres.execution.deps.configs import PostgresDynamicReadConfig
from forze_postgres.kernel.client import (
    PostgresClientPort,
    PostgresTransactionOptions,
)

from .errors import dynamic_read_error

# ----------------------- #

_MAX_STREAM_CHUNK = 1_000
"""Rows pulled per round trip while streaming, when the libpq in use supports chunked mode.

Only a latency/round-trip knob: the governing bound is the request's ``row_probe``, which is
never exceeded regardless of chunk size."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresDynamicReadAdapter(DynamicReadAdapter):
    """One governed dynamic-read route backed by PostgreSQL."""

    client: PostgresClientPort
    config: PostgresDynamicReadConfig

    _query_schema_cell: OnceCell[str] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )
    _role_cell: OnceCell[str] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )

    # ....................... #

    async def _fetch_rows(self, request: DynamicReadRequest) -> Sequence[JsonDict]:
        schema = await self._resolve(self.config.query_schema, self._query_schema_cell, request)
        role = await self._resolve(self.config.role, self._role_cell, request)

        # ``detached`` first, deliberately. A read-only *root* transaction is the only shape in
        # which Postgres will accept the read-only mode at all — inside a caller's transaction
        # this scope would be a savepoint, the mode would silently not apply, and the plane's
        # one engine-enforced guarantee would evaporate exactly where it is most needed. The
        # cost is one extra pooled connection and not seeing the caller's uncommitted writes,
        # which is the right trade for a read plane.
        async with (
            self.client.detached(),
            self.client.bound_connection() as conn,
            self.client.transaction(options=PostgresTransactionOptions(read_only=True)),
            self._translated(),
        ):
            await self._apply_session_settings(conn, request, schema=schema, role=role)

            return await self._stream_rows(conn, request)

    # ....................... #

    @asynccontextmanager
    async def _translated(self) -> AsyncGenerator[None]:
        """Re-raise a psycopg failure as this plane's taxonomy.

        Wrapping the whole scope rather than the fetch alone: a role that does not exist and a
        schema that cannot be entered both fail on the ``SET LOCAL`` line, and a caller reading
        ``dynamic_read_permission_denied`` is better served than one reading a raw ``42501``
        from a statement it never got to run.
        """

        try:
            yield

        except Exception as error:
            mapped = dynamic_read_error(error, route=str(self.spec.name))

            if mapped is None:
                raise

            raise mapped from error

    # ....................... #

    async def _apply_session_settings(
        self,
        conn: AsyncConnection,
        request: DynamicReadRequest,
        *,
        schema: str | None,
        role: str | None,
    ) -> None:
        """Bound the transaction before the statement enters it.

        All three are ``SET LOCAL``: they die with the transaction, so nothing here can leak
        into the next user of this pooled connection. ``ROLE`` goes last because everything
        after it runs as the confined identity — including the statement, which is the point.
        """

        await conn.execute(
            sql.SQL("SET LOCAL statement_timeout = {}").format(
                sql.Literal(_timeout_ms(request)),
            ),
        )

        if schema is not None:
            # Tenant schema first (its relations shadow ``public``), then ``public`` so shared
            # lookups and unqualified extension objects stay reachable.
            await conn.execute(
                sql.SQL("SET LOCAL search_path TO {}, {}").format(
                    sql.Identifier(schema),
                    sql.Identifier("public"),
                ),
            )

        if role is not None:
            try:
                await conn.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))

            except errors.Error as error:
                # A role that does not exist (42704) or that the connection user is not a
                # member of (42501) is a *wiring* fault, and it happens before the statement is
                # even sent. Left to the generic mapping it would egress as
                # ``dynamic_read_statement_invalid`` and blame the caller's statement for a
                # deployment that never granted the membership.
                raise role_unavailable(
                    str(self.spec.name),
                    role=role,
                    detail=str(error.diag.message_primary or error),
                ) from error

    # ....................... #

    async def _stream_rows(
        self,
        conn: AsyncConnection,
        request: DynamicReadRequest,
    ) -> list[JsonDict]:
        """Stream at most ``row_probe`` rows, then stop.

        ``stream`` rather than a plain fetch because the cap has to bound *memory*, not only
        the answer: a plain cursor buffers every row libpq receives before the adapter can
        count them, so a runaway statement would be paid for in full and only then refused.
        Streaming also forces the extended query protocol unconditionally (psycopg's
        ``force_extended``), which is what makes ``'SELECT 1; …'`` a server-side refusal
        instead of something this adapter would have to detect.

        Not a server-side (named) cursor, deliberately: that would wrap the statement in
        ``DECLARE … CURSOR FOR``, where a write is rejected as a *syntax* error before the
        read-only transaction ever gets to refuse it — turning the plane's clearest guarantee
        into a confusing one.
        """

        rows: list[JsonDict] = []

        async with conn.cursor(row_factory=dict_row) as cur:
            stream = cur.stream(
                cast(QueryNoTemplate, request.statement),
                request.params,
                size=_stream_chunk_size(request.row_probe),
            )

            async for row in stream:
                rows.append(dict(row))

                if len(rows) >= request.row_probe:
                    # Exiting the generator cancels the running statement and drains what the
                    # server already sent, so the connection goes back to the pool clean.
                    break

        return rows

    # ....................... #

    async def _resolve(
        self,
        spec: Any,
        cell: OnceCell[str],
        request: DynamicReadRequest,
    ) -> str | None:
        if spec is None:
            return None

        return await resolve_scoped_namespace(
            spec,
            tenant_id=request.tenant_id,
            cell=cell,
        )


# ....................... #


def _timeout_ms(request: DynamicReadRequest) -> int:
    """The effective timeout in whole milliseconds, never rounded down to "no timeout".

    ``SET LOCAL statement_timeout = 0`` disables the timeout entirely, so a sub-millisecond
    request must round *up*: the plane has no spelling for unlimited and must not acquire one
    by arithmetic.
    """

    return max(1, int(request.timeout.total_seconds() * 1000))


# ....................... #


def _stream_chunk_size(row_probe: int) -> int:
    """Rows per round trip, capped by what this libpq can do.

    Chunked streaming needs libpq 17+; older builds get single-row mode, which is slower but
    identical in behaviour. Resolved per call rather than cached because the answer is a
    property of the linked library, and getting it wrong in either direction is worse than one
    cheap check.
    """

    if not capabilities.has_stream_chunked():
        return 1

    return max(1, min(row_probe, _MAX_STREAM_CHUNK))
