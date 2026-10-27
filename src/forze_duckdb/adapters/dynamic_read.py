"""DuckDB governed dynamic-read adapter — trusted lake reads on a per-tenant instance.

The shared shell (:class:`~forze.application.integrations.dynamic_read.DynamicReadAdapter`) has
applied every ceiling by the time this adapter runs. What is left happens on the tenant's own
locked-down instance (:class:`~forze_duckdb.RoutedDuckDbClient`):

.. code-block:: text

    USE lake.<namespace>                    -- on a fresh cursor
    extract_statements(<statement>)         -- DuckDB's parser: exactly one SELECT
    execute(<the classified Statement>)     -- never the string, never rewritten
    fetchmany(row_probe)
    DROP TEMPORARY SECRET __internal_ic_…   -- the credentials the statement was vended

The classification is a belt. Read-only belongs to the catalog's ``select``-only grant, the
store, and the ``READ_ONLY`` attach; the container is the instance, whose catalog principal
reaches only the tenant's warehouse (RFC 0078 §3.5).

The translation below maps DuckDB's exception classes, plus the messages of the bare
``duckdb.Error`` the ``iceberg`` extension raises for catalog refusals, onto the plane's
taxonomy (RFC 0078 §4.5, EXECUTION-LOG D-029).
"""

from forze_duckdb._compat import require_duckdb

require_duckdb()

# ....................... #

import re
from collections.abc import Sequence
from typing import final

import attrs
import duckdb

from forze.application.integrations.dynamic_read import (
    MULTI_STATEMENT_CODE,
    STATEMENT_INVALID_CODE,
    WRITE_REFUSED_CODE,
    DynamicReadAdapter,
    DynamicReadRequest,
    multi_statement,
    permission_denied,
    statement_invalid,
    timed_out,
    write_refused,
)
from forze.base.exceptions import CoreException, exc, fallback_exception_mapper
from forze.base.primitives import JsonDict
from forze_duckdb.execution.deps.configs import DuckDbDynamicReadConfig
from forze_duckdb.kernel.client import (
    LAKE_CATALOG,
    QUERY_TIMEOUT_CODE,
    DuckDbDynamicReadClientPort,
)

# ----------------------- #

DUCKDB_TENANT_PLACEHOLDER = r"\$tenant\b"
"""DuckDB's named-parameter spelling of the shared tenant parameter."""

_CATALOG_REFUSALS = (
    "NotFound_404",
    "Unauthorized_401",
    "Forbidden_403",
    "No region was provided via the vended credentials",
)
"""Messages of the bare ``duckdb.Error`` that mean the catalog refused the principal.

``NotFound_404`` is how Lakekeeper answers a principal it hides a warehouse from; the last one
is a ``LoadTable`` that came back without credentials, which is what a revocation looks like
while the authorizer's cache expires (D-032) and also what a store profile with no region
produces. Every one carries its message into the error details, so the two stay apart."""

_HTTP_REFUSAL = re.compile(r"HTTP (?:code )?40[13]\b")
"""A 401 or 403 from the store, as ``HTTPException`` spells it in its message."""

_READ_ONLY_ATTACH = "attached in read-only mode"

_LOCKED = ("the configuration has been locked", "Displaying unredacted secrets is disabled")
"""``InvalidInputException`` messages that are the session's lock refusing a statement."""

_fallback = fallback_exception_mapper("DuckDB")

# ....................... #


def dynamic_read_error(error: Exception, *, route: str) -> Exception:
    """Translate *error* from a dynamic read on *route* into this plane's taxonomy.

    What is not the statement's doing — an ``IOException`` from a hung or failing request, an
    ``OutOfMemoryException`` — becomes an ``infrastructure`` error. A
    :class:`~forze.base.exceptions.CoreException` the client raised passes through, except
    the classification refusals and the timeout, which gain the route.
    """

    if isinstance(error, CoreException):
        return _core_error(error, route=route)

    if not isinstance(error, duckdb.Error):
        return error

    message = str(error)

    if isinstance(error, duckdb.InterruptException):
        return timed_out(route)

    if isinstance(error, duckdb.PermissionException):
        return permission_denied(route, detail=message)

    # Before the infrastructure fallback: ``HTTPException`` is an ``IOException``.
    if isinstance(error, duckdb.HTTPException) and _HTTP_REFUSAL.search(message):
        return permission_denied(route, detail=message)

    if type(error) is duckdb.Error and any(marker in message for marker in _CATALOG_REFUSALS):
        return permission_denied(route, detail=message)

    if isinstance(error, duckdb.InvalidInputException):
        return _invalid_input(message, route=route)

    if isinstance(error, duckdb.ConversionException):
        # The message quotes the value that failed, which may have come out of a column.
        return statement_invalid(route, detail=type(error).__name__)

    if isinstance(error, (duckdb.ParserException, duckdb.BinderException, duckdb.CatalogException)):
        return statement_invalid(route, detail=message)

    return _fallback(error, site="dynamic_read", details={"route": route})


def _core_error(error: CoreException, *, route: str) -> CoreException:
    if error.code == MULTI_STATEMENT_CODE:
        return multi_statement(route)

    if error.code == WRITE_REFUSED_CODE:
        return write_refused(route, statement_type=(error.details or {}).get("statement_type"))

    if error.code == QUERY_TIMEOUT_CODE:
        return timed_out(route)

    if error.code == STATEMENT_INVALID_CODE:
        return statement_invalid(route, detail=(error.details or {}).get("detail"))

    return error


def _invalid_input(message: str, *, route: str) -> CoreException:
    if _READ_ONLY_ATTACH in message:
        return write_refused(route)

    if any(marker in message for marker in _LOCKED):
        return permission_denied(route, detail=message)

    return statement_invalid(route, detail=message)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DuckDbDynamicReadAdapter(DynamicReadAdapter):
    """One governed dynamic-read route over a tenant's lake warehouse."""

    client: DuckDbDynamicReadClientPort
    config: DuckDbDynamicReadConfig
    """The route's wiring. Its ``statement_timeout`` must match the shell's."""

    tenant_placeholder_pattern: str = attrs.field(default=DUCKDB_TENANT_PLACEHOLDER)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.statement_timeout != self.config.statement_timeout:
            raise exc.internal(
                "Dynamic-read adapter built with a statement_timeout that disagrees with its "
                "config; pass config.statement_timeout.",
                details={
                    "route": str(self.spec.name),
                    "adapter": str(self.statement_timeout),
                    "config": str(self.config.statement_timeout),
                },
            )

    # ....................... #

    async def _fetch_rows(self, request: DynamicReadRequest) -> Sequence[JsonDict]:
        try:
            return await self.client.run_dynamic(
                request.statement,
                request.params,
                row_probe=request.row_probe,
                timeout=request.timeout,
                use=f"{LAKE_CATALOG}.{self.config.namespace}",
            )

        except Exception as error:
            mapped = dynamic_read_error(error, route=str(self.spec.name))

            if mapped is error:
                raise

            raise mapped from error
