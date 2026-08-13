"""The governance shell every dynamic-read adapter shares.

The plane's whole claim is that the *framework* — not the caller — enforces the limits,
tenancy resolution and taxonomy for statements it cannot inspect. If each backend
re-implemented that shell, the claim would be true per-backend at best: caps would drift, one
adapter would truncate where another raises, and the mock would stop being a differential
oracle for any of it.

So the shell lives here once and the backends differ in exactly one step — ``_fetch_rows``,
"run this statement and hand back up to *n* rows". Everything before and after it (byte cap,
row cap probe, per-call clamping, tenant resolution and fail-closed refusal, tenant-parameter
merge, row validation) is the same code on every engine, which is what makes a mock ≡ real
comparison of it meaningful.

What the shell deliberately does **not** do is inspect the statement. Every refusal that
depends on what the statement *says* — a write, a second command, a cross-schema read — is the
engine's to make, because a parser the framework writes is a parser an attacker outgrows.
"""

from abc import abstractmethod
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any, final
from uuid import UUID

import attrs
from pydantic import BaseModel, ValidationError

from forze.application.contracts.dynamic_read import (
    DynamicReadOptions,
    DynamicReadPort,
    DynamicReadSpec,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from ..tenancy_sql import TENANT_PARAM, sql_references_param
from .errors import row_cap_exceeded, statement_too_large

# ----------------------- #

POSTGRES_TENANT_PLACEHOLDER = r"%\(tenant\)s"
"""psycopg's named-placeholder spelling of the shared tenant parameter."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DynamicReadRequest:
    """One governed execution, after the shell has resolved every ceiling.

    Handed to :meth:`DynamicReadAdapter._fetch_rows` so a backend never re-derives a limit —
    and so the mock can *observe* the clamping the real engine applies, which is the only way
    a differential can pin it.
    """

    statement: str
    """The runtime-authored statement, unmodified. The shell never rewrites it."""

    params: JsonDict
    """Bound parameters, including the tenant id when the statement references it."""

    row_cap: int
    """Effective row ceiling — the spec's cap, clamped down by the call's option."""

    row_probe: int
    """Rows to actually request: :attr:`row_cap` + 1, so "too many" is observable.

    The extra row is what turns a silent truncation into a refusal. Fetching exactly the cap
    cannot distinguish "the result is cap rows" from "the result is larger and you are seeing
    a prefix", and those two answers render the same dashboard with different meanings.
    """

    timeout: timedelta
    """Effective statement timeout — the route's, clamped down by the call's option."""

    tenant_id: UUID | None
    """The bound tenant, when one is bound. ``None`` on a route with no tenant in context."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DynamicReadAdapter(DynamicReadPort, TenancyMixin):
    """Shared governance for the dynamic-read plane; backends implement one method."""

    spec: DynamicReadSpec
    """The route's spec — the source of the byte cap, row cap and capture policy."""

    statement_timeout: timedelta
    """The route's configured statement timeout; a call may clamp below it, never above."""

    tenant_placeholder_pattern: str = attrs.field(default=POSTGRES_TENANT_PLACEHOLDER)
    """Regex for the backend's spelling of the tenant placeholder.

    Only used to decide whether to *merge* the tenant id into the bound parameters. It is
    advisory convenience for a trusted statement that wants a predicate in addition to its
    container — the container (schema / role / database) is the boundary, and a statement that
    never mentions the placeholder is still confined by it."""

    # ....................... #

    @abstractmethod
    async def _fetch_rows(self, request: DynamicReadRequest) -> Sequence[JsonDict]:
        """Execute *request* and return at most :attr:`DynamicReadRequest.row_probe` rows.

        The one backend-specific step. It must not truncate to ``row_cap`` itself: the shell
        needs the probe row to tell a full result from an overflowing one.
        """

        ...  # pragma: no cover

    # ....................... #

    async def run(
        self,
        statement: str,
        params: JsonDict | None = None,
        *,
        options: DynamicReadOptions | None = None,
    ) -> Sequence[JsonDict]:
        request = self._prepare(statement, params, options)
        rows = await self._fetch_rows(request)

        if len(rows) > request.row_cap:
            raise row_cap_exceeded(str(self.spec.name), row_cap=request.row_cap)

        return rows

    # ....................... #

    async def select[T: BaseModel](
        self,
        return_type: type[T],
        statement: str,
        params: JsonDict | None = None,
        *,
        options: DynamicReadOptions | None = None,
    ) -> Sequence[T]:
        rows = await self.run(statement, params, options=options)

        try:
            return [return_type.model_validate(row) for row in rows]

        except ValidationError as error:
            # The caller compiled the statement *and* named the type it should produce, so a
            # mismatch between them is its own bug — not the engine's, and not an internal
            # one. Surfaced at the port boundary rather than leaking half-validated rows.
            raise exc.validation(
                f"Dynamic read rows do not match {return_type.__name__}.",
                code="dynamic_read_row_type_mismatch",
                details={"route": str(self.spec.name), "detail": str(error)},
            ) from error

    # ....................... #

    def _prepare(
        self,
        statement: str,
        params: JsonDict | None,
        options: DynamicReadOptions | None,
    ) -> DynamicReadRequest:
        """Apply every ceiling and resolve tenancy — before any connection is touched.

        Order matters: the byte cap and the tenancy refusal both run ahead of the engine, so a
        route with no bound tenant fails closed without spending a connection, and an oversized
        statement never reaches the wire.
        """

        self._check_statement_size(statement)

        # Fail closed first: on a tenant-aware route with no bound tenant this raises
        # ``tenant_required``, which is the answer whether or not the statement is well-formed.
        tenant_id = self._tenant_id_for_resolve()

        bound: JsonDict = dict(params or {})

        if tenant_id is not None and sql_references_param(
            statement,
            pattern=self.tenant_placeholder_pattern,
        ):
            bound[TENANT_PARAM] = str(tenant_id)

        row_cap = self._effective_row_cap(options)

        return DynamicReadRequest(
            statement=statement,
            params=bound,
            row_cap=row_cap,
            row_probe=row_cap + 1,
            timeout=self._effective_timeout(options),
            tenant_id=tenant_id,
        )

    # ....................... #

    def _check_statement_size(self, statement: str) -> None:
        if not statement.strip():
            raise exc.validation(
                "Dynamic read statement must be non-empty.",
                code="dynamic_read_statement_empty",
                details={"route": str(self.spec.name)},
            )

        size = len(statement.encode("utf-8"))

        if size > self.spec.max_statement_bytes:
            raise statement_too_large(
                str(self.spec.name),
                size=size,
                limit=self.spec.max_statement_bytes,
            )

    # ....................... #

    def _effective_row_cap(self, options: DynamicReadOptions | None) -> int:
        requested = self._option(options, "row_cap")

        if requested is None:
            return self.spec.row_cap

        if not isinstance(requested, int) or isinstance(requested, bool) or requested < 1:
            raise exc.validation(
                "Dynamic read options.row_cap must be a positive integer.",
                code="dynamic_read_option_invalid",
                details={"route": str(self.spec.name), "row_cap": repr(requested)},
            )

        # Clamp down only. A per-call ceiling above the route's would let the caller undo the
        # wiring decision it is supposed to be bounded by.
        return min(requested, self.spec.row_cap)

    # ....................... #

    def _effective_timeout(self, options: DynamicReadOptions | None) -> timedelta:
        requested = self._option(options, "timeout")

        if requested is None:
            return self.statement_timeout

        if not isinstance(requested, timedelta) or requested.total_seconds() <= 0:
            raise exc.validation(
                "Dynamic read options.timeout must be a positive timedelta.",
                code="dynamic_read_option_invalid",
                details={"route": str(self.spec.name), "timeout": repr(requested)},
            )

        return min(requested, self.statement_timeout)

    # ....................... #

    @staticmethod
    def _option(options: DynamicReadOptions | None, key: str) -> Any:
        if not options:
            return None

        return dict[str, Any](options).get(key)

    # ....................... #

    @staticmethod
    def _rows_as_mappings(rows: Sequence[Mapping[str, Any]]) -> list[JsonDict]:
        """Normalize backend rows to plain dicts in column order."""

        return [dict(row) for row in rows]
