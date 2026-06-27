from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Final
from uuid import UUID

import attrs
from fastapi import Request
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from forze.application.contracts.envelope import HTTP_HEADER_DEADLINE_BUDGET
from forze.application.execution.context import (
    ExecutionContextFactory,
    InvocationMetadata,
)
from forze.base.primitives import uuid7

# ----------------------- #

IDEMPOTENCY_KEY_HEADER: Final[str] = "Idempotency-Key"
"""Canonical idempotency key header name (per the IETF httpapi draft).

Default for :attr:`InvocationMetadataMiddleware.idem_header` and the name the
generated route OpenAPI documents for idempotency-capable operations. If an app
overrides ``idem_header``, the documented header name will not match — making
the projection follow the override is a follow-up."""


@attrs.define(slots=True, frozen=True)
class InvocationMetadataMiddleware:
    """Middleware that binds invocation metadata"""

    app: ASGIApp
    """The next ASGI application."""

    ctx_dep: ExecutionContextFactory = attrs.field(kw_only=True)
    """The dependency to resolve the execution context."""

    exec_header: str = attrs.field(default="X-Request-ID", kw_only=True)
    """Header name for the execution id. Only for injection purposes."""

    corr_header: str = attrs.field(default="X-Correlation-ID", kw_only=True)
    """Header name for the correlation id."""

    caus_header: str = attrs.field(default="X-Causation-ID", kw_only=True)
    """Header name for the causation id."""

    deadline_header: str = attrs.field(default=HTTP_HEADER_DEADLINE_BUDGET, kw_only=True)
    """Header carrying the caller's remaining time budget in seconds."""

    bind_deadline_from_header: bool = attrs.field(default=False, kw_only=True)
    """Honor the deadline-budget header (opt-in: any client can send it).

    Low-risk by construction — binding is tighten-only, so a forged value can
    only shorten the sender's own request, never extend a deadline — but kept
    opt-in so honoring caller budgets is a declared trust decision. Enable for
    service-to-service surfaces where callers propagate budgets (the outbound
    HTTP adapter attaches this header automatically)."""

    idem_header: str = attrs.field(default=IDEMPOTENCY_KEY_HEADER, kw_only=True)
    """Header name for the idempotency key (canonical, per the IETF httpapi draft)."""

    # ....................... #

    @staticmethod
    def _parse_uuid_header(raw: str | None) -> UUID | None:
        """Parse an advisory UUID header, ignoring malformed values."""

        if not raw:
            return None

        try:
            return UUID(raw)

        except ValueError:
            return None

    # ....................... #

    def _decode_metadata(self, request: Request) -> InvocationMetadata:
        execution_id = uuid7()
        corr_raw = request.headers.get(self.corr_header)
        caus_raw = request.headers.get(self.caus_header)

        # The headers are advisory client input: fall back to a fresh id
        # (or no causation) instead of failing the request on garbage values.
        correlation_id = self._parse_uuid_header(corr_raw) or uuid7()
        causation_id = self._parse_uuid_header(caus_raw)

        return InvocationMetadata(
            execution_id=execution_id,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    # ....................... #

    def _encode_metadata(
        self,
        headers: list[tuple[bytes, bytes]],
        metadata: InvocationMetadata,
    ) -> list[tuple[bytes, bytes]]:
        _headers = list(headers)

        _headers.extend(
            [
                (
                    self.exec_header.encode("latin-1"),
                    str(metadata.execution_id).encode("latin-1"),
                ),
                (
                    self.corr_header.encode("latin-1"),
                    str(metadata.correlation_id).encode("latin-1"),
                ),
            ]
        )

        if metadata.causation_id is not None:
            _headers.append(
                (
                    self.caus_header.encode("latin-1"),
                    str(metadata.causation_id).encode("latin-1"),
                )
            )

        return _headers

    # ....................... #

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)
        ctx = self.ctx_dep()
        # Forze binds its own invocation envelope (request/correlation/causation ids) below. W3C
        # trace-context (``traceparent``) extraction is intentionally NOT done here: add the standard
        # ``opentelemetry-instrumentation-fastapi`` for inbound HTTP — it creates the server span and
        # establishes the upstream context, which the operation span then nests under. Doing it here too
        # would re-parent past that server span. (Forze does propagate trace context where OTel cannot:
        # the async outbox→inbox envelope and outbound HTTP — see ``tracing.propagation``.)
        metadata = self._decode_metadata(request)
        idempotency_key = request.headers.get(self.idem_header) or None
        budget = (
            _parse_budget_header(request.headers.get(self.deadline_header))
            if self.bind_deadline_from_header
            else None
        )

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers = self._encode_metadata(headers, metadata)
                message["headers"] = headers

            await send(message)

        with (
            ctx.inv_ctx.bind_metadata(metadata=metadata),
            ctx.inv_ctx.bind_idempotency(idempotency_key),
            # None is a no-op passthrough; a bound budget is tighten-only.
            ctx.inv_ctx.bind_deadline(budget),
        ):
            await self.app(scope, receive, send_wrapper)

# ....................... #


def _parse_budget_header(value: str | None) -> float | None:
    """Best-effort positive-seconds parse; malformed values are ignored."""

    if value is None:
        return None

    try:
        budget = float(value)

    except ValueError:
        return None

    if budget <= 0 or budget != budget or budget == float("inf"):
        return None

    return budget
