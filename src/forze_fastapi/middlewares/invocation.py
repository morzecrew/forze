from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from uuid import UUID

import attrs
from fastapi import Request
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from forze.application.execution.context import (
    ExecutionContextFactory,
    InvocationMetadata,
)
from forze.base.primitives import uuid7

# ----------------------- #


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

    idem_header: str = attrs.field(default="Idempotency-Key", kw_only=True)
    """Header name for the idempotency key (canonical, per the IETF httpapi draft)."""

    # ....................... #

    def _decode_metadata(self, request: Request) -> InvocationMetadata:
        execution_id = uuid7()
        corr_raw = request.headers.get(self.corr_header)
        caus_raw = request.headers.get(self.caus_header)

        correlation_id = UUID(corr_raw) if corr_raw else uuid7()
        causation_id = UUID(caus_raw) if caus_raw else None

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
        metadata = self._decode_metadata(request)
        idempotency_key = request.headers.get(self.idem_header) or None

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers = self._encode_metadata(headers, metadata)
                message["headers"] = headers

            await send(message)

        with (
            ctx.inv_ctx.bind_metadata(metadata=metadata),
            ctx.inv_ctx.bind_idempotency(idempotency_key),
        ):
            await self.app(scope, receive, send_wrapper)
