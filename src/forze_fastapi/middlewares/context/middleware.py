from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Callable

import attrs
from starlette.requests import Request
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from forze.application.execution import ExecutionContext

from .defaults import DefaultCallContextCodec
from .ports import CallContextCodecPort, PrincipalContextCodecPort

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class ContextBindingMiddleware:
    """
    Middleware that binds the call and principal context to the request
    and injects the call context headers into the response.
    """

    app: ASGIApp
    """The next ASGI application."""

    ctx_dep: Callable[[], ExecutionContext] = attrs.field(kw_only=True)
    """The dependency to resolve the execution context."""

    call_ctx_codec: CallContextCodecPort = attrs.field(
        kw_only=True,
        factory=DefaultCallContextCodec,
    )
    """The codec to encode and decode the call context."""

    principal_ctx_codec: PrincipalContextCodecPort | None = attrs.field(
        kw_only=True,
        default=None,
    )
    """The codec to encode and decode the principal context."""

    # ....................... #

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)
        ctx = self.ctx_dep()
        call_ctx = self.call_ctx_codec.decode(request)
        principal_ctx = None

        if self.principal_ctx_codec is not None:
            principal_ctx = self.principal_ctx_codec.decode(request)

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers = self.call_ctx_codec.encode(headers, call_ctx)
                message["headers"] = headers

            await send(message)

        with ctx.bind_call(call=call_ctx, principal=principal_ctx):
            await self.app(scope, receive, send_wrapper)
