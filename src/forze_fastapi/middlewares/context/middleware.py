from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Callable

import attrs
from starlette.requests import Request
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from forze.application.execution import ExecutionContext

from .defaults import DefaultCallContextCodec
from .ports import AuthIdentityCodecPort, AuthIdentityResolverPort, CallContextCodecPort

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class ContextBindingMiddleware:
    """
    Middleware that binds the call context and optional auth identity to the request
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

    auth_identity_codec: AuthIdentityCodecPort | None = attrs.field(
        kw_only=True,
        default=None,
    )
    """The codec to decode :class:`~forze.application.execution.AuthIdentity`."""

    auth_identity_resolver: AuthIdentityResolverPort | None = attrs.field(
        kw_only=True,
        default=None,
    )
    """Async resolver that authenticates the request into an auth identity."""

    # ....................... #

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)
        ctx = self.ctx_dep()
        call_ctx = self.call_ctx_codec.decode(request)
        identity = None

        if self.auth_identity_resolver is not None:
            identity = await self.auth_identity_resolver.resolve(request, ctx)

        elif self.auth_identity_codec is not None:
            identity = self.auth_identity_codec.decode(request)

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers = self.call_ctx_codec.encode(headers, call_ctx)
                message["headers"] = headers

            await send(message)

        with ctx.bind_call(call=call_ctx, identity=identity):
            await self.app(scope, receive, send_wrapper)
