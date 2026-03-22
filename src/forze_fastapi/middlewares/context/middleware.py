import attrs

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Callable

from starlette.requests import Request
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from forze.application.execution import ExecutionContext

from .defaults import DefaultCallContextResolverInjector
from .ports import (
    CallContextInjectorPort,
    CallContextResolverPort,
    PrincipalContextResolverPort,
)

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

    call_ctx_resolver: CallContextResolverPort = attrs.field(
        kw_only=True,
        factory=DefaultCallContextResolverInjector,
    )
    """The resolver to resolve the call context."""

    call_ctx_injector: CallContextInjectorPort = attrs.field(
        kw_only=True,
        factory=DefaultCallContextResolverInjector,
    )
    """The injector to inject the call context headers into the response."""

    principal_ctx_resolver: PrincipalContextResolverPort | None = attrs.field(
        kw_only=True,
        default=None,
    )
    """The resolver to resolve the principal context."""

    # ....................... #

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)
        ctx = self.ctx_dep()
        call_ctx = self.call_ctx_resolver.resolve(request)
        principal_ctx = None

        if self.principal_ctx_resolver is not None:
            principal_ctx = self.principal_ctx_resolver.resolve(request)

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers = self.call_ctx_injector.inject(headers, call_ctx)
                message["headers"] = headers

            await send(message)

        with ctx.bind_call(call=call_ctx, principal=principal_ctx):
            await self.app(scope, receive, send_wrapper)
