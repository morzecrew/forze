from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Awaitable, Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

from forze.application.execution import ExecutionContext

from .defaults import DefaultCallContextResolverInjector
from .ports import (
    CallContextInjectorPort,
    CallContextResolverPort,
    PrincipalContextResolverPort,
)

# ----------------------- #


class ContextBindingMiddleware(BaseHTTPMiddleware):
    """
    Middleware that binds the call and principal context to the request
    and injects the call context headers into the response.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        ctx_dep: Callable[[], ExecutionContext],
        call_ctx_resolver: CallContextResolverPort = DefaultCallContextResolverInjector(),
        call_ctx_injector: CallContextInjectorPort = DefaultCallContextResolverInjector(),
        principal_ctx_resolver: PrincipalContextResolverPort | None = None,
    ) -> None:
        super().__init__(app)

        self.ctx_dep = ctx_dep
        self.call_ctx_resolver = call_ctx_resolver
        self.principal_ctx_resolver = principal_ctx_resolver
        self.call_ctx_injector = call_ctx_injector

    # ....................... #

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        ctx = self.ctx_dep()
        call_ctx = self.call_ctx_resolver.resolve(request)
        principal_ctx = None

        if self.principal_ctx_resolver is not None:
            principal_ctx = self.principal_ctx_resolver.resolve(request)

        response = Response(status_code=500)

        with ctx.bind_call(call=call_ctx, principal=principal_ctx):
            try:
                response = await call_next(request)

            except Exception:
                # log
                raise

            finally:
                response = self.call_ctx_injector.inject(response, call_ctx)

        return response
