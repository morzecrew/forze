from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #


from typing import Literal

import attrs
from fastapi import Request
from starlette.types import ASGIApp, Receive, Scope, Send

from forze.application.contracts.authn import AuthnResult
from forze.application.execution.context import (
    ExecutionContext,
    ExecutionContextFactory,
)
from forze.base.exceptions import CoreException, exc

from ..exceptions import build_core_exception_response
from ..security import AuthnRequirement, resolve_authn_ingress, resolve_tenant_identity

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class SecurityContextMiddleware:

    app: ASGIApp
    """The next ASGI application."""

    ctx_dep: ExecutionContextFactory = attrs.field(kw_only=True)
    """The dependency to resolve the execution context."""

    authn: AuthnRequirement
    """Authn requirement declaration"""

    when_multiple_credentials: Literal["first_in_order", "reject"]
    """Policy for handling more than one resolver returning a non-``None`` identity."""

    trust_tenant_header: bool = attrs.field(default=False, kw_only=True)
    """Trust the raw ``X-Tenant-Id`` header when no tenancy resolver validates it.

    Default ``False`` (deny): an unvalidated header tenant is unauthenticated input.
    Enable only behind a trusted gateway that sets the header authoritatively.
    """

    # ....................... #

    async def _resolve_authn(
        self,
        request: Request,
        ctx: ExecutionContext,
    ) -> AuthnResult | None:
        results: list[AuthnResult] = []

        for x in self.authn.ingress:
            res = await resolve_authn_ingress(x, request=request, ctx=ctx)

            if res is None:
                continue

            results.append(res)

            if self.when_multiple_credentials == "first_in_order":
                return res

        if not results:
            return None

        if self.when_multiple_credentials == "reject" and len(results) > 1:
            raise exc.authentication(
                "Multiple authentication credentials present",
                code="ambiguous_credentials",
            )

        return results[0]

    # ....................... #

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)
        ctx = self.ctx_dep()

        try:
            authn_res = await self._resolve_authn(request, ctx)
            authn = authn_res.identity if authn_res is not None else None
            tenant = await resolve_tenant_identity(
                authn_res,
                request=request,
                ctx=ctx,
                trust_tenant_header=self.trust_tenant_header,
            )

        except CoreException as error:
            # This middleware runs above Starlette's ExceptionMiddleware, so the
            # registered CoreException handler never sees errors raised here.
            # Convert them to the standard JSON error response in place.
            response = build_core_exception_response(error)
            await response(scope, receive, send)
            return

        with ctx.inv_ctx.bind_identity(authn=authn, tenant=tenant):
            await self.app(scope, receive, send)
