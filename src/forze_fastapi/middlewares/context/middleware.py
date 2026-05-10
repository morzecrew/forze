from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Callable

import attrs
from starlette.requests import Request
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError
from forze.base.validators import NoneValidator

from .callctx import HeaderCallContextCodec
from .ports import (
    AuthnIdentityResolverPort,
    CallContextCodecPort,
    TenantIdentityCodecPort,
    TenantIdentityResolverPort,
)

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
        factory=HeaderCallContextCodec,
    )
    """The codec to encode and decode the call context."""

    authn_identity_resolver: AuthnIdentityResolverPort | None = attrs.field(
        kw_only=True,
        default=None,
    )
    """Resolves the authenticated identity from a request."""

    tenant_identity_codec: TenantIdentityCodecPort | None = attrs.field(
        kw_only=True,
        default=None,
    )
    """Decodes the tenant identity from a request."""

    tenant_identity_resolver: TenantIdentityResolverPort | None = attrs.field(
        kw_only=True,
        default=None,
    )
    """Resolves the tenant identity from a request."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not NoneValidator.one_or_none(
            self.tenant_identity_resolver,
            self.tenant_identity_codec,
        ):
            raise CoreError(
                "Only one of tenant_identity_resolver or tenant_identity_codec must be provided"
            )

    # ....................... #

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)
        ctx = self.ctx_dep()
        call_ctx = self.call_ctx_codec.decode(request)
        identity: AuthnIdentity | None = None
        tenant: TenantIdentity | None = None

        if self.authn_identity_resolver is not None:
            identity = await self.authn_identity_resolver.resolve(request, ctx)

        if self.tenant_identity_resolver is not None:
            tenant = await self.tenant_identity_resolver.resolve(request, ctx, identity)

        elif self.tenant_identity_codec is not None:
            tenant = self.tenant_identity_codec.decode(request)

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers = self.call_ctx_codec.encode(headers, call_ctx)
                message["headers"] = headers

            await send(message)

        with ctx.bind_call(call=call_ctx, identity=identity, tenancy=tenant):
            await self.app(scope, receive, send_wrapper)
