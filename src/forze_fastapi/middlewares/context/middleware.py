from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Callable, Literal, Sequence

import attrs
from starlette.requests import Request
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.base.errors import AuthenticationError, CoreError
from forze.base.validators import NoneValidator

from .callctx import HeaderCallContextCodec
from .ports import (
    AuthnIdentityResolverPort,
    CallContextCodecPort,
    TenantIdentityCodecPort,
    TenantIdentityResolverPort,
)

# ----------------------- #


MultipleCredentialPolicy = Literal["first_in_order", "reject"]
"""How to behave when more than one configured authn resolver yielded an identity."""


# ....................... #


@attrs.define(slots=True, frozen=True)
class ContextBindingMiddleware:
    """Middleware that binds the call context, optional auth identity, and tenancy.

    Multiple :class:`AuthnIdentityResolverPort` instances can be provided to
    cover several credential sources on the same route (e.g. cookie + header
    + API key). Resolvers are tried in order; the resulting policy is governed
    by :attr:`when_multiple_credentials`:

    * ``first_in_order`` (default): take the first non-``None`` identity and
      ignore the rest. Resolvers later in the sequence are still consulted to
      surface verification errors on present-but-invalid credentials.
    * ``reject``: if more than one resolver returns a non-``None`` identity,
      raise :class:`AuthenticationError(code="ambiguous_credentials")`.
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

    authn_identity_resolvers: Sequence[AuthnIdentityResolverPort] = attrs.field(
        kw_only=True,
        factory=tuple,
    )
    """Resolvers tried (in order) to extract an authn identity from the request."""

    when_multiple_credentials: MultipleCredentialPolicy = attrs.field(
        kw_only=True,
        default="first_in_order",
    )
    """Policy for handling more than one resolver returning a non-``None`` identity."""

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

    async def _resolve_identity(
        self,
        request: Request,
        ctx: ExecutionContext,
    ) -> AuthnIdentity | None:
        if not self.authn_identity_resolvers:
            return None

        identities: list[AuthnIdentity] = []

        for resolver in self.authn_identity_resolvers:
            ident = await resolver.resolve(request, ctx)

            if ident is None:
                continue

            identities.append(ident)

            if self.when_multiple_credentials == "first_in_order":
                # Short-circuit: keep first hit; do not continue (later resolvers
                # may still raise on bad-but-present creds, but the user picked
                # "first_in_order" which means they don't want that surface).
                return ident

        if not identities:
            return None

        if self.when_multiple_credentials == "reject" and len(identities) > 1:
            raise AuthenticationError(
                "Multiple authentication credentials present",
                code="ambiguous_credentials",
            )

        return identities[0]

    # ....................... #

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)
        ctx = self.ctx_dep()
        call_ctx = self.call_ctx_codec.decode(request)
        identity: AuthnIdentity | None = await self._resolve_identity(request, ctx)
        tenant: TenantIdentity | None = None

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
