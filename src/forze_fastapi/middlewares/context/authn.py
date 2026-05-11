from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Sequence, final

import attrs
from fastapi import Request

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    ApiKeyCredentials,
    AuthnDepKey,
    AuthnIdentity,
    AuthnSpec,
)
from forze.application.execution import ExecutionContext
from forze.base.errors import AuthenticationError

from .ports import AuthnIdentityResolverPort

# ----------------------- #


def _split_authorization(raw: str, sep: str = " ") -> tuple[str, str | None]:
    """Split an authorization-style header into ``(scheme, value)`` (or ``(value, None)``)."""

    parts: Sequence[str] = raw.strip(sep).split(maxsplit=1)

    if len(parts) == 1:
        return parts[0], None

    return parts[0], parts[1]


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class HeaderTokenAuthnIdentityResolver(AuthnIdentityResolverPort):
    """Authenticate HTTP requests from a bearer-token header.

    Returns ``None`` when the configured header is absent. Raises
    :class:`AuthenticationError` when the header is present but the underlying
    verification fails (so the middleware never silently swallows bad
    credentials), or when ``required=True`` and the header is missing.
    """

    spec: AuthnSpec
    """Authn provider spec used to resolve the configured authentication port."""

    token_header: str = "Authorization"
    """Header carrying the bearer token."""

    required: bool = False
    """Whether a missing header should raise :class:`AuthenticationError`."""

    # ....................... #

    async def resolve(
        self,
        request: Request,
        ctx: ExecutionContext,
    ) -> AuthnIdentity | None:
        raw = request.headers.get(self.token_header)

        if raw is None:
            if self.required:
                raise AuthenticationError(
                    "Authentication credentials are required",
                    code="auth_required",
                )

            return None

        scheme, token = _split_authorization(raw)

        if token is None:
            creds = AccessTokenCredentials(token=scheme)

        else:
            creds = AccessTokenCredentials(token=token, scheme=scheme)

        auth = ctx.dep(AuthnDepKey, route=self.spec.name)(ctx, self.spec)

        return await auth.authenticate_with_token(creds)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class CookieTokenAuthnIdentityResolver(AuthnIdentityResolverPort):
    """Authenticate HTTP requests from an access token stored in a cookie.

    Cookie-based access tokens are convenient for first-party apps but require
    careful **CSRF** handling for browser clients (``SameSite``, anti-CSRF
    tokens, or restricting this resolver to non-browser flows). Prefer header
    bearer tokens for third-party API access unless you understand the threat
    model.
    """

    spec: AuthnSpec
    """Authn provider spec used to resolve the configured authentication port."""

    cookie_name: str = "access_token"
    """Cookie name carrying the access token."""

    scheme: str = "Bearer"
    """Scheme label stored on :class:`AccessTokenCredentials`."""

    required: bool = False
    """Whether a missing cookie should raise :class:`AuthenticationError`."""

    # ....................... #

    async def resolve(
        self,
        request: Request,
        ctx: ExecutionContext,
    ) -> AuthnIdentity | None:
        raw = request.cookies.get(self.cookie_name)

        if raw is None or not str(raw).strip():
            if self.required:
                raise AuthenticationError(
                    "Authentication cookie is required",
                    code="auth_required",
                )

            return None

        creds = AccessTokenCredentials(
            token=str(raw).strip(),
            scheme=self.scheme,
        )

        auth = ctx.dep(AuthnDepKey, route=self.spec.name)(ctx, self.spec)

        return await auth.authenticate_with_token(creds)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class HeaderApiKeyAuthnIdentityResolver(AuthnIdentityResolverPort):
    """Authenticate HTTP requests from an API key header.

    The header value supports both ``key`` and ``prefix:key`` forms; verifiers
    that read the prefix can use it as a routing hint.
    """

    spec: AuthnSpec
    """Authn provider spec used to resolve the configured authentication port."""

    api_key_header: str = "X-API-Key"
    """Header carrying the API key."""

    required: bool = False
    """Whether a missing header should raise :class:`AuthenticationError`."""

    # ....................... #

    async def resolve(
        self,
        request: Request,
        ctx: ExecutionContext,
    ) -> AuthnIdentity | None:
        raw = request.headers.get(self.api_key_header)

        if raw is None:
            if self.required:
                raise AuthenticationError(
                    "Authentication credentials are required",
                    code="auth_required",
                )

            return None

        prefix, key = _split_authorization(raw, sep=":")

        if key is None:
            creds = ApiKeyCredentials(key=prefix)

        else:
            creds = ApiKeyCredentials(key=key, prefix=prefix)

        auth = ctx.dep(AuthnDepKey, route=self.spec.name)(ctx, self.spec)

        return await auth.authenticate_with_api_key(creds)
