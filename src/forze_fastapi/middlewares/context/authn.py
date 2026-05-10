from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Literal, Sequence, final, get_args

import attrs
from fastapi import Request

from forze.application.contracts.authn import (
    ApiKeyCredentials,
    AuthnDepKey,
    AuthnIdentity,
    AuthnSpec,
    TokenCredentials,
)
from forze.application.execution import ExecutionContext
from forze.base.errors import AuthenticationError, CoreError

from .ports import AuthnIdentityResolverPort

# ----------------------- #

AuthnTrySource = Literal["token", "api_key"]
"""Which raw HTTP credential bucket to try."""

MultipleCredentialPolicy = Literal["first_in_order", "reject"]
"""How to behave when more than one credential source is present on the request."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class HeaderAuthnIdentityResolver(AuthnIdentityResolverPort):
    """Authenticate HTTP requests from bearer-token and/or API-key headers."""

    spec: AuthnSpec
    """Authn provider spec used to resolve the configured authentication port."""

    token_header: str = "Authorization"
    """Header carrying a bearer token."""

    api_key_header: str = "X-API-Key"
    """Header carrying an API key."""

    required: bool = False
    """Whether missing credentials should raise :class:`AuthenticationError`."""

    try_sources: set[AuthnTrySource] | Sequence[AuthnTrySource] = ("token", "api_key")
    """Order used when both ``token`` and ``api_key`` raw material may be present."""

    when_multiple_credentials: MultipleCredentialPolicy = "first_in_order"
    """``reject`` raises if both token and API key material are present; ``first_in_order`` picks the first hit in :attr:`try_sources`."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.try_sources:
            raise CoreError("try_sources must be non-empty")

        valid_sources = set(get_args(AuthnTrySource))

        if not valid_sources.issuperset(set(self.try_sources)):
            raise CoreError(
                "Invalid authn sources. Valid sources are: " + ", ".join(valid_sources)
            )

    # ....................... #

    async def resolve(
        self,
        request: Request,
        ctx: ExecutionContext,
    ) -> AuthnIdentity | None:
        auth = ctx.dep(AuthnDepKey, route=self.spec.name)(ctx, self.spec)
        token_creds = self._token_credentials(request)
        api_key_creds = self._api_key_credentials(request)

        if token_creds is not None and api_key_creds is not None:
            if self.when_multiple_credentials == "reject":
                raise AuthenticationError(
                    "Multiple authentication credentials present",
                    code="ambiguous_credentials",
                )

        for source in self.try_sources:
            if source == "token" and token_creds is not None:
                return await auth.authenticate_with_token(token_creds)

            if source == "api_key" and api_key_creds is not None:
                return await auth.authenticate_with_api_key(api_key_creds)

        if self.required:
            raise AuthenticationError(
                "Authentication credentials are required",
                code="auth_required",
            )

        return None

    # ....................... #

    def _token_credentials(self, request: Request) -> TokenCredentials | None:
        raw = request.headers.get(self.token_header)

        if raw is None:
            return None

        # Scheme is forwarded as a routing hint; verifiers decide whether to consult it.
        scheme, token = self._split_authorization(raw)

        if token is None:
            return TokenCredentials(token=scheme)

        return TokenCredentials(token=token, scheme=scheme)

    # ....................... #

    def _api_key_credentials(self, request: Request) -> ApiKeyCredentials | None:
        raw = request.headers.get(self.api_key_header)

        if raw is None:
            return None

        # Optional ``prefix:key`` shape; verifiers reading the prefix can use the hint.
        prefix, key = self._split_authorization(raw, sep=":")

        if key is None:
            return ApiKeyCredentials(key=prefix)

        return ApiKeyCredentials(key=key, prefix=prefix)

    # ....................... #

    @staticmethod
    def _split_authorization(raw: str, sep: str = " ") -> tuple[str, str | None]:
        parts: Sequence[str] = raw.strip(sep).split(maxsplit=1)

        if len(parts) == 1:
            return parts[0], None

        return parts[0], parts[1]


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class CookieAuthnIdentityResolver(AuthnIdentityResolverPort):
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

    scheme: str | None = "Bearer"
    """Optional scheme label stored on :class:`~forze.application.contracts.authn.TokenCredentials`."""

    kind: str | None = "access"
    """Optional token kind label (for example ``access``)."""

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

        auth = ctx.dep(AuthnDepKey, route=self.spec.name)(ctx, self.spec)
        creds = TokenCredentials(
            token=str(raw).strip(),
            scheme=self.scheme,
            kind=self.kind,
        )

        return await auth.authenticate_with_token(creds)
