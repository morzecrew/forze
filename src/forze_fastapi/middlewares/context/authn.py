from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Sequence, final

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
from forze.base.errors import AuthenticationError

from .ports import AuthnIdentityResolverPort

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class HeaderAuthIdentityResolver(AuthnIdentityResolverPort):
    """Authenticate HTTP requests from bearer-token or API-key headers."""

    spec: AuthnSpec
    """Authn provider spec used to resolve the configured authentication port."""

    token_header: str = "Authorization"
    """Header carrying a bearer token."""

    api_key_header: str = "X-API-Key"
    """Header carrying an API key."""

    api_key_prefix_header: str = "X-API-Key-Prefix"
    """Optional header carrying the API-key prefix."""

    required: bool = False
    """Whether missing credentials should raise :class:`AuthenticationError`."""

    # ....................... #

    async def resolve(
        self,
        request: Request,
        ctx: ExecutionContext,
    ) -> AuthnIdentity | None:
        auth = ctx.dep(AuthnDepKey, route=self.spec.name)(ctx, self.spec)
        token = self._token_credentials(request)

        if token is not None:
            return await auth.authenticate_with_token(token)

        api_key = self._api_key_credentials(request)

        if api_key is not None:
            return await auth.authenticate_with_api_key(api_key)

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

        scheme, token = self._split_authorization(raw)

        if token is None:
            return TokenCredentials(token=scheme)

        return TokenCredentials(token=token, scheme=scheme)

    # ....................... #

    def _api_key_credentials(self, request: Request) -> ApiKeyCredentials | None:
        key = request.headers.get(self.api_key_header)

        if key is None:
            return None

        return ApiKeyCredentials(
            key=key,
            prefix=request.headers.get(self.api_key_prefix_header),
        )

    # ....................... #

    @staticmethod
    def _split_authorization(raw: str) -> tuple[str, str | None]:
        parts: Sequence[str] = raw.strip().split(maxsplit=1)

        if len(parts) == 1:
            return parts[0], None

        return parts[0], parts[1]
