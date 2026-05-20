from typing import Callable

import attrs

from forze.application.contracts.authn import AuthnIdentity, TokenLifecyclePort
from forze.application.execution import Handler
from forze.base.errors import AuthenticationError

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnLogout(Handler[None, None]):
    """Usecase for revoking the current session(s) of an authenticated identity.

    Pulls the bound :class:`~forze.application.contracts.authn.AuthnIdentity`
    from the execution context and delegates to
    :meth:`~forze.application.contracts.authn.TokenLifecyclePort.revoke_tokens`.
    Raises :class:`AuthenticationError` when no identity is bound, so callers
    can surface a consistent 401 to clients.
    """

    resolver: Callable[[], AuthnIdentity | None]
    """Callable that resolves the current authenticated identity."""

    token_lifecycle: TokenLifecyclePort
    """Token lifecycle port."""

    # ....................... #

    async def main(self, args: None) -> None:
        _ = args

        identity = self.resolver()

        if identity is None:
            raise AuthenticationError(
                "Authentication required",
                code="auth_required",
            )

        await self.token_lifecycle.revoke_tokens(identity)
