from typing import Callable

import attrs

from forze.application.contracts.authn import AuthnIdentity, PasswordLifecyclePort
from forze.application.dto import AuthnChangePasswordRequestDTO
from forze.application.execution.core import Handler
from forze.base.errors import AuthenticationError

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnChangePassword(Handler[AuthnChangePasswordRequestDTO, None]):
    """Handler for changing the password of the currently authenticated identity."""

    resolver: Callable[[], AuthnIdentity | None]
    """Callable that resolves the current authenticated identity."""

    password_lifecycle: PasswordLifecyclePort
    """Password lifecycle port."""

    # ....................... #

    async def __call__(self, args: AuthnChangePasswordRequestDTO) -> None:
        identity = self.resolver()

        if identity is None:
            raise AuthenticationError(
                "Authentication required",
                code="auth_required",
            )

        await self.password_lifecycle.change_password(identity, args.new_password)
