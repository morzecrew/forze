import attrs

from forze.application.contracts.authn import PasswordLifecyclePort
from forze.application.dto import AuthnChangePasswordRequestDTO
from forze.application.execution import Usecase
from forze.base.errors import AuthenticationError

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnChangePassword(Usecase[AuthnChangePasswordRequestDTO, None]):
    """Usecase for changing the password of the currently authenticated identity.

    Pulls the bound :class:`~forze.application.contracts.authn.AuthnIdentity`
    from the execution context and delegates to
    :meth:`~forze.application.contracts.authn.PasswordLifecyclePort.change_password`.
    Re-authentication with the current password is intentionally **not** part of
    this usecase; callers wanting strong change-password guards can compose
    :class:`~forze.application.usecases.authn.AuthnPasswordLogin` first.
    """

    password_lifecycle: PasswordLifecyclePort
    """Password lifecycle port."""

    # ....................... #

    async def main(self, args: AuthnChangePasswordRequestDTO) -> None:
        identity = self.ctx.get_authn_identity()

        if identity is None:
            raise AuthenticationError(
                "Authentication required",
                code="auth_required",
            )

        await self.password_lifecycle.change_password(identity, args.new_password)
