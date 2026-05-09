import attrs

from forze.application.contracts.authn import (
    AuthnPort,
    PasswordCredentials,
    TokenLifecyclePort,
)
from forze.application.dto import AuthnLoginRequestDTO, AuthnTokenResponseDTO
from forze.application.execution import Usecase

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnPasswordLogin(Usecase[AuthnLoginRequestDTO, AuthnTokenResponseDTO]):
    """Usecase for password-based authentication login."""

    authn: AuthnPort
    """Authentication port."""

    token_lifecycle: TokenLifecyclePort
    """Token lifecycle port."""

    # ....................... #

    async def main(self, args: AuthnLoginRequestDTO) -> AuthnTokenResponseDTO:
        creds = PasswordCredentials(
            login=args.login,
            password=args.password,
        )

        identity = await self.authn.authenticate_with_password(creds)
        tokens = await self.token_lifecycle.issue_tokens(identity)

        access_token = tokens.access_token.token.token
        access_token_type = tokens.access_token.token.scheme or "bearer"
        refresh_token: str | None = None

        if tokens.refresh_token:
            refresh_token = tokens.refresh_token.token.token

        return AuthnTokenResponseDTO(
            access_token=access_token,
            refresh_token=refresh_token,
            access_token_type=access_token_type,
        )
