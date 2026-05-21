import attrs

from forze.application.contracts.authn import (
    AuthnPort,
    IssuedTokens,
    PasswordCredentials,
    TokenLifecyclePort,
)
from forze.application.dto import AuthnLoginRequestDTO, AuthnTokenResponseDTO
from forze.application.execution.core import Handler

from ._utils import token_response_from_issued_tokens

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnPasswordLogin(Handler[AuthnLoginRequestDTO, AuthnTokenResponseDTO]):
    """Handler for password-based authentication login."""

    authn: AuthnPort
    """Authentication port."""

    token_lifecycle: TokenLifecyclePort
    """Token lifecycle port."""

    # ....................... #

    async def __call__(self, args: AuthnLoginRequestDTO) -> AuthnTokenResponseDTO:
        creds = PasswordCredentials(
            login=args.login,
            password=args.password,
        )

        identity = await self.authn.authenticate_with_password(creds)
        tokens: IssuedTokens = await self.token_lifecycle.issue_tokens(identity)

        return token_response_from_issued_tokens(tokens)
