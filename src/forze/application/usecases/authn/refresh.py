import attrs

from forze.application.contracts.authn import (
    IssuedTokens,
    RefreshTokenCredentials,
    TokenLifecyclePort,
)
from forze.application.dto import AuthnRefreshRequestDTO, AuthnTokenResponseDTO
from forze.application.execution import Usecase

from ._utils import token_response_from_issued_tokens

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnRefreshTokens(Usecase[AuthnRefreshRequestDTO, AuthnTokenResponseDTO]):
    """Usecase for refreshing authentication tokens."""

    token_lifecycle: TokenLifecyclePort
    """Token lifecycle port."""

    # ....................... #

    async def main(self, args: AuthnRefreshRequestDTO) -> AuthnTokenResponseDTO:
        creds = RefreshTokenCredentials(token=args.refresh_token)

        tokens: IssuedTokens = await self.token_lifecycle.refresh_tokens(creds)

        return token_response_from_issued_tokens(tokens)
