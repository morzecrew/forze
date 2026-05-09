import attrs

from forze.application.contracts.authn import (
    OAuth2Tokens,
    TokenCredentials,
    TokenLifecyclePort,
)
from forze.application.dto import AuthnRefreshRequestDTO, AuthnTokenResponseDTO
from forze.application.execution import Usecase

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnRefreshTokens(Usecase[AuthnRefreshRequestDTO, AuthnTokenResponseDTO]):
    """Usecase for refreshing authentication tokens."""

    token_lifecycle: TokenLifecyclePort
    """Token lifecycle port."""

    # ....................... #

    async def main(self, args: AuthnRefreshRequestDTO) -> AuthnTokenResponseDTO:
        creds = OAuth2Tokens(
            access_token=TokenCredentials(token="unused", kind="access"),  # nosec: B106
            refresh_token=TokenCredentials(
                token=args.refresh_token,
                kind="refresh",
            ),
        )

        tokens = await self.token_lifecycle.refresh_tokens(creds)
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
