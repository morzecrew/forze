"""Shared helpers for authn usecases."""

from forze.application.contracts.authn import CredentialLifetime, IssuedTokens
from forze.application.dto import AuthnTokenResponseDTO

# ----------------------- #


def _expires_in_seconds(lifetime: CredentialLifetime | None) -> int | None:
    if lifetime is None or lifetime.expires_in is None:
        return None

    return int(lifetime.expires_in.total_seconds())


# ....................... #


def token_response_from_issued_tokens(tokens: IssuedTokens) -> AuthnTokenResponseDTO:
    """Map an :class:`IssuedTokens` bundle onto :class:`AuthnTokenResponseDTO`.

    Both the access and refresh ``expires_in`` are emitted as integer seconds
    when the underlying lifecycle reports a lifetime, so HTTP transports can
    derive cookie ``Max-Age`` values without re-deriving them from the token
    itself.
    """

    access = tokens.access
    refresh = tokens.refresh

    access_token = access.token.token
    access_token_type = access.token.scheme
    access_expires_in = _expires_in_seconds(access.lifetime)

    refresh_token: str | None = None
    refresh_expires_in: int | None = None

    if refresh is not None:
        refresh_token = refresh.token.token
        refresh_expires_in = _expires_in_seconds(refresh.lifetime)

    return AuthnTokenResponseDTO(
        access_token=access_token,
        refresh_token=refresh_token,
        access_token_type=access_token_type,
        access_expires_in=access_expires_in,
        refresh_expires_in=refresh_expires_in,
    )
