"""Shared helpers for authn usecases."""

from collections.abc import Callable

from forze.application.contracts.authn import AuthnIdentity, CredentialLifetime, IssuedTokens
from forze.base.exceptions import exc

from ..dto import AuthnTokenResponseDTO

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


# ....................... #


def require_identity(
    resolver: Callable[[], AuthnIdentity | None],
) -> AuthnIdentity:
    """Pull the bound identity or raise the uniform 401 (self-service guard)."""

    identity = resolver()

    if identity is None:
        raise exc.authentication("Authentication required", code="auth_required")

    return identity


def require_own_identity(
    resolver: Callable[[], AuthnIdentity | None],
) -> AuthnIdentity:
    """Pull the bound identity, refusing a delegated one (account self-service guard).

    An agent acting for a principal holds the intersection of the two. Minting that principal's
    API key or a token for another tenant would hand it a credential that authenticates as the
    principal alone, with no actor, and so escapes the intersection; revoking the principal's keys
    or sessions, changing its password, or dropping its tenant membership acts on the principal's
    own standing. Only the principal manages its account.
    """

    identity = require_identity(resolver)

    if identity.actor is not None:
        raise exc.authorization(
            "A delegated caller cannot manage the account of the principal it acts for",
            code="delegate_denied",
        )

    return identity


def require_admin_identity(
    resolver: Callable[[], AuthnIdentity | None],
) -> AuthnIdentity:
    """Pull the bound identity, refusing a delegated one (administrative guard).

    An act on another principal is the administrator's own, never taken by an agent acting
    for one. Whether the identity may administer at all is the operation's authorization.
    """

    identity = require_identity(resolver)

    if identity.actor is not None:
        raise exc.authorization(
            "A delegated caller cannot act as an administrator",
            code="delegate_denied",
        )

    return identity
