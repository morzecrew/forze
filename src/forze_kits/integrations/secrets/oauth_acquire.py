"""The callback handoff — exchange a code, then persist, in that order and without a gap.

One function, and the order is the whole content of it: a token pair that has been issued
but not stored is a credential nobody can rotate and nobody knows exists, so nothing
observes it before the store holds it.

The window is honestly milder than the rotating store's refresh path, and worth calibrating
so the discipline is not over-copied. What a crash here wastes is an **authorization code**
— single-use, cheap, and re-minted the moment the user clicks connect again. What a crash
in the refresh path wastes is a live refresh token, and losing that bricks an idle grant
until a human re-authorizes. So this persists before it returns, and it does not need the
single-flight-and-burn machinery the refresh path has: a second consent simply issues a
fresh code, and there is no token family to revoke by racing.
"""

from collections.abc import Mapping

from forze.application.contracts.secrets import (
    RotatingCredential,
    RotatingCredentialStorePort,
    SecretRef,
)

from .oauth_client import OAuth2TokenClient

# ----------------------- #


async def complete_authorization(
    store: RotatingCredentialStorePort,
    client: OAuth2TokenClient,
    ref: SecretRef,
    *,
    code: str,
    code_verifier: str | None,
    redirect_uri: str,
    requested_scopes: Mapping[str, str] | None = None,
) -> RotatingCredential:
    """Exchange *code* for a grant and store it under *ref*, persisting before returning.

    :param store: Where the grant lives from now on. Its tenancy is ambient, so the
        credential lands in the slot of whichever tenant the calling request is bound to.
    :param client: The provider's token client.
    :param ref: The application's name for this connection — typically derived from the
        provider and the connected account.
    :param code: The single-use authorization code from the callback.
    :param code_verifier: The PKCE verifier, taken from the session where the authorize
        step stashed it. Never from the request: a request-supplied verifier proves
        nothing, because whoever intercepted the code could supply it too.
    :param redirect_uri: The registered redirect URI, identical to the authorize step's.
    :param requested_scopes: What was asked for, as ``{"scope": "a b"}``, so a provider
        granting less is visible in the stored metadata rather than only as a later
        permission error.
    :returns: The stored credential, at the version the store assigned it.
    :raises CoreException: Whatever the exchange raised, unchanged — including
        ``INVALID_GRANT_CODE`` for a code the provider refused. A failure to persist after
        a successful exchange propagates loudly rather than as something retryable: the
        code is already spent, so retrying this call cannot work, and the user must
        reconnect.
    """

    credential = await client.exchange_code(
        code=code,
        code_verifier=code_verifier,
        redirect_uri=redirect_uri,
        requested_scopes=requested_scopes,
    )

    # Nothing between these two lines, and nothing returns the credential to a caller
    # before the store holds it. `put` is unconditional by the store's own contract: a
    # user has just proven possession, so there is no earlier version worth defending.
    return await store.put(ref, credential)
