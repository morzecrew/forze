from ._compat import require_oidc

require_oidc()

# ....................... #

import hmac
from collections.abc import Mapping

from jwt import InvalidTokenError
from jwt import decode as jwt_decode

from forze.base.exceptions import exc

# ----------------------- #


def verify_id_token_nonce(
    claims_or_token: Mapping[str, object] | str,
    expected_nonce: str,
) -> None:
    """Bind an ``id_token`` to the nonce generated for *this* login attempt.

    :class:`OidcTokenVerifier` is stateless (``require_nonce`` is presence-only),
    so nonce *value* binding happens in the callback handler, where the per-request
    nonce stored in the server session is available. Call this with the claims (or
    the raw ``id_token``) returned by the code exchange and the session nonce.

    When given a raw token string, the payload is decoded **without signature
    verification** — this helper compares claims only. Signature verification of
    the same token still happens in the bootstrap route's ``TokenVerifierPort``;
    the binding stays sound because an injected token cannot carry the victim's
    session nonce.

    :param claims_or_token: Decoded ``id_token`` claims, or the raw compact JWT.
    :param expected_nonce: Nonce generated before the authorize redirect (see
        :func:`forze_identity.oauth.generate_nonce`) and stored in the session.
    :raises CoreException: ``authentication`` when the ``nonce`` claim is missing,
        not a string, or differs from ``expected_nonce`` (single failure shape —
        callers cannot distinguish missing from mismatched), or when a raw token
        cannot be decoded.
    """

    if isinstance(claims_or_token, str):
        try:
            claims: Mapping[str, object] = jwt_decode(  # pyright: ignore[reportUnknownMemberType]
                jwt=claims_or_token,
                options={"verify_signature": False},
            )

        except InvalidTokenError as e:
            raise exc.authentication(
                "Invalid OIDC token",
                code="invalid_oidc_token",
            ) from e

    else:
        claims = claims_or_token

    claim = claims.get("nonce")

    # hmac.compare_digest keeps the comparison constant-time so the callback
    # cannot be used as a byte-by-byte oracle for the session nonce.
    matches = (
        bool(expected_nonce)
        and isinstance(claim, str)
        and hmac.compare_digest(claim.encode(), expected_nonce.encode())
    )

    if not matches:
        raise exc.authentication(
            "OIDC nonce mismatch",
            code="oidc_nonce_mismatch",
        )
