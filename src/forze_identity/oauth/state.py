"""``state`` and ``nonce`` generation for OAuth/OIDC authorization-code flows.

Symmetric with :func:`~forze_identity.oauth.pkce.generate_pkce`: generate before the
authorize redirect, store in the server session, consume once on the callback. No
storage port on purpose — the session *is* the storage (see the social sign-in
recipe's callback hardening checklist).
"""

from forze.base.primitives import secure_token_urlsafe

# ----------------------- #


def generate_nonce() -> str:
    """Generate an OIDC ``nonce`` (256 bits, base64url).

    Put it in the authorize URL and keep it in the session; on the callback bind
    the returned ``id_token`` to it with
    :func:`forze_identity.oidc.verify_id_token_nonce`.
    """

    return secure_token_urlsafe(32)


# ....................... #


def generate_state() -> str:
    """Generate an OAuth ``state`` value (256 bits, base64url).

    Put it in the authorize URL and keep it in the session; on the callback compare
    the returned ``state`` with :func:`hmac.compare_digest` before anything else.
    """

    return secure_token_urlsafe(32)
