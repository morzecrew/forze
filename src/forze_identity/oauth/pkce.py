"""PKCE helpers for OAuth 2.1 authorization-code flows."""

import base64
import hashlib
from typing import final

import attrs

from forze.base.primitives import secure_token_urlsafe

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PkcePair:
    """Generated PKCE verifier and S256 challenge."""

    code_verifier: str
    """High-entropy verifier sent to the token endpoint."""

    code_challenge: str
    """Base64url-encoded SHA-256 of the verifier (method S256)."""


# ....................... #


def generate_pkce() -> PkcePair:
    """Generate a PKCE pair suitable for OAuth 2.1 (RFC 7636)."""

    verifier = secure_token_urlsafe(48)
    digest = hashlib.sha256(verifier.encode()).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return PkcePair(code_verifier=verifier, code_challenge=challenge)
