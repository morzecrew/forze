"""Shared peppered-HMAC opaque-token base for refresh and invite token services."""

import base64
import hashlib
import hmac
from typing import Protocol

import attrs

from forze.base.primitives import secure_random_bytes

# ----------------------- #


class TokenConfigLike(Protocol):
    """Config shape required by :class:`HmacTokenService` (random-material length)."""

    @property
    def length(self) -> int: ...


# ....................... #


@attrs.define(slots=True, kw_only=True)
class HmacTokenService:
    """Mint opaque URL-safe tokens and verify them via peppered HMAC-SHA256 digests.

    Digests of live tokens are persisted (e.g. in session or invite stores), so
    :meth:`calculate_token_digest` must stay byte-identical across releases for a
    given ``(pepper, token)`` pair.
    """

    pepper: bytes = attrs.field(repr=False, validator=attrs.validators.min_len(32))
    config: TokenConfigLike

    # ....................... #

    def generate_token(self) -> str:
        raw = secure_random_bytes(self.config.length)

        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

    # ....................... #

    def calculate_token_digest(self, token: str) -> str:
        raw = self._b64url_decode(token)

        return hmac.new(self.pepper, raw, hashlib.sha256).hexdigest()

    # ....................... #

    def verify_token(self, token: str, expected_digest: str) -> bool:
        try:
            got = self.calculate_token_digest(token)

        except ValueError:
            # Covers binascii.Error (malformed base64) and UnicodeEncodeError
            # (non-ASCII token); anything else is a programming error and raises.
            return False

        return hmac.compare_digest(got, expected_digest)

    # ....................... #

    @staticmethod
    def _b64url_decode(token: str) -> bytes:
        pad = "=" * (-len(token) % 4)

        return base64.urlsafe_b64decode((token + pad).encode("ascii"))
