import base64
import hashlib
import hmac
import secrets
from datetime import timedelta

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RefreshTokenConfig:
    """Refresh token configuration."""

    length: int = 32
    """Length of the token."""

    expires_in: timedelta = timedelta(days=7)
    """Expiration time of the token."""


# ....................... #


@attrs.define(slots=True, kw_only=True)
class RefreshTokenGateway:
    """Refresh token gateway."""

    pepper: bytes = attrs.field(validator=attrs.validators.min_len(32))
    config: RefreshTokenConfig = attrs.field(factory=RefreshTokenConfig)

    # ....................... #

    def generate_token(self) -> str:
        raw = secrets.token_bytes(self.config.length)

        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

    # ....................... #

    def calculate_token_digest(self, token: str) -> bytes:
        raw = self._b64url_decode(token)

        return hmac.new(self.pepper, raw, hashlib.sha256).digest()

    # ....................... #

    def verify_token(self, token: str, expected_digest: bytes) -> bool:
        try:
            got = self.calculate_token_digest(token)

        except Exception:
            return False

        return hmac.compare_digest(got, expected_digest)

    # ....................... #

    @staticmethod
    def _b64url_decode(token: str) -> bytes:
        pad = "=" * (len(token) % 4)

        return base64.urlsafe_b64decode((token + pad).encode("ascii"))
