import base64
import hashlib
import hmac
import secrets
from datetime import timedelta

import attrs

from forze.base.exceptions import exc

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class InviteTokenConfig:
    """Password invite token configuration."""

    length: int = 32
    """Length of the random token material."""

    expires_in: timedelta = timedelta(days=7)
    """Time until issued invites expire."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.expires_in.total_seconds() <= 0:
            raise exc.configuration("Expires in must be positive")


# ....................... #


@attrs.define(slots=True, kw_only=True)
class InviteTokenService:
    """Password invite token generation and verification service."""

    pepper: bytes = attrs.field(validator=attrs.validators.min_len(32))
    config: InviteTokenConfig = attrs.field(factory=InviteTokenConfig)

    # ....................... #

    def generate_token(self) -> str:
        raw = secrets.token_bytes(self.config.length)

        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

    # ....................... #

    def calculate_token_digest(self, token: str) -> str:
        raw = self._b64url_decode(token)

        return hmac.new(self.pepper, raw, hashlib.sha256).hexdigest()

    # ....................... #

    def verify_token(self, token: str, expected_digest: str) -> bool:
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
