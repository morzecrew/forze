import base64
import hashlib
import hmac
import secrets
from datetime import timedelta

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ApiKeyConfig:
    """API key configuration."""

    prefix: str | None = attrs.field(default=None)
    """Optional API key prefix."""

    length: int = 32
    """Length of the random key material."""

    expires_in: timedelta | None = attrs.field(default=None)
    """Time until issued API keys expire, if applicable."""


# ....................... #


@attrs.define(slots=True, kw_only=True)
class ApiKeyGateway:
    """API key generation and verification gateway."""

    pepper: bytes = attrs.field(validator=attrs.validators.min_len(32))
    config: ApiKeyConfig = attrs.field(factory=ApiKeyConfig)

    # ....................... #

    def generate_key(self, *, prefix: str | None = None) -> str:
        raw = secrets.token_bytes(self.config.length)
        key = base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
        actual_prefix = self.config.prefix if prefix is None else prefix

        if actual_prefix is None:
            return key

        return f"{actual_prefix}.{key}"

    # ....................... #

    def calculate_key_digest(self, key: str) -> str:
        return hmac.new(self.pepper, key.encode("utf-8"), hashlib.sha256).hexdigest()

    # ....................... #

    def verify_key(self, key: str, expected_digest: str) -> bool:
        try:
            got = self.calculate_key_digest(key)

        except Exception:
            return False

        return hmac.compare_digest(got, expected_digest)
