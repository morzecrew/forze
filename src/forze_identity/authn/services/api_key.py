import base64
import hashlib
import hmac
from datetime import timedelta

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import current_entropy_source

# ----------------------- #


def _validate_prefix(prefix: str) -> None:
    """Reject prefixes that would not survive header transport.

    Ingress parses ``prefix:key`` by splitting on the first ``:``, so a prefix
    that is empty or contains whitespace or a ``:`` would corrupt the presented
    credentials.
    """

    if not prefix or any(ch.isspace() for ch in prefix) or ":" in prefix:
        raise exc.configuration(
            "API key prefix must be non-empty and contain no whitespace or ':'",
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ApiKeyConfig:
    """API key configuration."""

    prefix: str | None = attrs.field(default=None)
    """Optional API key prefix (e.g. ``"sk"``); validated non-empty, no whitespace."""

    length: int = 32
    """Length of the random key material."""

    expires_in: timedelta | None = attrs.field(default=None)
    """Time until issued API keys expire, if applicable."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.expires_in is not None and self.expires_in.total_seconds() <= 0:
            raise exc.configuration("Expires in must be positive")

        if self.prefix is not None:
            _validate_prefix(self.prefix)


# ....................... #


@attrs.define(slots=True, kw_only=True)
class ApiKeyService:
    """API key generation and verification service."""

    pepper: bytes = attrs.field(repr=False, validator=attrs.validators.min_len(32))
    config: ApiKeyConfig = attrs.field(factory=ApiKeyConfig)

    # ....................... #

    def generate_key(self, *, prefix: str | None = None) -> str | tuple[str, str]:
        raw = current_entropy_source().random_bytes(self.config.length)
        key = base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

        actual_prefix = self.config.prefix if prefix is None else prefix

        if actual_prefix is None:
            return key

        _validate_prefix(actual_prefix)

        return actual_prefix, key

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
