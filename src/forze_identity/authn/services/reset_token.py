from datetime import timedelta

import attrs

from forze.base.exceptions import exc

from ._hmac_token import HmacTokenService, TokenConfigLike

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ResetTokenConfig(TokenConfigLike):
    """Self-service password reset token configuration.

    Resets are short-lived by design (the raw token transits an out-of-band
    delivery channel), so the default TTL is one hour — much tighter than the
    seven-day operator-issued invites.
    """

    length: int = 32  # type: ignore[override]
    """Length of the random token material."""

    expires_in: timedelta = timedelta(hours=1)
    """Time until issued reset tokens expire."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.expires_in.total_seconds() <= 0:
            raise exc.configuration("Expires in must be positive")


# ....................... #


@attrs.define(slots=True, kw_only=True)
class ResetTokenService(HmacTokenService):
    """Password reset token generation and verification service."""

    config: ResetTokenConfig = attrs.field(factory=ResetTokenConfig)  # type: ignore[assignment]
