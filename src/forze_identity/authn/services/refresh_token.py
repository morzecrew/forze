from datetime import timedelta

import attrs

from forze.base.exceptions import exc

from ._hmac_token import HmacTokenService, TokenConfigLike

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RefreshTokenConfig(TokenConfigLike):
    """Refresh token configuration."""

    length: int = 32  # type: ignore[override]
    """Length of the token."""

    expires_in: timedelta = timedelta(days=7)
    """Expiration time of the token."""

    reuse_grace: timedelta = timedelta(0)
    """How long after a rotation presenting the rotated token is an overlapping refresh, not
    reuse: refused as ``refresh_superseded`` without revoking the family, while its successor
    is unused. ``0`` treats every presentation as reuse. Keep it to seconds: a stolen token
    replayed within it raises no theft signal (it still mints nothing)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.expires_in.total_seconds() <= 0:
            raise exc.configuration("Expires in must be positive")

        if self.reuse_grace < timedelta(0) or self.reuse_grace >= self.expires_in:
            raise exc.configuration("Reuse grace must be non-negative and shorter than expires in")


# ....................... #


@attrs.define(slots=True, kw_only=True)
class RefreshTokenService(HmacTokenService):
    """Refresh token service."""

    config: RefreshTokenConfig = attrs.field(factory=RefreshTokenConfig)  # type: ignore[assignment]
