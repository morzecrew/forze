from datetime import timedelta

import attrs

from forze.base.exceptions import exc

from ._hmac_token import _HmacTokenService

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RefreshTokenConfig:
    """Refresh token configuration."""

    length: int = 32
    """Length of the token."""

    expires_in: timedelta = timedelta(days=7)
    """Expiration time of the token."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.expires_in.total_seconds() <= 0:
            raise exc.configuration("Expires in must be positive")


# ....................... #


@attrs.define(slots=True, kw_only=True)
class RefreshTokenService(_HmacTokenService):
    """Refresh token service."""

    config: RefreshTokenConfig = attrs.field(factory=RefreshTokenConfig)
