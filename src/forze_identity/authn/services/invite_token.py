from datetime import timedelta

import attrs

from forze.base.exceptions import exc

from ._hmac_token import _HmacTokenService

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
class InviteTokenService(_HmacTokenService):
    """Password invite token generation and verification service."""

    config: InviteTokenConfig = attrs.field(factory=InviteTokenConfig)
