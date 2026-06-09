"""HTTP client configuration."""

from datetime import timedelta
from typing import final

import attrs

from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class HttpxConfig:
    """Connection settings for :class:`~forze_http.kernel.client.client.HttpxClient`."""

    timeout: timedelta = attrs.field(default=timedelta(seconds=30))
    """HTTP client timeout."""

    follow_redirects: bool = True
    """Whether to follow redirects."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.timeout.total_seconds() <= 0:
            raise exc.configuration("Timeout must be positive")
