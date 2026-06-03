"""HTTP client configuration."""

from datetime import timedelta
from typing import final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class HttpxConfig:
    """Connection settings for :class:`~forze_http.kernel.client.client.HttpxClient`."""

    timeout: timedelta = attrs.field(default=timedelta(seconds=30))
    """HTTP client timeout."""

    follow_redirects: bool = True
    """Whether to follow redirects."""
