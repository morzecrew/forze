from typing import final

import attrs

from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GcpKmsConfig:
    """GCP KMS optional configuration."""

    request_timeout: float | None = None
    """Per-call deadline in seconds (``None`` = the client library default)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.request_timeout is not None and self.request_timeout <= 0:
            raise exc.configuration("Request timeout must be positive")
