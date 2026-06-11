from datetime import timedelta
from typing import final

import attrs

from forze.base.exceptions import exc

# ----------------------- #

DEFAULT_TIMEOUT = timedelta(seconds=30)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GCSConfig:
    """Optional overrides for :class:`GCSClient`."""

    service_file: str | None = None
    """Path to a GCP service account JSON key file."""

    timeout: timedelta = attrs.field(default=DEFAULT_TIMEOUT)
    """Request timeout for GCS API calls."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.timeout.total_seconds() <= 0:
            raise exc.configuration("Timeout must be positive")
