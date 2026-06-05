from datetime import timedelta
from typing import final

import attrs

from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class InngestConfig:
    """Configuration for :class:`~forze_inngest.kernel.client.client.InngestClient`."""

    is_production: bool | None = None
    """When ``True``, use Inngest Cloud defaults and signing verification."""

    event_key: str | None = None
    """Inngest event key (overrides ``INNGEST_EVENT_KEY``)."""

    signing_key: str | None = None
    """Inngest signing key (overrides ``INNGEST_SIGNING_KEY``)."""

    request_timeout: timedelta | None = attrs.field(default=None)
    """HTTP request timeout for the Inngest SDK client."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if (
            self.request_timeout is not None
            and self.request_timeout.total_seconds() <= 0
        ):
            raise exc.configuration("Request timeout must be positive")
