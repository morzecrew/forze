"""HTTP client configuration."""

from datetime import timedelta
from typing import final

import attrs

from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class HttpConfig:
    """Connection settings for :class:`~forze_http.kernel.client.client.HttpClient`."""

    timeout: timedelta = attrs.field(default=timedelta(seconds=30))
    """HTTP client timeout."""

    follow_redirects: bool = False
    """Whether to follow redirects. Disabled by default because httpx only strips
    the ``Authorization`` header on cross-origin redirects — custom credential
    headers (e.g. ``X-API-Key`` from routing credentials or default headers) would
    otherwise be re-sent to whatever host a malicious 30x points at."""

    max_response_bytes: int | None = None
    """Cap on the in-memory response body size. ``None`` (default) keeps the
    previous unbounded behaviour. When set, a response whose ``Content-Length``
    exceeds the cap is refused before the body is read, and a chunked/unsized
    response is aborted once the accumulated body crosses the cap — so a large or
    attacker-influenced upstream cannot blow up app memory (×concurrent calls)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.timeout.total_seconds() <= 0:
            raise exc.configuration("Timeout must be positive")

        if self.max_response_bytes is not None and self.max_response_bytes <= 0:
            raise exc.configuration("max_response_bytes must be positive when set")
