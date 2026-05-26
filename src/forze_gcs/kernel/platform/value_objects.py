from datetime import datetime, timedelta
from typing import TypedDict, final

import attrs

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


@final
class GCSListedObject(TypedDict):
    """Minimal object descriptor returned by :meth:`GCSClientPort.list_objects`."""

    Key: str
    """Object key (blob name)."""


# ....................... #


@final
class GCSHead(TypedDict, total=False):
    """Metadata returned by a GCS object metadata request."""

    content_type: str
    """MIME type of the object."""

    metadata: dict[str, str]
    """Custom metadata key-value pairs."""

    size: int
    """Content length in bytes."""

    last_modified: datetime
    """Timestamp of the last modification."""

    etag: str
    """Entity tag."""
