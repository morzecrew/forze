from datetime import datetime, timedelta
from typing import Mapping, final

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
@attrs.define(slots=True, kw_only=True, frozen=True)
class GCSListedObject:
    """Minimal object descriptor returned by :meth:`GCSClientPort.list_objects`."""

    Key: str
    """Object key (blob name)."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GCSHead:
    """Metadata returned by a GCS object metadata request."""

    content_type: str = "application/octet-stream"
    """MIME type of the object."""

    metadata: Mapping[str, str] = attrs.field(factory=dict[str, str])
    """Custom metadata key-value pairs."""

    size: int = 0
    """Content length in bytes."""

    last_modified: datetime | None = None
    """Timestamp of the last modification."""

    etag: str = ""
    """Entity tag."""
