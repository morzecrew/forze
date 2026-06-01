from datetime import timedelta
from typing import final

import attrs

from forze.application.integrations.storage.client import (
    ObjectStorageHead,
    ObjectStorageListedObject,
)

GCSHead = ObjectStorageHead
GCSListedObject = ObjectStorageListedObject

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
