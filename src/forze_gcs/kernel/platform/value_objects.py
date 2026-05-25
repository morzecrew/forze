from datetime import datetime
from typing import TypedDict, final

# ----------------------- #


@final
class GCSConfig(TypedDict, total=False):
    """Optional :class:`gcloud.aio.storage.Storage` configuration."""

    service_file: str
    """Path to a GCP service account JSON key file."""

    timeout: int
    """Request timeout in seconds for GCS API calls."""

    api_root: str
    """Override API root URL (alternative to ``STORAGE_EMULATOR_HOST``)."""


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
