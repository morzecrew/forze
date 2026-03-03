from datetime import datetime
from typing import NotRequired, Optional, TypedDict

# ----------------------- #


class StoredObject(TypedDict):
    """Metadata for an object returned after uploading to storage."""

    key: str
    """Opaque storage key used to retrieve the object later."""

    filename: str
    """Original filename associated with the upload."""

    description: Optional[str]
    """Optional human-friendly description."""

    content_type: str
    """MIME content type of the stored data."""

    size: int
    """Object size in bytes."""

    created_at: datetime
    """Backend timestamp when the object was created."""


# ....................... #


class ObjectMetadata(TypedDict):
    """Human-readable object metadata used in listings."""

    filename: str
    """Original filename associated with the object."""

    created_at: str
    """Formatted creation timestamp."""

    size: str
    """Formatted size (e.g. ``"42 KB"``)."""

    description: NotRequired[str]
    """Optional description if present."""


# ....................... #


class DownloadedObject(TypedDict):
    """Data and headers returned when downloading an object."""

    data: bytes
    """Raw object payload."""

    content_type: str
    """MIME content type associated with the payload."""

    filename: str
    """Filename suggested for saving the downloaded data."""
