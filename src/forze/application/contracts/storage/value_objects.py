from datetime import datetime
from typing import Mapping

import msgspec

# ----------------------- #


class _InternalMetadata(msgspec.Struct):
    """Optional metadata for an object."""

    filename: str
    """Original filename associated with the object."""

    created_at: datetime
    """Backend timestamp when the object was created."""

    size: int
    """Object size in bytes."""


# ....................... #


class UploadedObject(msgspec.Struct):
    """Value object for an uploaded object."""

    filename: str
    """Original filename associated with the upload."""

    data: bytes
    """Raw bytes payload to store."""

    description: str | None = None
    """Optional human-readable description."""

    tags: Mapping[str, str] | None = None
    """Optional tags associated with the object."""

    prefix: str | None = None
    """Optional key prefix (folder-like namespace)."""


# ....................... #


class DownloadedObject(msgspec.Struct):
    """Value object for a downloaded object."""

    data: bytes
    """Raw object payload."""

    content_type: str
    """MIME content type of the downloaded data."""

    filename: str
    """Original filename associated with the downloaded data."""


# ....................... #


class ObjectMetadata(_InternalMetadata):
    """Value object for an object metadata."""

    description: str | None = None
    """Optional human-readable description."""


# ....................... #


class StoredObject(_InternalMetadata):
    """Value object for a stored object."""

    key: str
    """Opaque storage key used to retrieve the object later."""

    content_type: str
    """MIME content type of the stored data."""

    tags: Mapping[str, str] | None = None
    """Optional tags associated with the object."""

    description: str | None = None
    """Optional human-readable description."""
