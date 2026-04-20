from datetime import datetime
from typing import Mapping

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class UploadedObject:
    """Value object for an uploaded object."""

    filename: str
    """Original filename associated with the upload."""

    data: bytes
    """Raw bytes payload to store."""

    description: str | None = attrs.field(default=None)
    """Optional human-readable description."""

    tags: Mapping[str, str] | None = attrs.field(default=None)
    """Optional tags associated with the object."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DownloadedObject:
    """Value object for a downloaded object."""

    data: bytes
    """Raw object payload."""

    content_type: str
    """MIME content type of the downloaded data."""

    filename: str
    """Original filename associated with the downloaded data."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class StoredObject:
    """Value object for a stored object."""

    key: str
    """Opaque storage key used to retrieve the object later."""

    filename: str
    """Original filename associated with the upload."""

    description: str | None = attrs.field(default=None)
    """Optional human-readable description."""

    tags: Mapping[str, str] | None = attrs.field(default=None)
    """Optional tags associated with the object."""

    content_type: str
    """MIME content type of the stored data."""

    size: int
    """Object size in bytes."""

    created_at: datetime
    """Backend timestamp when the object was created."""
