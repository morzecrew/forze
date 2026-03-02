from datetime import datetime
from typing import (
    Awaitable,
    NotRequired,
    Optional,
    Protocol,
    TypedDict,
    runtime_checkable,
)

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


class DownloadedObject(TypedDict):
    """Data and headers returned when downloading an object."""

    data: bytes
    """Raw object payload."""

    content_type: str
    """MIME content type associated with the payload."""

    filename: str
    """Filename suggested for saving the downloaded data."""


# ....................... #


@runtime_checkable
class StoragePort(Protocol):
    """Abstraction over object storage providers (e.g. S3-compatible services)."""

    def upload(
        self,
        filename: str,
        data: bytes,
        description: Optional[str] = None,
        *,
        prefix: Optional[str] = None,
    ) -> Awaitable[StoredObject]:
        """Upload an object and return its stored metadata.

        :param filename: Original filename for the object.
        :param data: Raw bytes to store.
        :param description: Optional human-readable description.
        :param prefix: Optional key prefix (folder-like namespace).
        """
        ...

    def download(self, key: str) -> Awaitable[DownloadedObject]:
        """Download previously stored object data by key."""
        ...

    def delete(self, key: str) -> Awaitable[None]:
        """Delete an object identified by ``key``."""
        ...

    def list(
        self,
        limit: int,
        offset: int,
        *,
        prefix: Optional[str] = None,
    ) -> Awaitable[tuple[list[StoredObject], int]]:
        """List stored objects with pagination.

        :param limit: Maximum number of objects to return.
        :param offset: Offset into the result set.
        :param prefix: Optional prefix filter.
        :returns: A pair of results and the total count.
        """
        ...
