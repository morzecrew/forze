"""Storage port and object metadata TypedDicts."""

from typing import Awaitable, Protocol, runtime_checkable

from .types import DownloadedObject, StoredObject

# ----------------------- #


@runtime_checkable
class StoragePort(Protocol):
    """Abstraction over object storage providers (e.g. S3-compatible services)."""

    def upload(
        self,
        filename: str,
        data: bytes,
        description: str | None = None,
        *,
        prefix: str | None = None,
    ) -> Awaitable[StoredObject]:
        """Upload an object and return its stored metadata.

        :param filename: Original filename for the object.
        :param data: Raw bytes to store.
        :param description: Optional human-readable description.
        :param prefix: Optional key prefix (folder-like namespace).
        """
        ...  # pragma: no cover

    def download(self, key: str) -> Awaitable[DownloadedObject]:
        """Download previously stored object data by key."""
        ...  # pragma: no cover

    def delete(self, key: str) -> Awaitable[None]:
        """Delete an object identified by ``key``."""
        ...  # pragma: no cover

    def list(
        self,
        limit: int,
        offset: int,
        *,
        prefix: str | None = None,
    ) -> Awaitable[tuple[list[StoredObject], int]]:
        """List stored objects with pagination.

        :param limit: Maximum number of objects to return.
        :param offset: Offset into the result set.
        :param prefix: Optional prefix filter.
        :returns: A pair of results and the total count.
        """
        ...  # pragma: no cover
