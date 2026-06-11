"""Storage query and command ports for object storage providers."""

from typing import Awaitable, Protocol, runtime_checkable

from .value_objects import DownloadedObject, StoredObject, UploadedObject

# ----------------------- #


@runtime_checkable
class StorageQueryPort(Protocol):
    """Read-only operations over object storage providers (e.g. S3-compatible services)."""

    def download(self, key: str) -> Awaitable[DownloadedObject]:
        """Download previously stored object data by key."""
        ...  # pragma: no cover

    def list(
        self,
        limit: int,
        offset: int,
        *,
        prefix: str | None = None,
        include_tags: bool = False,
    ) -> Awaitable[tuple[list[StoredObject], int]]:
        """List stored objects with pagination.

        ``include_tags`` is a **guarantee, not a filter**: with ``False``
        (default) :attr:`StoredObject.tags` may be absent on backends that
        need extra calls to fetch tags (S3) — backends that get them for free
        (GCS, mock) still include them; with ``True`` tags are guaranteed
        populated, and backends needing extra calls pay them (S3: one
        ``GetObjectTagging`` per listed object, requiring the
        ``s3:GetObjectTagging`` permission).

        :param limit: Maximum number of objects to return.
        :param offset: Offset into the result set.
        :param prefix: Optional prefix filter.
        :param include_tags: Guarantee tags are populated on results.
        :returns: A pair of results and the total count.
        """
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class StorageCommandPort(Protocol):
    """Write operations over object storage providers (e.g. S3-compatible services)."""

    def upload(self, obj: UploadedObject) -> Awaitable[StoredObject]:
        """Upload an object and return its stored metadata.

        :param obj: Uploaded object.
        """
        ...  # pragma: no cover

    def delete(self, key: str) -> Awaitable[None]:
        """Delete an object identified by ``key``."""
        ...  # pragma: no cover
