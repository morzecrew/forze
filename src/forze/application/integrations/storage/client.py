"""Unified object-storage client port and value objects for S3/GCS integrations."""

from datetime import datetime
from typing import AsyncContextManager, Awaitable, Mapping, Protocol, final

import attrs

from forze.base.exceptions import exc

# ----------------------- #


def normalize_list_window(limit: int | None, offset: int | None) -> tuple[int, int]:
    """Validate and default an object-listing window to ``(limit, offset)``.

    :param limit: Requested max items (``None`` means effectively unbounded).
    :param offset: Requested start offset (``None`` means ``0``).
    :returns: ``(effective_limit, effective_offset)``.
    :raises CoreException: When ``limit <= 0`` or ``offset < 0``.
    """

    if limit is not None and limit <= 0:
        raise exc.internal("limit must be > 0")

    if offset is not None and offset < 0:
        raise exc.internal("offset must be >= 0")

    return (limit if limit is not None else 10_000_000), (offset or 0)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ObjectStorageHead:
    """Metadata returned by an object head/metadata request."""

    content_type: str = "application/octet-stream"
    """MIME type of the object."""

    metadata: Mapping[str, str] = attrs.field(factory=dict[str, str])
    """User-defined metadata key-value pairs."""

    size: int = 0
    """Content length in bytes."""

    last_modified: datetime | None = None
    """Timestamp of the last modification."""

    etag: str = ""
    """Entity tag with surrounding quotes stripped when applicable."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ObjectStorageListedObject:
    """Minimal object descriptor returned by :meth:`ObjectStorageClientPort.list_objects`."""

    key: str
    """Object key (blob name)."""


# ....................... #


class ObjectStorageClientPort(Protocol):
    """Operations implemented by storage clients."""

    def close(self) -> Awaitable[None]: ...  # pragma: no cover

    def client(self) -> AsyncContextManager[object]: ...  # pragma: no cover

    def health(self) -> Awaitable[tuple[str, bool]]: ...  # pragma: no cover

    def bucket_exists(self, bucket: str) -> Awaitable[bool]: ...  # pragma: no cover

    def create_bucket(self, bucket: str) -> Awaitable[None]: ...  # pragma: no cover

    def ensure_bucket(self, bucket: str) -> Awaitable[None]: ...  # pragma: no cover

    def object_exists(
        self, bucket: str, key: str
    ) -> Awaitable[bool]: ...  # pragma: no cover

    def upload_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        tags: dict[str, str] | None = None,
    ) -> Awaitable[None]: ...  # pragma: no cover

    def download_bytes(
        self,
        bucket: str,
        key: str,
    ) -> Awaitable[bytes]: ...  # pragma: no cover

    def delete_object(
        self,
        bucket: str,
        key: str,
    ) -> Awaitable[None]: ...  # pragma: no cover

    def list_objects(
        self,
        bucket: str,
        prefix: str | None = None,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Awaitable[tuple[list[ObjectStorageListedObject], int]]: ...  # pragma: no cover

    def head_object(
        self,
        bucket: str,
        key: str,
    ) -> Awaitable[ObjectStorageHead]: ...  # pragma: no cover
