"""Unified object-storage client port and value objects for S3/GCS integrations."""

from datetime import datetime
from typing import AsyncContextManager, Awaitable, Mapping, Protocol, final

import attrs

# ----------------------- #


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
