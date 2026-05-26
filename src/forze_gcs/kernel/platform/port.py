"""Structural protocol for GCS clients."""

from typing import AsyncContextManager, Awaitable, Protocol

from gcloud.aio.storage import Storage

from .value_objects import GCSHead, GCSListedObject

# ----------------------- #


class GCSClientPort(Protocol):
    """Operations implemented by :class:`GCSClient`."""

    def close(self) -> Awaitable[None]: ...  # pragma: no cover

    def client(self) -> AsyncContextManager[Storage]: ...  # pragma: no cover

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
    ) -> Awaitable[None]: ...  # pragma: no cover

    def download_bytes(
        self, bucket: str, key: str
    ) -> Awaitable[bytes]: ...  # pragma: no cover

    def delete_object(
        self, bucket: str, key: str
    ) -> Awaitable[None]: ...  # pragma: no cover

    def list_objects(
        self,
        bucket: str,
        prefix: str | None = None,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Awaitable[tuple[list[GCSListedObject], int]]: ...  # pragma: no cover

    def head_object(
        self, bucket: str, key: str
    ) -> Awaitable[GCSHead]: ...  # pragma: no cover
