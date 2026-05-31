"""Structural protocol for S3 clients (single endpoint or tenant-routed)."""

from typing import AsyncContextManager, Awaitable, Protocol

from types_aiobotocore_s3.client import S3Client as AsyncS3Client
from types_aiobotocore_s3.type_defs import ObjectTypeDef

from .value_objects import S3Head

# ----------------------- #


class S3ClientPort(Protocol):
    """Operations implemented by :class:`S3Client` and routed variants."""

    def close(self) -> Awaitable[None]: ...  # pragma: no cover

    def client(self) -> AsyncContextManager[AsyncS3Client]: ...  # pragma: no cover

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
    ) -> Awaitable[tuple[list[ObjectTypeDef], int]]: ...  # pragma: no cover

    def head_object(
        self, bucket: str, key: str
    ) -> Awaitable[S3Head]: ...  # pragma: no cover
