"""Structural protocol for S3 clients (single endpoint or tenant-routed)."""

from __future__ import annotations

from typing import AsyncContextManager, Protocol

from types_aiobotocore_s3.client import S3Client as AsyncS3Client
from types_aiobotocore_s3.type_defs import ObjectTypeDef

from .client import S3Head

# ----------------------- #


class S3ClientPort(Protocol):
    """Operations implemented by :class:`S3Client` and routed variants."""

    async def close(self) -> None:
        ...  # pragma: no cover

    def client(self) -> AsyncContextManager[AsyncS3Client]:
        ...  # pragma: no cover

    async def health(self) -> tuple[str, bool]:
        ...  # pragma: no cover

    async def bucket_exists(self, bucket: str) -> bool:
        ...  # pragma: no cover

    async def create_bucket(self, bucket: str) -> None:
        ...  # pragma: no cover

    async def ensure_bucket(self, bucket: str) -> None:
        ...  # pragma: no cover

    async def object_exists(self, bucket: str, key: str) -> bool:
        ...  # pragma: no cover

    async def upload_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        tags: dict[str, str] | None = None,
    ) -> None:
        ...  # pragma: no cover

    async def download_bytes(self, bucket: str, key: str) -> bytes:
        ...  # pragma: no cover

    async def delete_object(self, bucket: str, key: str) -> None:
        ...  # pragma: no cover

    async def list_objects(
        self,
        bucket: str,
        prefix: str | None = None,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> tuple[list[ObjectTypeDef], int]:
        ...  # pragma: no cover

    async def head_object(self, bucket: str, key: str) -> S3Head:
        ...  # pragma: no cover
