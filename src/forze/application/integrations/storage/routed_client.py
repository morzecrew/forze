"""Tenant-routed object-storage client base shared by S3/GCS integrations."""

from typing import Protocol

import attrs

from forze.application.contracts.tenancy.routed_client_base import (
    StructuredSecretRoutedTenantClientBase,
)

from .client import (
    ObjectStorageClientPort,
    ObjectStorageHead,
    ObjectStorageListedObject,
)
from collections.abc import Awaitable

# ----------------------- #


class _RoutedStorageInnerClient(ObjectStorageClientPort, Protocol):
    """Inner storage client whose ``close`` is a coroutine (for pool disposal)."""

    def close(self) -> Awaitable[None]: ...


# ....................... #


@attrs.define(slots=True, kw_only=True)
class RoutedObjectStorageClientBase[C: _RoutedStorageInnerClient](
    StructuredSecretRoutedTenantClientBase[C],
):
    """Forward :class:`ObjectStorageClientPort` operations to the current tenant's client.

    Backends supply ``initialize_client`` / ``credential_fingerprint`` (via the
    structured routed base) and a backend-typed ``client()``; the bucket and
    object operations below are identical across S3 and GCS and live here once.
    """

    async def health(self) -> tuple[str, bool]:
        inner = await self._get_client()

        async with inner.client():
            return await inner.health()

    async def bucket_exists(self, bucket: str) -> bool:
        inner = await self._get_client()

        async with inner.client():
            return await inner.bucket_exists(bucket)

    async def create_bucket(self, bucket: str) -> None:
        inner = await self._get_client()

        async with inner.client():
            await inner.create_bucket(bucket)

    async def ensure_bucket(self, bucket: str) -> None:
        inner = await self._get_client()

        async with inner.client():
            await inner.ensure_bucket(bucket)

    async def object_exists(self, bucket: str, key: str) -> bool:
        inner = await self._get_client()

        async with inner.client():
            return await inner.object_exists(bucket, key)

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
        inner = await self._get_client()

        async with inner.client():
            await inner.upload_bytes(
                bucket,
                key,
                data,
                content_type=content_type,
                metadata=metadata,
                tags=tags,
            )

    async def download_bytes(self, bucket: str, key: str) -> bytes:
        inner = await self._get_client()

        async with inner.client():
            return await inner.download_bytes(bucket, key)

    async def delete_object(self, bucket: str, key: str) -> None:
        inner = await self._get_client()

        async with inner.client():
            await inner.delete_object(bucket, key)

    async def list_objects(
        self,
        bucket: str,
        prefix: str | None = None,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> tuple[list[ObjectStorageListedObject], int]:
        inner = await self._get_client()

        async with inner.client():
            return await inner.list_objects(bucket, prefix, limit=limit, offset=offset)

    async def head_object(self, bucket: str, key: str) -> ObjectStorageHead:
        inner = await self._get_client()

        async with inner.client():
            return await inner.head_object(bucket, key)
