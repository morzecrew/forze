"""Tenant-routed object-storage client base shared by S3/GCS integrations."""

from datetime import datetime, timedelta
from typing import Mapping, Protocol, Sequence

import attrs

from forze.application.contracts.storage import PresignedUrl
from forze.application.contracts.tenancy.routed_client_base import (
    StructuredSecretRoutedTenantClientBase,
)

from .client import (
    ObjectStorageClientPort,
    ObjectStorageHead,
    ObjectStorageListedObject,
    ObjectStoragePartInfo,
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

    async def download_range_bytes(
        self,
        bucket: str,
        key: str,
        *,
        start: int,
        end: int | None = None,
    ) -> tuple[bytes, str, int]:
        inner = await self._get_client()

        async with inner.client():
            return await inner.download_range_bytes(
                bucket,
                key,
                start=start,
                end=end,
            )

    async def download_bytes_conditional(
        self,
        bucket: str,
        key: str,
        *,
        if_none_match: str | None = None,
        if_modified_since: datetime | None = None,
    ) -> tuple[bytes, str] | None:
        inner = await self._get_client()

        async with inner.client():
            return await inner.download_bytes_conditional(
                bucket,
                key,
                if_none_match=if_none_match,
                if_modified_since=if_modified_since,
            )

    async def copy_object(
        self,
        bucket: str,
        src_key: str,
        dst_key: str,
    ) -> None:
        inner = await self._get_client()

        async with inner.client():
            await inner.copy_object(bucket, src_key, dst_key)

    async def put_object_tags(
        self,
        bucket: str,
        key: str,
        tags: Mapping[str, str],
    ) -> None:
        inner = await self._get_client()

        async with inner.client():
            await inner.put_object_tags(bucket, key, tags)

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
        include_tags: bool = False,
    ) -> tuple[list[ObjectStorageListedObject], int]:
        inner = await self._get_client()

        async with inner.client():
            return await inner.list_objects(
                bucket,
                prefix,
                limit=limit,
                offset=offset,
                include_tags=include_tags,
            )

    async def head_object(
        self,
        bucket: str,
        key: str,
        *,
        include_tags: bool = False,
    ) -> ObjectStorageHead:
        inner = await self._get_client()

        async with inner.client():
            return await inner.head_object(bucket, key, include_tags=include_tags)

    async def presign_download_url(
        self,
        bucket: str,
        key: str,
        *,
        expires_in: timedelta,
    ) -> PresignedUrl:
        inner = await self._get_client()

        async with inner.client():
            return await inner.presign_download_url(
                bucket,
                key,
                expires_in=expires_in,
            )

    async def presign_upload_url(
        self,
        bucket: str,
        key: str,
        *,
        expires_in: timedelta,
        content_type: str | None = None,
    ) -> PresignedUrl:
        inner = await self._get_client()

        async with inner.client():
            return await inner.presign_upload_url(
                bucket,
                key,
                expires_in=expires_in,
                content_type=content_type,
            )

    # ....................... #
    # Resumable multipart upload primitives.

    async def create_multipart_upload(
        self,
        bucket: str,
        key: str,
        *,
        content_type: str | None = None,
    ) -> str:
        inner = await self._get_client()

        async with inner.client():
            return await inner.create_multipart_upload(
                bucket,
                key,
                content_type=content_type,
            )

    async def presign_multipart_part(
        self,
        bucket: str,
        key: str,
        *,
        upload_id: str,
        part_number: int,
        expires_in: timedelta,
    ) -> PresignedUrl:
        inner = await self._get_client()

        async with inner.client():
            return await inner.presign_multipart_part(
                bucket,
                key,
                upload_id=upload_id,
                part_number=part_number,
                expires_in=expires_in,
            )

    async def list_multipart_parts(
        self,
        bucket: str,
        key: str,
        *,
        upload_id: str,
    ) -> list[ObjectStoragePartInfo]:
        inner = await self._get_client()

        async with inner.client():
            return await inner.list_multipart_parts(
                bucket,
                key,
                upload_id=upload_id,
            )

    async def complete_multipart_upload(
        self,
        bucket: str,
        key: str,
        *,
        upload_id: str,
        parts: Sequence[ObjectStoragePartInfo],
    ) -> None:
        inner = await self._get_client()

        async with inner.client():
            await inner.complete_multipart_upload(
                bucket,
                key,
                upload_id=upload_id,
                parts=parts,
            )

    async def abort_multipart_upload(
        self,
        bucket: str,
        key: str,
        *,
        upload_id: str,
    ) -> None:
        inner = await self._get_client()

        async with inner.client():
            await inner.abort_multipart_upload(
                bucket,
                key,
                upload_id=upload_id,
            )
