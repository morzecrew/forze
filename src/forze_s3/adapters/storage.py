"""S3-backed implementation of :class:`~forze.application.contracts.storage.StoragePort`."""

from forze_s3._compat import require_s3

require_s3()

# ....................... #

import asyncio
import mimetypes
import re
from datetime import datetime
from typing import Optional, final

import attrs
import magic

from forze.application.contracts.storage import (
    DownloadedObject,
    ObjectMetadata,
    StoragePort,
    StoredObject,
)
from forze.application.contracts.tenant import TenantContextPort
from forze.base.codecs import AsciiB64Codec, PathCodec
from forze.base.errors import CoreError, ValidationError
from forze.base.primitives import utcnow, uuid7

from ..kernel.platform import S3Client

# ----------------------- #
#! TODO: add tenant context support on prefix level!


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class S3StorageAdapter(StoragePort):
    """Storage adapter that persists files in an S3-compatible bucket.

    Implements :class:`~forze.application.contracts.storage.StoragePort`.
    Object keys are built from an optional tenant prefix, a user-supplied
    prefix, and a UUID v7 to guarantee uniqueness. File names and
    descriptions are base-64 encoded into S3 user metadata so they survive
    round-trips through S3 ``HeadObject``.
    """

    client: S3Client
    bucket: str
    tenant_context: Optional[TenantContextPort] = None

    # Non initable fields
    path_codec: PathCodec = attrs.field(factory=PathCodec, init=False)
    ascii_b64_codec: AsciiB64Codec = attrs.field(factory=AsciiB64Codec, init=False)

    # ....................... #

    def __build_key(self, prefix: Optional[str] = None) -> str:
        uid = str(uuid7())

        parts: list[str] = []

        if self.tenant_context is not None:
            parts.append(str(self.tenant_context.get()))

        if prefix:
            parts.append(prefix)

        parts.append(uid)

        return self.path_codec.cond_join(*parts)

    # ....................... #

    def _validate_prefix(self, prefix: Optional[str]) -> None:
        if prefix is None:
            return

        if not re.match(r"^[a-zA-Z0-9!\-_.*'()/]*$", prefix):
            raise ValidationError(f"Invalid S3 prefix: {prefix}")

    # ....................... #

    async def upload(
        self,
        filename: str,
        data: bytes,
        description: Optional[str] = None,
        *,
        prefix: Optional[str] = None,
    ) -> StoredObject:
        """Upload a file to S3 and return its stored representation.

        :param filename: Original file name (preserved in metadata).
        :param data: Raw file bytes.
        :param description: Optional human-readable description.
        :param prefix: Optional key prefix segment.
        :returns: A :class:`StoredObject` with the generated key and metadata.
        """

        self._validate_prefix(prefix)
        key = self.__build_key(prefix)
        content_type = self._guess_content_type(filename, data)
        now = utcnow().isoformat()

        metadata = ObjectMetadata(
            filename=self.ascii_b64_codec.dumps(filename),
            created_at=now,
            size=str(len(data)),
        )

        if description:
            metadata["description"] = self.ascii_b64_codec.dumps(description)

        safe_meta = {k: str(v) for k, v in metadata.items() if v is not None}

        async with self.client.client():
            await self.client.ensure_bucket(self.bucket)

            await self.client.upload_bytes(
                bucket=self.bucket,
                key=key,
                data=data,
                content_type=content_type,
                metadata=safe_meta,
            )

        return StoredObject(
            key=key,
            filename=filename,
            description=description,
            content_type=content_type,
            size=len(data),
            created_at=datetime.fromisoformat(now),
        )

    # ....................... #

    async def download(self, key: str) -> DownloadedObject:
        """Download an object by key and return its data with metadata.

        :param key: Object key.
        :returns: A :class:`DownloadedObject` with content, type, and filename.
        :raises CoreError: If the object metadata is missing or malformed.
        """

        async with self.client.client():
            h = await self.client.head_object(bucket=self.bucket, key=key)

            if "metadata" not in h:
                raise CoreError("Invalid object metadata")

            try:
                meta = ObjectMetadata(**h["metadata"])  # type: ignore[typeddict-item]

            except Exception as e:
                raise CoreError("Invalid object metadata") from e

            data = await self.client.download_bytes(bucket=self.bucket, key=key)

            return DownloadedObject(
                data=data,
                content_type=str(h["content_type"]),  # type: ignore[arg-type]
                filename=self.ascii_b64_codec.loads(meta["filename"]),
            )

    # ....................... #

    async def delete(self, key: str) -> None:
        """Delete an object from the bucket by key.

        :param key: Object key to delete.
        """

        async with self.client.client():
            await self.client.delete_object(bucket=self.bucket, key=key)

    # ....................... #

    async def list(
        self,
        limit: int,
        offset: int,
        *,
        prefix: Optional[str] = None,
    ) -> tuple[list[StoredObject], int]:
        """List stored objects with pagination.

        Fetches object keys via :meth:`S3Client.list_objects` and enriches each
        entry with head metadata in parallel.

        :param limit: Maximum number of objects to return.
        :param offset: Number of objects to skip.
        :param prefix: Optional key prefix filter.
        :returns: A tuple of ``(objects, total_count)``.
        """

        self._validate_prefix(prefix)

        parts: list[str] = []

        if self.tenant_context is not None:
            parts.append(str(self.tenant_context.get()))

        if prefix:
            parts.append(prefix)

        p = self.path_codec.cond_join(*parts)

        async with self.client.client():
            await self.client.ensure_bucket(self.bucket)

            objects, total_count = await self.client.list_objects(
                bucket=self.bucket, prefix=p, limit=limit, offset=offset
            )

            for o in objects:
                if "Key" not in o:
                    raise CoreError("Invalid object key")

            heads = await asyncio.gather(
                *(
                    self.client.head_object(bucket=self.bucket, key=o["Key"])
                    for o in objects
                )
            )

            out: list[StoredObject] = []

            for o, h in zip(objects, heads, strict=True):
                if "metadata" not in h:
                    raise CoreError("Invalid object metadata")

                try:
                    meta = ObjectMetadata(**h["metadata"])  # type: ignore[typeddict-item]

                except Exception as e:
                    raise CoreError("Invalid object metadata") from e

                out.append(
                    StoredObject(
                        key=o["Key"],
                        filename=self.ascii_b64_codec.loads(meta["filename"]),
                        description=(
                            self.ascii_b64_codec.loads(meta["description"])
                            if "description" in meta
                            else None
                        ),
                        content_type=h.get("content_type", "application/json"),
                        size=int(meta["size"]),
                        created_at=datetime.fromisoformat(meta["created_at"]),
                    )
                )

        return out, total_count

    # ....................... #

    @staticmethod
    def _guess_content_type(filename: str, data: bytes) -> str:
        try:
            ct_magic = magic.from_buffer(data, mime=True)

            if ct_magic:
                return ct_magic

        except Exception:  # nosec B110
            pass

        ct_mimetypes, _ = mimetypes.guess_type(filename)

        if ct_mimetypes:
            return ct_mimetypes

        return "application/octet-stream"
