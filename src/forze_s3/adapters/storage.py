from forze_s3._compat import require_s3

require_s3()

# ....................... #

import mimetypes
import re
from datetime import datetime
from typing import Optional, final

from forze.application.contracts.tenant import TenantContextPort

import attrs
import magic

from forze.application.contracts.storage import (
    DownloadedObject,
    ObjectMetadata,
    StoragePort,
    StoredObject,
)
from forze.base.errors import CoreError, ValidationError
from forze.base.primitives import utcnow, uuid7
from forze.utils.codecs import AsciiB64Codec, PathCodec

from ..kernel.platform import S3Client

# ----------------------- #
#! TODO: add tenant context support on prefix level!


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class S3StorageAdapter(StoragePort):
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
        async with self.client.client():
            h = await self.client.head_object(bucket=self.bucket, key=key)

            if "metadata" not in h:
                raise CoreError("Invalid object metadata")

            try:
                meta = ObjectMetadata(**h["metadata"])

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

            out: list[StoredObject] = []

            for o in objects:
                if "Key" not in o:
                    raise CoreError("Invalid object key")

                h = await self.client.head_object(bucket=self.bucket, key=o["Key"])

                if "metadata" not in h:
                    raise CoreError("Invalid object metadata")

                try:
                    meta = ObjectMetadata(**h["metadata"])

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
            ct = magic.from_buffer(data, mime=True)

            if ct:
                return ct

        except Exception:  # nosec B110
            pass

        ct, _ = mimetypes.guess_type(filename)

        if ct:
            return ct

        return "application/octet-stream"
