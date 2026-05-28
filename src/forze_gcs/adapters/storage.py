"""GCS-backed implementation of :class:`~forze.application.contracts.storage.StoragePort`."""

import msgspec

from forze_gcs._compat import require_gcs

require_gcs()

# ....................... #

import asyncio
import mimetypes
import re
from datetime import datetime
from typing import Callable, Mapping, final

import attrs
import magic

from forze.application.contracts.storage import (
    DownloadedObject,
    ObjectMetadata,
    StoragePort,
    StoredObject,
    UploadedObject,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import JsonDict, utcnow, uuid7

from ..kernel.platform import GCSClientPort
from .codecs import default_b64_codec, default_path_codec

# ----------------------- #


def _object_metadata_from_gcs_custom(meta: Mapping[str, str]) -> ObjectMetadata:
    """Decode :class:`ObjectMetadata` from GCS custom metadata (string values)."""

    try:
        filename = meta["filename"]
        size = int(meta["size"])
        created_at_raw = meta["created_at"]

    except KeyError as e:
        raise exc.internal("Invalid object metadata") from e

    except ValueError as e:
        raise exc.internal("Invalid object metadata") from e

    if created_at_raw.endswith("Z"):
        created_at_raw = f"{created_at_raw[:-1]}+00:00"

    created_at = datetime.fromisoformat(created_at_raw)

    return ObjectMetadata(
        filename=filename,
        created_at=created_at,
        size=size,
        description=meta.get("description"),
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GCSStorageAdapter(StoragePort, TenancyMixin):
    """Storage adapter that persists files in a GCS bucket.

    Implements :class:`~forze.application.contracts.storage.StoragePort`.
    Object keys are built from an optional tenant prefix, a user-supplied
    prefix, and a key generator (defaults to UUID v7). Filenames and
    descriptions are base-64 encoded into GCS custom metadata (lowercase keys).
    """

    client: GCSClientPort
    """GCS client."""

    bucket: str
    """GCS bucket name."""

    key_generator: Callable[[], str] = attrs.field(default=lambda: str(uuid7()))
    """Callable to generate a unique key segment."""

    # ....................... #

    def __tenant_prefix(self) -> str | None:
        tenant_id = self.require_tenant_if_aware()

        if tenant_id is not None:
            return f"tenant_{tenant_id}"

        return None

    # ....................... #

    def construct_path(self, prefix: tuple[str, ...] | str | None) -> str:
        tenant_prefix = self.__tenant_prefix()

        if isinstance(prefix, tuple):
            prefix = default_path_codec.join(*prefix)

        return default_path_codec.cond_join(tenant_prefix, prefix)

    # ....................... #

    def construct_key(self, prefix: tuple[str, ...] | str | None) -> str:
        key = self.key_generator()

        parts: tuple[str, ...]

        if prefix is None:
            parts = (key,)
        elif isinstance(prefix, str):
            parts = (prefix, key)
        else:
            parts = (*prefix, key)

        return self.construct_path(parts)

    # ....................... #

    def _validate_prefix(self, prefix: str | None) -> None:
        if prefix is None:
            return

        if not re.match(r"^[a-zA-Z0-9!\-_.*'()/]*$", prefix):
            raise exc.precondition(f"Invalid GCS prefix: {prefix}")

    # ....................... #

    async def upload(self, obj: UploadedObject) -> StoredObject:
        filename = obj.filename
        data = obj.data
        prefix = obj.prefix
        description = obj.description

        if description:
            description = default_b64_codec.dumps(description)

        self._validate_prefix(prefix)
        key = self.construct_key(prefix)

        content_type = self._guess_content_type(filename, data)
        now = utcnow()

        metadata = ObjectMetadata(
            filename=default_b64_codec.dumps(filename),
            created_at=now,
            size=len(data),
            description=description,
        )
        meta_dict: JsonDict = msgspec.to_builtins(metadata, str_keys=True)
        safe_meta = {k: str(v) for k, v in meta_dict.items() if v is not None}

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
            created_at=now,
        )

    # ....................... #

    async def download(self, key: str) -> DownloadedObject:
        async with self.client.client():
            h = await self.client.head_object(bucket=self.bucket, key=key)

            if "metadata" not in h:
                raise exc.internal("Invalid object metadata")

            try:
                meta = _object_metadata_from_gcs_custom(h["metadata"])

            except CoreException:
                raise

            except Exception as e:
                raise exc.internal("Invalid object metadata") from e

            data = await self.client.download_bytes(bucket=self.bucket, key=key)

            return DownloadedObject(
                data=data,
                content_type=str(h.get("content_type", "application/octet-stream")),
                filename=default_b64_codec.loads(meta.filename),
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
        prefix: tuple[str, ...] | str | None = None,
    ) -> tuple[list[StoredObject], int]:
        prefix = default_path_codec.join(prefix)
        self._validate_prefix(prefix)

        path = self.construct_path(prefix)

        async with self.client.client():
            await self.client.ensure_bucket(self.bucket)

            objects, total_count = await self.client.list_objects(
                bucket=self.bucket,
                prefix=path,
                limit=limit,
                offset=offset,
            )

            for o in objects:
                if "Key" not in o:
                    raise exc.internal("Invalid object key")

            heads = await asyncio.gather(
                *(
                    self.client.head_object(bucket=self.bucket, key=o["Key"])
                    for o in objects
                )
            )

            out: list[StoredObject] = []

            for o, h in zip(objects, heads, strict=True):
                if "metadata" not in h:
                    raise exc.internal("Invalid object metadata")

                try:
                    meta = _object_metadata_from_gcs_custom(h["metadata"])

                except CoreException:
                    raise

                except Exception as e:
                    raise exc.internal("Invalid object metadata") from e

                out.append(
                    StoredObject(
                        key=o["Key"],
                        filename=default_b64_codec.loads(meta.filename),
                        description=(
                            default_b64_codec.loads(meta.description)
                            if meta.description
                            else None
                        ),
                        content_type=h.get("content_type", "application/json"),
                        size=meta.size,
                        created_at=meta.created_at,
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
