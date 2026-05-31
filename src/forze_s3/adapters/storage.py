"""S3-backed implementation of :class:`~forze.application.contracts.storage.StoragePort`."""

import msgspec

from forze_s3._compat import require_s3

require_s3()

# ....................... #

import asyncio
import mimetypes
import re
from datetime import datetime
from typing import Callable, Mapping, final
from uuid import UUID

import attrs
import magic

from forze.application.contracts.resolution import NamedResourceSpec, is_static_named_resource
from forze.application.contracts.storage import (
    DownloadedObject,
    ObjectMetadata,
    StoragePort,
    StoredObject,
    UploadedObject,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, utcnow, uuid7

from ..kernel.client import S3ClientPort
from ..kernel.relation import resolve_s3_bucket
from .codecs import default_b64_codec, default_path_codec

# ----------------------- #


def _object_metadata_from_s3_user(meta: Mapping[str, str]) -> ObjectMetadata:
    """Decode :class:`ObjectMetadata` from S3 user metadata (all string values)."""

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
class S3StorageAdapter(StoragePort, TenancyMixin):
    """Storage adapter that persists files in an S3-compatible bucket.

    Implements :class:`~forze.application.contracts.storage.StoragePort`.
    Object keys are built from an optional tenant prefix, a user-supplied
    prefix, and a key generator to guarantee uniqueness (defaults to a UUID v7). File names and
    descriptions are base-64 encoded into S3 user metadata so they survive
    round-trips through S3 ``HeadObject``.
    """

    client: S3ClientPort
    """S3 client."""

    bucket_spec: NamedResourceSpec
    """S3 bucket name (static or tenant-scoped resolver)."""

    _bucket_resolved: str | None = attrs.field(
        default=None,
        init=False,
        eq=False,
        repr=False,
    )

    key_generator: Callable[[], str] = attrs.field(default=lambda: str(uuid7()))
    """Callable to generate a unique key. Defaults to a UUID v7 per :func:`~forze.base.primitives.uuid7`."""

    # ....................... #

    def _tenant_id_for_resolve(self) -> UUID | None:
        if self.tenant_provider is None:
            return None

        tenant = self.tenant_provider()

        if tenant is None:
            if self.tenant_aware:
                raise exc.internal("Tenant ID is required for the storage adapter")

            return None

        return tenant.tenant_id

    # ....................... #

    async def _resolved_bucket(self) -> str:
        if self._bucket_resolved is not None:
            return self._bucket_resolved

        resolved = await resolve_s3_bucket(self.bucket_spec, self._tenant_id_for_resolve())
        object.__setattr__(self, "_bucket_resolved", resolved)

        return resolved

    # ....................... #

    @property
    def bucket(self) -> str:
        """Best-effort sync access when :attr:`bucket_spec` is static."""

        if is_static_named_resource(self.bucket_spec):
            return self.bucket_spec

        if self._bucket_resolved is not None:
            return self._bucket_resolved

        raise exc.internal(
            "bucket is only available for static bucket names; await _resolved_bucket()",
        )

    # ....................... #

    def __tenant_prefix(self) -> str | None:
        """Construct a tenant prefix from attached tenant ID if any."""

        tenant_id = self.require_tenant_if_aware()

        if tenant_id is not None:
            return f"tenant_{tenant_id}"

        return None

    # ....................... #

    def construct_path(self, prefix: tuple[str, ...] | str | None) -> str:
        """Construct a path for the given prefix."""

        tenant_prefix = self.__tenant_prefix()

        if isinstance(prefix, tuple):
            prefix = default_path_codec.join(*prefix)

        return default_path_codec.cond_join(tenant_prefix, prefix)

    # ....................... #

    def construct_key(self, prefix: tuple[str, ...] | str | None) -> str:
        """Construct a unique key for the given prefix."""

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

    #! move outside (func)
    def _validate_prefix(self, prefix: str | None) -> None:
        if prefix is None:
            return

        if not re.match(r"^[a-zA-Z0-9!\-_.*'()/]*$", prefix):
            raise exc.precondition(f"Invalid S3 prefix: {prefix}")

    # ....................... #

    async def upload(self, obj: UploadedObject) -> StoredObject:
        """Upload a file to S3 and return its stored representation.

        :param obj: Uploaded object.
        :returns: A :class:`StoredObject` with the generated key and metadata.
        """

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

        bucket = await self._resolved_bucket()

        async with self.client.client():
            await self.client.ensure_bucket(bucket)

            await self.client.upload_bytes(
                bucket=bucket,
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
        """Download an object by key and return its data with metadata.

        :param key: Object key.
        :returns: A :class:`DownloadedObject` with content, type, and filename.
        :raises exc.internal: If the object metadata is missing or malformed.
        """

        bucket = await self._resolved_bucket()

        async with self.client.client():
            h = await self.client.head_object(bucket=bucket, key=key)

            if not h.metadata:
                raise exc.internal("Invalid object metadata")

            try:
                meta = _object_metadata_from_s3_user(dict(h.metadata))

            except exc:
                raise

            except Exception as e:
                raise exc.internal("Invalid object metadata") from e

            data = await self.client.download_bytes(bucket=bucket, key=key)

            return DownloadedObject(
                data=data,
                content_type=h.content_type,
                filename=default_b64_codec.loads(meta.filename),
            )

    # ....................... #

    async def delete(self, key: str) -> None:
        """Delete an object from the bucket by key.

        :param key: Object key to delete.
        """

        bucket = await self._resolved_bucket()

        async with self.client.client():
            await self.client.delete_object(bucket=bucket, key=key)

    # ....................... #

    async def list(
        self,
        limit: int,
        offset: int,
        *,
        prefix: tuple[str, ...] | str | None = None,
    ) -> tuple[list[StoredObject], int]:
        """List stored objects with pagination.

        Fetches object keys via :meth:`S3ClientPort.list_objects` and enriches each
        entry with head metadata in parallel.

        :param limit: Maximum number of objects to return.
        :param offset: Number of objects to skip.
        :param prefix: Optional key prefix filter.
        :returns: A tuple of ``(objects, total_count)``.
        """

        prefix = default_path_codec.join(prefix)
        self._validate_prefix(prefix)

        path = self.construct_path(prefix)

        bucket = await self._resolved_bucket()

        async with self.client.client():
            await self.client.ensure_bucket(bucket)

            objects, total_count = await self.client.list_objects(
                bucket=bucket,
                prefix=path,
                limit=limit,
                offset=offset,
            )

            for o in objects:
                if "Key" not in o:
                    raise exc.internal("Invalid object key")

            heads = await asyncio.gather(
                *(
                    self.client.head_object(bucket=bucket, key=o["Key"])  # type: ignore[typeddict-item]
                    for o in objects
                )
            )

            out: list[StoredObject] = []

            for o, h in zip(objects, heads, strict=True):
                if not h.metadata:
                    raise exc.internal("Invalid object metadata")

                try:
                    meta = _object_metadata_from_s3_user(dict(h.metadata))

                except exc:
                    raise

                except Exception as e:
                    raise exc.internal("Invalid object metadata") from e

                out.append(
                    StoredObject(
                        key=o["Key"],  # type: ignore[typeddict-item]
                        filename=default_b64_codec.loads(meta.filename),
                        description=(
                            default_b64_codec.loads(meta.description)
                            if meta.description
                            else None
                        ),
                        content_type=h.content_type or "application/json",
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
