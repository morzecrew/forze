"""Shared object-storage adapter implementing :class:`~forze.application.contracts.storage.StoragePort`."""

import asyncio
import mimetypes
import re
from collections.abc import Awaitable, Callable
from uuid import UUID

import attrs
import msgspec

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    is_static_named_resource,
)
from forze.application.contracts.storage.ports import StoragePort
from forze.application.contracts.storage.value_objects import (
    DownloadedObject,
    ObjectMetadata,
    StoredObject,
    UploadedObject,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.application.integrations.storage.client import ObjectStorageClientPort
from forze.application.integrations.storage.codec import default_path_codec
from forze.application.integrations.storage.metadata import (
    object_metadata_from_user_metadata,
)
from forze.base.codecs import AsciiB64Codec
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import JsonDict, utcnow, uuid7

# ----------------------- #

default_b64_codec = AsciiB64Codec()

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ObjectStorageAdapter(StoragePort, TenancyMixin):
    """Storage adapter that persists files in an object-storage bucket.

    Implements :class:`~forze.application.contracts.storage.StoragePort`.
    Object keys are built from an optional tenant prefix, a user-supplied
    prefix, and a key generator (defaults to UUID v7). Filenames and
    descriptions are base-64 encoded into user metadata.
    """

    client: ObjectStorageClientPort
    """Object storage client."""

    bucket_spec: NamedResourceSpec
    """Bucket name (static or tenant-scoped resolver)."""

    resolve_bucket: Callable[[NamedResourceSpec, UUID | None], Awaitable[str]]
    """Resolve :attr:`bucket_spec` to a physical bucket name."""

    _bucket_resolved: str | None = attrs.field(
        default=None,
        init=False,
        eq=False,
        repr=False,
    )

    key_generator: Callable[[], str] = attrs.field(default=lambda: str(uuid7()))
    """Callable to generate a unique key segment."""

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

        resolved = await self.resolve_bucket(
            self.bucket_spec,
            self._tenant_id_for_resolve(),
        )
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

    def _validate_prefix(self, prefix: str | None) -> None:
        if prefix is None:
            return

        if not re.match(r"^[a-zA-Z0-9!\-_.*'()/]*$", prefix):
            raise exc.precondition(f"Invalid object storage prefix: {prefix}")

    # ....................... #

    async def upload(self, obj: UploadedObject) -> StoredObject:
        """Upload a file and return its stored representation."""

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
        """Download an object by key and return its data with metadata."""

        bucket = await self._resolved_bucket()

        async with self.client.client():
            h = await self.client.head_object(bucket=bucket, key=key)

            if not h.metadata:
                raise exc.internal("Invalid object metadata")

            try:
                meta = object_metadata_from_user_metadata(dict(h.metadata))

            except CoreException:
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
        """Delete an object from the bucket by key."""

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
        """List stored objects with pagination."""

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
                if not o.key:
                    raise exc.internal("Invalid object key")

            heads = await asyncio.gather(
                *(self.client.head_object(bucket=bucket, key=o.key) for o in objects)
            )

            out: list[StoredObject] = []

            for o, h in zip(objects, heads, strict=True):
                if not h.metadata:
                    raise exc.internal("Invalid object metadata")

                try:
                    meta = object_metadata_from_user_metadata(dict(h.metadata))

                except CoreException:
                    raise

                except Exception as e:
                    raise exc.internal("Invalid object metadata") from e

                out.append(
                    StoredObject(
                        key=o.key,
                        filename=default_b64_codec.loads(meta.filename),
                        description=(
                            default_b64_codec.loads(meta.description)
                            if meta.description
                            else None
                        ),
                        content_type=h.content_type,
                        size=meta.size,
                        created_at=meta.created_at,
                    )
                )

        return out, total_count

    # ....................... #

    @staticmethod
    def _guess_content_type(filename: str, data: bytes) -> str:
        _ = data

        return _guess_content_type_from_name(filename)


# ....................... #


def _guess_content_type_from_name(filename: str) -> str:
    """Guess a MIME type from *filename* alone, defaulting to ``application/octet-stream``."""

    ct_mimetypes, _ = mimetypes.guess_type(filename)

    return ct_mimetypes or "application/octet-stream"


# ....................... #


def guess_content_type_with_magic(filename: str, data: bytes) -> str:
    """Sniff the MIME type from *data* via ``python-magic``, else guess from *filename*.

    Shared by content-sniffing object-storage adapters (S3, GCS). ``python-magic`` is
    imported lazily so core carries no hard dependency on it; on any failure (including
    the package being absent) this falls back to extension-based guessing.
    """

    try:
        import magic

        ct_magic = magic.from_buffer(data, mime=True)

        if ct_magic:
            return ct_magic

    except Exception:  # nosec B110
        pass

    return _guess_content_type_from_name(filename)
