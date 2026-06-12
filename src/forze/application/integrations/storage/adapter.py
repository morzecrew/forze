"""Shared object-storage adapter implementing the storage query and command ports."""

import asyncio
import mimetypes
import re
from collections.abc import Awaitable, Callable
from datetime import timedelta
from uuid import UUID

import attrs
import msgspec

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    is_static_named_resource,
)
from forze.application.contracts.storage.ports import (
    StorageCommandPort,
    StorageQueryPort,
)
from forze.application.contracts.storage.value_objects import (
    DownloadedObject,
    ObjectMetadata,
    PresignedUrl,
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
from forze.base.primitives import JsonDict, OnceCell, utcnow, uuid7

# ----------------------- #

default_b64_codec = AsciiB64Codec()

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ObjectStorageAdapter(StorageQueryPort, StorageCommandPort, TenancyMixin):
    """Storage adapter that persists files in an object-storage bucket.

    Implements both :class:`~forze.application.contracts.storage.StorageQueryPort`
    and :class:`~forze.application.contracts.storage.StorageCommandPort`.
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

    _bucket_cell: OnceCell[str] = attrs.field(
        factory=OnceCell,
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
        async def _factory() -> str:
            return await self.resolve_bucket(
                self.bucket_spec,
                self._tenant_id_for_resolve(),
            )

        # Only memoize tenant-independent (static) bucket names; a dynamic resolver
        # depends on the bound tenant and the adapter may be shared across tenants.
        return await self._bucket_cell.resolve(
            _factory,
            cache=is_static_named_resource(self.bucket_spec),
        )

    # ....................... #

    @property
    def bucket(self) -> str:
        """Best-effort sync access when :attr:`bucket_spec` is static."""

        if is_static_named_resource(self.bucket_spec):
            return self.bucket_spec

        resolved = self._bucket_cell.peek()

        if resolved is not None:
            return resolved

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

    def _validate_key(self, key: str) -> None:
        """Reject object keys that could escape the bucket/tenant prefix.

        Keys minted by this adapter are a validated prefix plus a generated id, so a
        well-formed key matches the same safe charset and contains no ``..`` segments.
        A ``key`` supplied to :meth:`download` / :meth:`delete` that fails this check
        (path traversal, absolute path, control characters) is rejected instead of
        being forwarded to the object store, blunting cross-object access from
        untrusted input.
        """

        if not key or not re.match(r"^[a-zA-Z0-9!\-_.*'()/]+$", key):
            raise exc.precondition(f"Invalid object storage key: {key!r}")

        if key.startswith("/") or ".." in key.split("/"):
            raise exc.precondition(f"Unsafe object storage key: {key!r}")

    # ....................... #

    async def upload(self, obj: UploadedObject) -> StoredObject:
        """Upload a file and return its stored representation."""

        filename = obj.filename
        data = obj.data
        prefix = obj.prefix
        description = obj.description
        tags = dict(obj.tags) if obj.tags else None

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
                tags=tags,
            )

        return StoredObject(
            key=key,
            filename=filename,
            description=description,
            content_type=content_type,
            size=len(data),
            created_at=now,
            tags=tags,
        )

    # ....................... #

    async def download(self, key: str) -> DownloadedObject:
        """Download an object by key and return its data with metadata."""

        self._validate_key(key)
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

    async def presign_download(
        self,
        key: str,
        *,
        expires_in: timedelta,
    ) -> PresignedUrl:
        """Mint a time-limited direct-download URL for an existing key.

        Applies the same key validation as :meth:`download` (path traversal,
        absolute paths, and control characters are rejected before anything is
        signed) and the same tenant-aware bucket resolution, then delegates the
        signing to the client. Existence is **not** checked — signing is local
        and a ``GET`` on a missing object simply fails at request time.
        """

        self._validate_key(key)
        bucket = await self._resolved_bucket()

        async with self.client.client():
            return await self.client.presign_download_url(
                bucket=bucket,
                key=key,
                expires_in=expires_in,
            )

    # ....................... #

    async def presign_upload(
        self,
        key: str,
        *,
        expires_in: timedelta,
        content_type: str | None = None,
    ) -> PresignedUrl:
        """Mint a time-limited direct-upload URL for a caller-supplied key.

        Unlike :meth:`upload`, which mints its own keys, the target *key* is
        supplied by the caller (e.g. built via :meth:`construct_key`) and runs
        through the same validation as :meth:`download` / :meth:`delete`
        before anything is signed. The bucket is tenant-resolved and ensured
        like :meth:`upload`, so the handed-out URL targets a bucket that
        exists.

        Objects uploaded through the URL bypass this adapter's metadata
        envelope — see
        :meth:`forze.application.contracts.storage.StorageCommandPort.presign_upload`.
        """

        self._validate_key(key)
        bucket = await self._resolved_bucket()

        async with self.client.client():
            await self.client.ensure_bucket(bucket)

            return await self.client.presign_upload_url(
                bucket=bucket,
                key=key,
                expires_in=expires_in,
                content_type=content_type,
            )

    # ....................... #

    async def delete(self, key: str) -> None:
        """Delete an object from the bucket by key."""

        self._validate_key(key)
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
        include_tags: bool = False,
    ) -> tuple[list[StoredObject], int]:
        """List stored objects with pagination.

        ``include_tags`` is a guarantee, not a filter: with the default
        ``False`` tags may be absent on backends that need extra calls (S3) —
        backends that get them for free (GCS) still include them via head
        metadata; with ``True`` tags are guaranteed populated and backends
        needing extra calls pay them (S3 fans out ``GetObjectTagging`` per
        listed object with bounded concurrency).
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
                include_tags=include_tags,
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

                # Tags either ride on the listed object (S3 with
                # ``include_tags=True``) or on the head metadata (GCS, which
                # round-trips them for free); prefer the listed-object tags.
                tags = dict(o.tags) if o.tags else (dict(h.tags) if h.tags else None)

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
                        tags=tags,
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
