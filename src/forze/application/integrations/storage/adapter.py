"""Shared object-storage adapter implementing the storage query and command ports."""

import asyncio
import builtins
import mimetypes
import re
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import suppress
from datetime import datetime, timedelta
from uuid import UUID

import attrs

from forze.application.contracts.crypto import (
    BytesCipherPort,
    ChunkedStreamOpener,
    StreamingBytesCipherPort,
)
from forze.application.contracts.resolution import (
    NamedResourceSpec,
    is_static_named_resource,
    resolve_scoped_namespace,
)
from forze.application.contracts.storage.ports import (
    StorageCommandPort,
    StorageQueryPort,
    StorageUploadSessionPort,
)
from forze.application.contracts.storage.value_objects import (
    RANGE_WHOLE_PAYLOAD_UNSUPPORTED_CODE,
    DownloadedObject,
    ObjectHead,
    ObjectMetadata,
    PresignedUrl,
    RangedDownload,
    StoredObject,
    StreamedDownload,
    UploadedObject,
    UploadPart,
    UploadSession,
)
from forze.application.contracts.tenancy import TenancyMixin, TenantIdentity
from forze.application.integrations.storage.client import (
    ObjectStorageClientPort,
    ObjectStorageHead,
    ObjectStorageListedObject,
    ObjectStoragePartInfo,
    ObjectStorageSSE,
    unsatisfiable_range,
    validate_range,
)
from forze.application.integrations.storage.codec import default_path_codec
from forze.application.integrations.storage.metadata import (
    object_metadata_from_user_metadata,
)
from forze.base.codecs import AsciiB64Codec
from forze.base.crypto import (
    DEFAULT_CHUNK_SIZE,
    chunk_frame_stride,
    is_chunked_envelope,
    is_envelope,
    parse_frame,
)
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import JsonDict, OnceCell, utcnow, uuid7

# ----------------------- #

default_b64_codec = AsciiB64Codec()

_DEFAULT_STREAM_PART_SIZE = 8 * 1024 * 1024
"""Default transfer part size for streaming uploads/downloads (8 MiB).

Above S3's 5 MiB minimum non-final part, and the memory ceiling of a streamed
transfer (roughly one part plus one crypto chunk). Distinct from the crypto
``chunk_size`` — this is the storage transfer granularity, not the AEAD framing."""

_HEADER_PROBE_SIZE = 8192
"""Leading bytes fetched to parse a chunked object's header + first frame prefix on a
ranged read. Comfortably covers the header (magic + key ids + wrapped DEK + chunk size)
and the ~18-byte first-frame length prefix used to derive the per-chunk stride."""

_LIST_HEAD_FANOUT = 8
"""Max concurrent ``head_object`` calls while resolving a listing page's metadata.

``list`` HEADs each object it returns; an unbounded ``gather`` over a large page (a
``size=10000`` list) would schedule that many concurrent requests at once, saturating the
connection pool and stalling every other storage op behind it. The same bound S3's tag
fan-out already uses (``GET_OBJECT_TAGGING_CONCURRENCY``)."""

_FRAME_PREFIX_MAX = 512
"""Upper bound on a frame's length-prefix region (is_final + nonce + ct-length), used
for the rare header-probe-too-small fallback when deriving the stride."""


async def _count_bytes(source: AsyncIterator[bytes], total: list[int]) -> AsyncIterator[bytes]:
    """Pass *source* through untouched while tallying its byte count into ``total[0]``."""

    async for piece in source:
        total[0] += len(piece)
        yield piece


async def _prepend(first: bytes, rest: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
    """Yield *first* (when non-empty) then the remaining pieces of *rest*."""

    if first:
        yield first

    async for piece in rest:
        yield piece


async def _aiter_once(data: bytes) -> AsyncIterator[bytes]:
    """A single-element async byte stream (feeds a fully-buffered blob to a stream API)."""

    yield data


# ....................... #


def _reject_duplicate_part_numbers(parts: Sequence[UploadPart]) -> None:
    """Reject a parts list with a repeated ``part_number`` before assembly.

    Duplicate part numbers would silently corrupt the assembled object (a later
    part overwriting an earlier one, or both being composed), so fail closed.
    """

    seen: set[int] = set()

    for part in parts:
        if part.part_number in seen:
            raise exc.validation(
                f"Duplicate part_number in complete_upload: {part.part_number}",
            )

        seen.add(part.part_number)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ObjectStorageAdapter(
    StorageQueryPort,
    StorageCommandPort,
    StorageUploadSessionPort,
    TenancyMixin,
):
    """Storage adapter that persists files in an object-storage bucket.

    Implements :class:`~forze.application.contracts.storage.StorageQueryPort`,
    :class:`~forze.application.contracts.storage.StorageCommandPort`, and
    :class:`~forze.application.contracts.storage.StorageUploadSessionPort`.
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

    cipher: BytesCipherPort | None = None
    """Optional keyring for client-side (envelope) encryption. When set, object
    bytes are encrypted before upload and decrypted after download; presigned
    URLs are refused (they bypass this adapter)."""

    sse: ObjectStorageSSE | None = None
    """Optional **server-side** (backend, at-rest) encryption request, threaded
    per-call to the client on every write/direct-upload path (upload, copy,
    move, presign-upload, multipart begin/complete). This is the *at-rest* axis:
    the backend encrypts the stored bytes (S3 SSE-S3/SSE-KMS, GCS per-object
    CMEK), the app holds no keys, so it is compatible with — and does **not**
    trigger the refusals of — :attr:`cipher` (client-side envelope). ``None``
    leaves the bucket's default encryption in effect. Set from per-route config
    by the integration factory."""

    stream_part_size: int = _DEFAULT_STREAM_PART_SIZE
    """Transfer part size for :meth:`upload_stream` / read granularity for
    :meth:`download_stream`. Must clear the backend's minimum non-final part size
    (S3's 5 MiB); the crypto ``chunk_size`` is independent. Lowerable for tests."""

    # ....................... #

    def _cipher_tenant(self) -> TenantIdentity | None:
        # Resolve through the canonical path so encryption key selection respects
        # ``tenant_aware`` (fail-closed when a tenant is required but unbound), exactly
        # like ``_encryption_aad`` — not the raw provider, which would silently route to
        # the no-tenant key.
        tenant_id = self._tenant_id_for_resolve()

        return None if tenant_id is None else TenantIdentity(tenant_id=tenant_id)

    # ....................... #

    def _encryption_aad(self, bucket: str, key: str) -> bytes:
        """Associated data binding ciphertext to its bucket, key, and tenant.

        A blob moved to a different key, bucket, or tenant fails to decrypt.
        """

        tenant_id = self._tenant_id_for_resolve()
        return f"forze.storage|{bucket}|{key}|tenant={tenant_id}".encode()

    # ....................... #

    def _reject_presign_when_encrypted(self) -> None:
        if self.cipher is not None:
            raise exc.precondition(
                "Presigned URLs are unavailable when client-side encryption is "
                "enabled: bytes transferred directly to/from the object store "
                "bypass the keyring and would be stored or served in the clear.",
            )

    # ....................... #

    def _reject_multipart_when_encrypted(self) -> None:
        if self.cipher is not None:
            raise exc.configuration(
                "Multipart upload sessions are unavailable when client-side "
                "encryption is enabled: the application never sees the part "
                "bytes (the client uploads them directly via presigned URLs), "
                "so it cannot encrypt them and the object would be stored in "
                "the clear.",
            )

    # ....................... #

    def _reject_copy_when_encrypted(self) -> None:
        if self.cipher is not None:
            raise exc.precondition(
                "Server-side copy/move is unavailable when client-side "
                "encryption is enabled: the encryption AAD binds the ciphertext "
                "to the source object key, so a copy to a new key would be "
                "undecryptable at the destination. Re-encrypt by downloading and "
                "re-uploading through this port instead.",
            )

    # ....................... #

    async def _resolved_bucket(self) -> str:
        return await resolve_scoped_namespace(
            self.bucket_spec,
            tenant_id=self._tenant_id_for_resolve(),
            cell=self._bucket_cell,
            resolver=self.resolve_bucket,
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

        return f"tenant_{tenant_id}" if tenant_id is not None else None

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

    @staticmethod
    def _validate_prefix(prefix: str | None) -> None:
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

        For a **tenant-aware** adapter the key must additionally lie within the active
        tenant's prefix. Every key this adapter mints carries that prefix
        (:meth:`construct_key` / :meth:`construct_path` prepend it), so a caller-supplied
        key that names a *different* tenant (e.g. ``tenant_<other>/xyz``) is a
        cross-tenant reference and is refused before it reaches the store. Under the
        ``tagged`` isolation tier (one shared bucket) this key check is the *only* thing
        isolating tenants on the read/delete/copy/presign/tag paths, which — unlike
        upload/list — take the key verbatim.
        """

        if not key or not re.match(r"^[a-zA-Z0-9!\-_.*'()/]+$", key):
            raise exc.precondition(f"Invalid object storage key: {key!r}")

        if key.startswith("/") or ".." in key.split("/"):
            raise exc.precondition(f"Unsafe object storage key: {key!r}")

        tenant_prefix = self.__tenant_prefix()

        if tenant_prefix is not None and not (
            key == tenant_prefix or key.startswith(f"{tenant_prefix}/")
        ):
            raise exc.precondition(
                f"Object storage key {key!r} is outside the active tenant's namespace",
                code="core.storage.key_outside_tenant",
            )

    # ....................... #

    @staticmethod
    def _key_basename(key: str) -> str:
        """Last path segment of *key* (the key itself when it has no segment)."""

        basename = key.rsplit("/", 1)[-1]

        return basename or key

    # ....................... #

    def _filename_from_metadata(
        self,
        key: str,
        metadata: Mapping[str, str],
    ) -> str:
        """Decode the filename from the object's metadata envelope.

        When the envelope is present (objects written through :meth:`upload`)
        the base-64-encoded filename is decoded. When it is absent — objects
        written through a presigned ``PUT`` carry no envelope — the filename
        falls back to the key's basename instead of raising, so the completion
        seam (raw uploads) stays downloadable.
        """

        if not metadata:
            return self._key_basename(key)

        try:
            meta = object_metadata_from_user_metadata(dict(metadata))

        except CoreException:
            raise

        except Exception as e:
            raise exc.internal("Invalid object metadata") from e

        return default_b64_codec.loads(meta.filename)

    # ....................... #

    def _description_from_metadata(
        self,
        metadata: Mapping[str, str],
    ) -> str | None:
        """Decode the description from the object's metadata envelope, if it carries one.

        Absent for objects with no envelope (a presigned ``PUT``, a streamed upload), so
        this answers ``None`` rather than raising.
        """

        if not metadata:
            return None

        try:
            meta = object_metadata_from_user_metadata(dict(metadata))

        except CoreException:
            raise

        except Exception as e:
            raise exc.internal("Invalid object metadata") from e

        return default_b64_codec.loads(meta.description) if meta.description else None

    # ....................... #

    def _created_at_from_metadata(
        self,
        metadata: Mapping[str, str],
    ) -> datetime | None:
        """Decode the *creation* time from the object's metadata envelope, if present.

        A rewrite does not create the object, and it leaves this envelope untouched — so
        reporting the current time would move every object's creation time to the sweep
        and disagree with what the next read decodes. ``None`` when there is no envelope.
        """

        if not metadata:
            return None

        try:
            meta = object_metadata_from_user_metadata(dict(metadata))

        except CoreException:
            raise

        except Exception as e:
            raise exc.internal("Invalid object metadata") from e

        return meta.created_at

    # ....................... #

    def _stored_from_head(
        self,
        key: str,
        head: ObjectStorageHead,
        listed_tags: Mapping[str, str] | None,
    ) -> StoredObject:
        """Build a :class:`StoredObject` from a listed object's head.

        Honors the metadata envelope when present (objects written through
        :meth:`upload`); for raw objects that carry **no** envelope — written
        through a presigned ``PUT`` or assembled by ``complete_upload`` — it
        falls back to honest head fields (basename for the filename, head size
        and last-modified) instead of raising, mirroring :meth:`download`.

        Tags either ride on the listed object (S3 with ``include_tags=True``)
        or on the head (GCS, which round-trips them for free); the listed-object
        tags win when present.
        """

        tags = dict(listed_tags) if listed_tags else (dict(head.tags) if head.tags else None)

        if not head.metadata:
            return StoredObject(
                key=key,
                filename=self._key_basename(key),
                description=None,
                content_type=head.content_type,
                size=head.size,
                created_at=head.last_modified or utcnow(),
                tags=tags,
            )

        try:
            meta = object_metadata_from_user_metadata(dict(head.metadata))

        except CoreException:
            raise

        except Exception as e:
            raise exc.internal("Invalid object metadata") from e

        return StoredObject(
            key=key,
            filename=default_b64_codec.loads(meta.filename),
            description=(default_b64_codec.loads(meta.description) if meta.description else None),
            content_type=head.content_type,
            size=meta.size,
            created_at=meta.created_at,
            tags=tags,
        )

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
        meta_dict: JsonDict = attrs.asdict(metadata)
        safe_meta = {
            k: v.isoformat() if isinstance(v, datetime) else str(v)
            for k, v in meta_dict.items()
            if v is not None
        }

        bucket = await self._resolved_bucket()

        # Content type and metadata are derived from the plaintext above; encrypt
        # only the bytes that hit the store. Metadata ``size`` stays the logical
        # (plaintext) size.
        payload = data

        if self.cipher is not None:
            payload = await self.cipher.encrypt(
                data,
                tenant=self._cipher_tenant(),
                aad=self._encryption_aad(bucket, key),
            )

        async with self.client.client():
            await self.client.ensure_bucket(bucket)

            await self.client.upload_bytes(
                bucket=bucket,
                key=key,
                data=payload,
                content_type=content_type,
                metadata=safe_meta,
                tags=tags,
                sse=self.sse,
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

    def _streaming_cipher(self) -> StreamingBytesCipherPort:
        """The wired cipher, asserted to support the chunked streaming path."""

        if not isinstance(self.cipher, StreamingBytesCipherPort):
            raise exc.configuration(
                "The wired cipher does not support streaming encryption; a keyring "
                "(StreamingBytesCipherPort) is required for upload_stream/download_stream.",
                code="core.storage.streaming_cipher_required",
            )

        return self.cipher

    # ....................... #

    async def overwrite_stream(
        self,
        key: str,
        chunks: AsyncIterator[bytes],
        *,
        content_type: str | None = None,
        metadata: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        if_match: str | None = None,
    ) -> StoredObject:
        """Replace the object at *key* from a stream of chunks, in bounded memory.

        The write-side counterpart of :meth:`download_stream`, and the only write that
        takes a **caller-supplied key** rather than minting one — so it is guarded like
        the other key-taking paths (:meth:`_validate_key`: a key outside the active
        tenant's prefix is refused). Re-writing *the same key* keeps the encryption AAD
        (which binds ``(bucket, key)``) valid, which is what makes an in-place
        re-encryption possible; see ``reencrypt_objects``.

        On an encrypting route the plaintext is re-sealed chunk-by-chunk under a **fresh
        data key** — that is the point: it is how a compromised key is retired. Carry the
        object's *metadata*, *content_type*, and *tags* over from a
        :meth:`head` so the round-trip preserves them.

        *if_match* (the ETag from the same :meth:`head`) makes the replace conditional at
        its **visibility point** — the multipart completion — so an object deleted or
        replaced by concurrent traffic while the stream was being uploaded is not
        clobbered (and a concurrent delete is not silently undone by recreating the
        object): the completion fails ``not_found`` / ``conflict`` instead (see the port
        contract). ``None`` keeps the unconditional replace.
        """

        self._validate_key(key)

        bucket = await self._resolved_bucket()
        resolved_type = content_type or "application/octet-stream"
        meta_map = dict(metadata) if metadata else None

        size_box = [0]
        counted = _count_bytes(chunks, size_box)

        if self.cipher is not None:
            byte_source: AsyncIterator[bytes] = self._streaming_cipher().encrypt_stream(
                counted,
                tenant=self._cipher_tenant(),
                aad=self._encryption_aad(bucket, key),
                chunk_size=chunk_size,
            )
        else:
            byte_source = counted

        async with self.client.client():
            upload_id = await self.client.create_multipart_upload(
                bucket=bucket,
                key=key,
                content_type=resolved_type,
                metadata=meta_map,
                sse=self.sse,
            )

            try:
                parts = await self._upload_stream_parts(bucket, key, upload_id, byte_source)
                await self.client.complete_multipart_upload(
                    bucket=bucket,
                    key=key,
                    upload_id=upload_id,
                    parts=parts,
                    content_type=resolved_type,
                    metadata=meta_map,
                    sse=self.sse,
                    if_match=if_match,
                )

            except BaseException:
                with suppress(Exception):
                    await self.client.abort_multipart_upload(
                        bucket=bucket, key=key, upload_id=upload_id
                    )
                raise

            if tags:
                await self.client.put_object_tags(bucket, key, tags)

        return StoredObject(
            key=key,
            filename=self._filename_from_metadata(key, meta_map or {}),
            # The carried-over envelope still holds the description and the creation time,
            # so report them rather than answering None / "now" and having a caller wipe
            # the description and re-date the object from an index, cache, or response.
            description=self._description_from_metadata(meta_map or {}),
            content_type=resolved_type,
            size=size_box[0],
            created_at=self._created_at_from_metadata(meta_map or {}) or utcnow(),
            tags=dict(tags) if tags else None,
        )

    # ....................... #

    async def upload_stream(
        self,
        chunks: AsyncIterator[bytes],
        *,
        filename: str,
        prefix: str | None = None,
        description: str | None = None,
        tags: Mapping[str, str] | None = None,
        content_type: str | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> StoredObject:
        """Stream an object to the store in bounded memory via a multipart upload.

        On an encrypting route the plaintext is sealed chunk-by-chunk (chunked-AEAD)
        before it is buffered into transfer parts; otherwise it streams through. The
        object carries no metadata envelope (like a presigned/multipart object), so a
        later read falls back to the key's basename for the filename.
        """

        self._validate_prefix(prefix)
        key = self.construct_key(prefix)
        resolved_type = content_type or self._guess_content_type(filename, b"")
        tag_map = dict(tags) if tags else None
        bucket = await self._resolved_bucket()

        # Count plaintext bytes for the returned StoredObject (the encrypted byte
        # stream is longer; the meaningful size is the logical, pre-encryption one).
        size_box = [0]
        counted = _count_bytes(chunks, size_box)

        if self.cipher is not None:
            byte_source: AsyncIterator[bytes] = self._streaming_cipher().encrypt_stream(
                counted,
                tenant=self._cipher_tenant(),
                aad=self._encryption_aad(bucket, key),
                chunk_size=chunk_size,
            )
        else:
            byte_source = counted

        async with self.client.client():
            await self.client.ensure_bucket(bucket)
            upload_id = await self.client.create_multipart_upload(
                bucket=bucket,
                key=key,
                content_type=resolved_type,
                sse=self.sse,
            )

            try:
                parts = await self._upload_stream_parts(bucket, key, upload_id, byte_source)
                await self.client.complete_multipart_upload(
                    bucket=bucket,
                    key=key,
                    upload_id=upload_id,
                    parts=parts,
                    content_type=resolved_type,
                    sse=self.sse,
                )

            except BaseException:
                with suppress(Exception):
                    await self.client.abort_multipart_upload(
                        bucket=bucket, key=key, upload_id=upload_id
                    )
                raise

            # Multipart completion carries no tagging, so — like ``overwrite_stream`` — the tags
            # are applied in a follow-up call, or a streamed upload would silently drop them (the
            # returned StoredObject would claim tags the object never actually got).
            if tag_map:
                await self.client.put_object_tags(bucket, key, tag_map)

        return StoredObject(
            key=key,
            filename=filename,
            description=description,
            content_type=resolved_type,
            size=size_box[0],
            created_at=utcnow(),
            tags=tag_map,
        )

    # ....................... #

    async def _upload_stream_parts(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        byte_source: AsyncIterator[bytes],
    ) -> list[ObjectStoragePartInfo]:
        """Buffer *byte_source* into ``stream_part_size`` parts and upload each.

        Non-final parts are exactly ``stream_part_size`` (clearing the backend
        minimum); the trailing bytes form the final part. Always uploads at least
        one part so an empty stream still completes.
        """

        parts: list[ObjectStoragePartInfo] = []
        buffer = bytearray()
        part_number = 1

        async for piece in byte_source:
            buffer.extend(piece)

            while len(buffer) >= self.stream_part_size:
                part = bytes(buffer[: self.stream_part_size])
                del buffer[: self.stream_part_size]
                parts.append(
                    await self.client.upload_multipart_part(
                        bucket=bucket,
                        key=key,
                        upload_id=upload_id,
                        part_number=part_number,
                        data=part,
                        sse=self.sse,
                    )
                )
                part_number += 1

        # The remaining bytes are the final part; upload it (or a single empty part
        # when the whole stream was empty) so completion always has ≥1 part.
        if buffer or not parts:
            parts.append(
                await self.client.upload_multipart_part(
                    bucket=bucket,
                    key=key,
                    upload_id=upload_id,
                    part_number=part_number,
                    data=bytes(buffer),
                    sse=self.sse,
                )
            )

        return parts

    # ....................... #

    async def download_stream(self, key: str) -> StreamedDownload:
        """Download an object as a bounded-memory plaintext stream (see the port doc)."""

        self._validate_key(key)
        bucket = await self._resolved_bucket()
        head = await self.head(key)

        filename = (
            self._filename_from_metadata(key, head.metadata)
            if head.metadata
            else self._key_basename(key)
        )

        raw = self._raw_ranges(bucket, key, head.size)
        body = raw if self.cipher is None else self._decrypt_ranges(raw, bucket, key)
        # The plaintext size is known only for a non-encrypted object (the stored
        # size is the raw bytes; encryption/​framing makes it larger than plaintext).
        size = head.size if self.cipher is None else None

        return StreamedDownload(
            content_type=head.content_type,
            filename=filename,
            chunks=body,
            size=size,
            etag=head.etag,
            last_modified=head.last_modified,
        )

    # ....................... #

    async def _raw_ranges(self, bucket: str, key: str, total: int) -> AsyncIterator[bytes]:
        """Yield an object's raw bytes in ``stream_part_size`` ranged GETs.

        Holds the client connection open across the whole iteration, so the caller
        must consume (or close) the stream promptly.
        """

        async with self.client.client():
            offset = 0

            while offset < total:
                end = offset + self.stream_part_size - 1
                body, _content_range, _real_total = await self.client.download_range_bytes(
                    bucket=bucket, key=key, start=offset, end=end
                )

                if not body.data:
                    break

                offset += len(body.data)
                yield body.data

    # ....................... #

    async def _decrypt_ranges(
        self,
        raw: AsyncIterator[bytes],
        bucket: str,
        key: str,
    ) -> AsyncIterator[bytes]:
        """Decrypt a raw byte stream, dispatching on its stored format.

        Peeks the leading magic: a chunked-AEAD object (``FZEc``) is decrypted
        chunk-by-chunk (bounded memory); a legacy whole-payload envelope (``FZEv``)
        is buffered and decrypted in one pass; anything else is legacy plaintext and
        passes straight through (migration tolerance, like :meth:`download`).
        """

        aad = self._encryption_aad(bucket, key)
        tail = raw.__aiter__()
        prefix = bytearray()

        async for piece in tail:
            prefix.extend(piece)
            if len(prefix) >= 4:  # both magics are 4 bytes
                break
        else:
            # Fewer than 4 bytes total: cannot be an envelope — legacy plaintext.
            if prefix:
                yield bytes(prefix)
            return

        magic = bytes(prefix[:4])

        if is_chunked_envelope(magic):
            async for plaintext in self._streaming_cipher().decrypt_stream(
                _prepend(bytes(prefix), tail),
                aad=aad,
                tenant=self._cipher_tenant(),
            ):
                yield plaintext

        elif is_envelope(magic):
            whole = bytearray(prefix)
            async for piece in tail:
                whole.extend(piece)
            yield await self.cipher.decrypt(  # type: ignore[union-attr]  # cipher set on this path
                bytes(whole), aad=aad, tenant=self._cipher_tenant()
            )

        else:
            async for piece in _prepend(bytes(prefix), tail):
                yield piece

    # ....................... #

    async def _decrypt_full(self, data: bytes, bucket: str, key: str) -> bytes:
        """Decrypt a fully-buffered object body according to its stored format.

        Recognizes both stored ciphertext shapes: a whole-payload envelope (``FZEv``)
        and a chunked-AEAD object (``FZEc``, written by :meth:`upload_stream`) — the
        latter is opened chunk-by-chunk and reassembled. A value that is neither is
        legacy plaintext, returned untouched (migration tolerance). The tenant is bound
        so the confused-deputy key-id guard runs, matching :meth:`download_stream` and
        the ranged path. A no-op when no cipher is wired.
        """

        if self.cipher is None:
            return data

        aad = self._encryption_aad(bucket, key)
        tenant = self._cipher_tenant()

        if is_chunked_envelope(data):
            out = bytearray()

            async for piece in self._streaming_cipher().decrypt_stream(
                _aiter_once(data), aad=aad, tenant=tenant
            ):
                out += piece

            return bytes(out)

        if is_envelope(data):
            return await self.cipher.decrypt(data, aad=aad, tenant=tenant)

        return data

    # ....................... #

    async def download(self, key: str) -> DownloadedObject:
        """Download an object by key and return its data with metadata.

        The body's content type and user metadata come back on the same ``GET``
        (no separate head call). When the metadata envelope is present its
        filename/description are decoded as on :meth:`upload`; when it is absent
        — an object written through a presigned ``PUT`` (the completion seam) —
        the filename falls back to the key's basename instead of raising.

        Both stored ciphertext formats decrypt transparently — a whole-payload
        envelope and a chunked object written by :meth:`upload_stream`; a legacy
        plaintext object is served as-is.
        """

        self._validate_key(key)
        bucket = await self._resolved_bucket()

        async with self.client.client():
            body = await self.client.download_bytes(bucket=bucket, key=key)

            data = await self._decrypt_full(body.data, bucket, key)

            filename = self._filename_from_metadata(key, body.metadata)

            return DownloadedObject(
                data=data,
                content_type=body.content_type,
                filename=filename,
            )

    # ....................... #

    async def head(
        self,
        key: str,
        *,
        include_tags: bool = False,
    ) -> ObjectHead:
        """Fetch an object's honest head/metadata view by key.

        Validates the key (same traversal/charset rules as :meth:`download`)
        and resolves the tenant-aware bucket, then surfaces the client's
        ``head_object`` result as a public :class:`ObjectHead`. Unlike
        :meth:`download` / :meth:`list`, this does **not** decode the metadata
        envelope, so it works for objects stored through a presigned ``PUT``
        (which carry no envelope) — the completion seam for direct uploads.

        ``include_tags`` is threaded to the client with the same guarantee
        semantics as :meth:`list` (S3 pays an extra ``GetObjectTagging`` call
        when ``True``; GCS/mock include tags for free).
        """

        self._validate_key(key)
        bucket = await self._resolved_bucket()

        async with self.client.client():
            h = await self.client.head_object(
                bucket=bucket,
                key=key,
                include_tags=include_tags,
            )

        return ObjectHead(
            content_type=h.content_type,
            size=h.size,
            etag=h.etag,
            last_modified=h.last_modified,
            metadata=dict(h.metadata),
            tags=dict(h.tags),
        )

    # ....................... #

    async def download_range(
        self,
        key: str,
        *,
        start: int,
        end: int | None = None,
    ) -> RangedDownload:
        """Download an inclusive byte range of an object.

        Validates the key and the range window (``start >= 0``, ``end >=
        start``), resolves the tenant-aware bucket, then delegates the ranged
        ``GET`` to the client. An unsatisfiable range (``start`` beyond the
        object) surfaces as a precondition error (the 416 equivalent).

        On a client-side-encrypting route, a chunked-AEAD object (written via
        :meth:`upload_stream`) is served by decrypting only the chunks the range covers
        (bounded memory); a legacy whole-payload envelope cannot be sliced and is refused
        (use :meth:`download` / :meth:`download_stream`); a plaintext object passes through.
        """

        validate_range(start, end)
        self._validate_key(key)
        bucket = await self._resolved_bucket()

        if self.cipher is not None:
            return await self._encrypted_download_range(bucket, key, start=start, end=end)

        return await self._raw_download_range(bucket, key, start=start, end=end)

    # ....................... #

    async def _raw_download_range(
        self, bucket: str, key: str, *, start: int, end: int | None
    ) -> RangedDownload:
        async with self.client.client():
            body, content_range, total = await self.client.download_range_bytes(
                bucket=bucket, key=key, start=start, end=end
            )

        return RangedDownload(
            data=body.data,
            content_type=body.content_type,
            content_range=content_range,
            total_size=total,
            filename=self._filename_from_metadata(key, body.metadata),
        )

    # ....................... #

    async def _encrypted_download_range(
        self, bucket: str, key: str, *, start: int, end: int | None
    ) -> RangedDownload:
        """Serve a ranged read against a client-side-encrypted object by stored format."""

        head = await self.head(key)
        raw_total = head.size

        if raw_total >= 4:  # need the 4-byte magic to classify the object
            async with self.client.client():
                probe, _cr, _t = await self.client.download_range_bytes(
                    bucket=bucket,
                    key=key,
                    start=0,
                    end=min(_HEADER_PROBE_SIZE, raw_total) - 1,
                )

            magic = probe.data[:4]

            if is_chunked_envelope(magic):
                return await self._chunked_download_range(
                    bucket,
                    key,
                    start=start,
                    end=end,
                    raw_total=raw_total,
                    content_type=head.content_type,
                    filename=self._filename_from_metadata(key, head.metadata),
                    probe=probe.data,
                )

            if is_envelope(magic):
                raise exc.precondition(
                    "Ranged downloads are unavailable for a whole-payload encrypted "
                    "object (a single AEAD blob cannot be sliced); use download() or "
                    "download_stream().",
                    code=RANGE_WHOLE_PAYLOAD_UNSUPPORTED_CODE,
                )

        # Plaintext (migration tolerance) — pass the ranged GET straight through.
        return await self._raw_download_range(bucket, key, start=start, end=end)

    # ....................... #

    async def _chunked_download_range(
        self,
        bucket: str,
        key: str,
        *,
        start: int,
        end: int | None,
        raw_total: int,
        content_type: str,
        filename: str,
        probe: bytes,
    ) -> RangedDownload:
        """Decrypt only the chunks a byte range covers, then trim to the exact bytes.

        The chunk layout is derived from the stored length, which a truncated object
        under-reports, so every request verifies the last stored frame carries the
        authenticated terminator flag before any range is served (see
        :meth:`_open_chunk_at`). The last frame is already fetched to learn the exact
        plaintext total, so the verification adds no extra reads.
        """

        opener = await self._streaming_cipher().open_chunked_stream(
            probe, aad=self._encryption_aad(bucket, key), tenant=self._cipher_tenant()
        )
        chunk_size = opener.chunk_size

        if raw_total <= opener.header_len:
            raise exc.validation(
                "Chunked object ends before its first frame (truncated)",
                code="core.crypto.chunked_truncated",
            )

        stride = chunk_frame_stride(probe, opener.header_len)

        if stride is None:  # header larger than the probe — widen and retry (rare)
            async with self.client.client():
                wider, _c, _t = await self.client.download_range_bytes(
                    bucket=bucket,
                    key=key,
                    start=opener.header_len,
                    end=min(opener.header_len + _FRAME_PREFIX_MAX, raw_total) - 1,
                )
            stride = chunk_frame_stride(wider.data, 0)

            if stride is None:
                raise exc.internal(
                    "Could not determine the chunked frame stride from the object header",
                )

        # Chunk count and plaintext total (the last chunk's length is only known once
        # decrypted, so read it — it is also reused if the range covers it). Opening
        # the last frame also proves it is the stream's authentic terminator.
        num_chunks = -(-(raw_total - opener.header_len) // stride)  # ceil division
        last_index = num_chunks - 1
        last_plaintext = await self._open_chunk_at(
            bucket, key, opener, stride, last_index, last_index, raw_total
        )
        plaintext_total = last_index * chunk_size + len(last_plaintext)

        if start >= plaintext_total:
            raise unsatisfiable_range(start, plaintext_total)

        end_byte = plaintext_total - 1 if end is None else min(end, plaintext_total - 1)
        first_chunk = start // chunk_size
        last_chunk = end_byte // chunk_size

        out = bytearray()

        for index in range(first_chunk, last_chunk + 1):
            out += (
                last_plaintext
                if index == last_index
                else await self._open_chunk_at(
                    bucket, key, opener, stride, index, last_index, raw_total
                )
            )

        front = start - first_chunk * chunk_size
        data = bytes(out[front : front + (end_byte - start + 1)])

        return RangedDownload(
            data=data,
            content_type=content_type,
            content_range=f"bytes {start}-{end_byte}/{plaintext_total}",
            total_size=plaintext_total,
            filename=filename,
        )

    # ....................... #

    async def _open_chunk_at(
        self,
        bucket: str,
        key: str,
        opener: ChunkedStreamOpener,
        stride: int,
        index: int,
        last_index: int,
        raw_total: int,
    ) -> bytes:
        """Fetch and decrypt a single chunk by index (ranged GET of its frame).

        Beyond AEAD authentication, the frame's terminator flag is checked against
        the stored layout: the last stored frame must carry it — otherwise the
        object lost its tail and the layout-derived plaintext size under-reports —
        and no earlier frame may, matching the streaming path's refusal of a
        truncated stream or of data after the final chunk.
        """

        offset = opener.header_len + index * stride
        end = min(offset + stride, raw_total) - 1  # over-read clamps to the final frame

        async with self.client.client():
            body, _cr, _t = await self.client.download_range_bytes(
                bucket=bucket, key=key, start=offset, end=end
            )

        frame, _consumed = parse_frame(body.data, 0)

        if index == last_index and not frame.is_final:
            raise exc.validation(
                "Chunked object's last stored frame is not its final chunk (truncated)",
                code="core.crypto.chunked_truncated",
            )

        if index != last_index and frame.is_final:
            raise exc.validation(
                "Chunked object carries a frame after its final chunk",
                code="core.crypto.chunked_trailing_data",
            )

        return opener.open_frame(index, frame)

    # ....................... #

    async def download_if_changed(
        self,
        key: str,
        *,
        if_none_match: str | None = None,
        if_modified_since: datetime | None = None,
    ) -> DownloadedObject | None:
        """Conditionally download an object, returning ``None`` when unchanged.

        Validates the key and that at least one condition is supplied, resolves
        the tenant-aware bucket, then issues a conditional ``GET`` through the
        client. ``None`` is the not-modified answer (HTTP 304 equivalent). When
        a body comes back its filename is decoded from the metadata envelope
        (same as :meth:`download`) — or falls back to the key's basename for
        raw/presigned objects with no envelope — and encryption envelopes are
        decrypted. The body's content type and metadata come back on the same
        conditional ``GET`` (no separate head call).
        """

        if if_none_match is None and if_modified_since is None:
            raise exc.validation(
                "download_if_changed requires at least one of if_none_match / if_modified_since",
            )

        self._validate_key(key)
        bucket = await self._resolved_bucket()

        async with self.client.client():
            body = await self.client.download_bytes_conditional(
                bucket=bucket,
                key=key,
                if_none_match=if_none_match,
                if_modified_since=if_modified_since,
            )

            if body is None:
                return None

            data = await self._decrypt_full(body.data, bucket, key)

            filename = self._filename_from_metadata(key, body.metadata)

        return DownloadedObject(
            data=data,
            content_type=body.content_type,
            filename=filename,
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

        Refused when client-side encryption is enabled: a direct ``GET`` would
        return ciphertext the caller cannot decrypt.
        """

        self._reject_presign_when_encrypted()
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

        Refused when client-side encryption is enabled: bytes uploaded directly
        would be stored unencrypted, silently breaking the encryption guarantee.
        """

        self._reject_presign_when_encrypted()
        self._validate_key(key)
        bucket = await self._resolved_bucket()

        async with self.client.client():
            await self.client.ensure_bucket(bucket)

            return await self.client.presign_upload_url(
                bucket=bucket,
                key=key,
                expires_in=expires_in,
                content_type=content_type,
                sse=self.sse,
            )

    # ....................... #

    async def delete(self, key: str) -> None:
        """Delete an object from the bucket by key."""

        self._validate_key(key)
        bucket = await self._resolved_bucket()

        async with self.client.client():
            await self.client.delete_object(bucket=bucket, key=key)

    # ....................... #

    async def copy(self, src_key: str, dst_key: str) -> ObjectHead:
        """Server-side copy *src_key* to *dst_key* within the resolved bucket.

        Both keys are validated (traversal/charset) before anything is copied,
        and the bucket is tenant-resolved exactly like :meth:`download`. The
        copy is server-side (no bytes through the app); the returned head is a
        fresh ``head_object`` on the destination. Same-bucket only.

        Refused when client-side encryption is enabled: the encryption AAD binds
        the ciphertext to the source key, so a server-side copy to a new key
        produces an object that cannot be decrypted at the destination.
        """

        self._reject_copy_when_encrypted()
        self._validate_key(src_key)
        self._validate_key(dst_key)
        bucket = await self._resolved_bucket()

        async with self.client.client():
            await self.client.copy_object(
                bucket=bucket,
                src_key=src_key,
                dst_key=dst_key,
                sse=self.sse,
            )
            h = await self.client.head_object(bucket=bucket, key=dst_key)

        return ObjectHead(
            content_type=h.content_type,
            size=h.size,
            etag=h.etag,
            last_modified=h.last_modified,
            metadata=dict(h.metadata),
            tags=dict(h.tags),
        )

    # ....................... #

    async def move(self, src_key: str, dst_key: str) -> ObjectHead:
        """Move *src_key* to *dst_key* (copy then delete source; non-atomic).

        Both keys are validated and the bucket tenant-resolved like
        :meth:`copy`. Implemented as a server-side copy followed by a delete of
        the source: a crash between the two leaves both keys present. The
        destination head is taken before the source delete, so the returned
        value reflects the copied object.

        Refused when client-side encryption is enabled (same reason as
        :meth:`copy` — the AAD binds ciphertext to the source key).
        """

        self._reject_copy_when_encrypted()
        self._validate_key(src_key)
        self._validate_key(dst_key)
        bucket = await self._resolved_bucket()

        async with self.client.client():
            await self.client.copy_object(
                bucket=bucket,
                src_key=src_key,
                dst_key=dst_key,
                sse=self.sse,
            )
            h = await self.client.head_object(bucket=bucket, key=dst_key)
            if src_key != dst_key:
                await self.client.delete_object(bucket=bucket, key=src_key)

        return ObjectHead(
            content_type=h.content_type,
            size=h.size,
            etag=h.etag,
            last_modified=h.last_modified,
            metadata=dict(h.metadata),
            tags=dict(h.tags),
        )

    # ....................... #

    async def put_object_tags(
        self,
        key: str,
        tags: Mapping[str, str],
    ) -> None:
        """Replace an object's tags (full replacement) by key.

        Validates the key (traversal/charset) and resolves the tenant-aware
        bucket, then delegates the tag replacement to the client.
        """

        self._validate_key(key)
        bucket = await self._resolved_bucket()

        async with self.client.client():
            await self.client.put_object_tags(
                bucket=bucket,
                key=key,
                tags=dict(tags),
            )

    # ....................... #

    async def list(
        self,
        limit: int,
        offset: int,
        *,
        prefix: tuple[str, ...] | str | None = None,
        include_tags: bool = False,
        missing_ok: bool = False,
    ) -> tuple[list[StoredObject], int]:
        """List stored objects with pagination.

        ``include_tags`` is a guarantee, not a filter: with the default
        ``False`` tags may be absent on backends that need extra calls (S3) —
        backends that get them for free (GCS) still include them via head
        metadata; with ``True`` tags are guaranteed populated and backends
        needing extra calls pay them (S3 fans out ``GetObjectTagging`` per
        listed object with bounded concurrency).

        Like every other read (:meth:`download` / :meth:`head` / :meth:`download_range`)
        this does **not** create the bucket: a missing container raises rather than being
        conjured into existence and reported as an empty listing. Only the write paths
        (``upload`` / ``upload_stream`` / ``presign_upload`` / ``begin_upload``) create on
        demand. A read that created would make an *absent* bucket indistinguishable from an
        *empty* one — and would silently undo a deletion for any caller that merely listed,
        including the re-encryption sweep, whose bucket-vanished guard is exactly that
        distinction.

        ``missing_ok`` opts a caller *out* of that distinction: when set, a bucket that
        does not yet exist yields an empty listing instead of raising. Use it where an
        unprovisioned bucket legitimately means "nothing stored yet" — the object-list
        route on a fresh deployment, a portability export of an app with no blobs — rather
        than a fault. The sweep keeps the default so a *vanished* bucket still raises.
        """

        prefix = default_path_codec.join(prefix)
        self._validate_prefix(prefix)

        path = self.construct_path(prefix)

        bucket = await self._resolved_bucket()

        async with self.client.client():
            try:
                objects, total_count = await self.client.list_objects(
                    bucket=bucket,
                    prefix=path,
                    limit=limit,
                    offset=offset,
                    include_tags=include_tags,
                )

            except CoreException:
                # A not-yet-provisioned bucket reads as empty only when the caller opted in
                # *and* the bucket genuinely does not exist — any other list failure (a real
                # outage, a permission error) still propagates. The existence probe is a
                # read, so it never creates the bucket the raise was distinguishing.
                if missing_ok and not await self.client.bucket_exists(bucket):
                    return [], 0

                raise

            for o in objects:
                if not o.key:
                    raise exc.internal("Invalid object key")

            heads = await self._head_all(bucket, objects)

            out = [
                self._stored_from_head(o.key, h, o.tags)
                for o, h in zip(objects, heads, strict=True)
            ]

        return out, total_count

    # ....................... #

    async def _head_all(
        self, bucket: str, objects: Sequence[ObjectStorageListedObject]
    ) -> builtins.list[ObjectStorageHead]:
        """HEAD every listed object with bounded concurrency (see :data:`_LIST_HEAD_FANOUT`).

        Preserves input order (the caller ``zip``s heads back onto objects), so results are
        collected by index rather than completion.
        """

        semaphore = asyncio.Semaphore(_LIST_HEAD_FANOUT)
        heads: builtins.list[ObjectStorageHead | None] = [None] * len(objects)

        async def _one(index: int, key: str) -> None:
            async with semaphore:
                heads[index] = await self.client.head_object(bucket=bucket, key=key)

        await asyncio.gather(*(_one(i, o.key) for i, o in enumerate(objects)))

        return [head for head in heads if head is not None]

    # ....................... #

    async def begin_upload(
        self,
        key: str,
        *,
        content_type: str | None = None,
    ) -> UploadSession:
        """Open a resumable multipart upload session targeting *key*.

        Refused on a client-side-encrypting route (the app never sees the part
        bytes, so it cannot encrypt them). Validates the key like
        :meth:`upload`, resolves and ensures the tenant bucket, then opens the
        backend session and returns the :class:`UploadSession` handle whose
        ``upload_id`` the caller must persist.
        """

        self._reject_multipart_when_encrypted()
        self._validate_key(key)
        bucket = await self._resolved_bucket()

        async with self.client.client():
            await self.client.ensure_bucket(bucket)

            upload_id = await self.client.create_multipart_upload(
                bucket=bucket,
                key=key,
                content_type=content_type,
                sse=self.sse,
            )

        return UploadSession(
            key=key,
            upload_id=upload_id,
            bucket=bucket,
            content_type=content_type,
        )

    # ....................... #

    async def presign_part(
        self,
        session: UploadSession,
        part_number: int,
        *,
        expires_in: timedelta,
    ) -> PresignedUrl:
        """Mint a time-limited ``PUT`` URL for one part (``part_number >= 1``).

        Refused on an encrypting route. Validates the session key and that
        ``part_number >= 1``, resolves the tenant bucket, then signs the part
        ``PUT``. The URL is a bearer credential — short ``expires_in``.
        """

        self._reject_multipart_when_encrypted()

        if part_number < 1:
            raise exc.validation(
                f"Multipart part_number must be >= 1, got {part_number}",
            )

        self._validate_key(session.key)
        bucket = await self._resolved_bucket()

        async with self.client.client():
            return await self.client.presign_multipart_part(
                bucket=bucket,
                key=session.key,
                upload_id=session.upload_id,
                part_number=part_number,
                expires_in=expires_in,
            )

    # ....................... #

    async def list_parts(self, session: UploadSession) -> builtins.list[UploadPart]:
        """List the parts that already landed for *session* (the resume primitive)."""

        self._reject_multipart_when_encrypted()
        self._validate_key(session.key)
        bucket = await self._resolved_bucket()

        async with self.client.client():
            parts = await self.client.list_multipart_parts(
                bucket=bucket,
                key=session.key,
                upload_id=session.upload_id,
            )

        return [
            UploadPart(
                part_number=p.part_number,
                etag=p.etag,
                size=p.size,
            )
            for p in parts
        ]

    # ....................... #

    async def complete_upload(
        self,
        session: UploadSession,
        parts: Sequence[UploadPart],
    ) -> ObjectHead:
        """Assemble *parts* into the final object and return its head.

        Refused on an encrypting route. Validates the session key, resolves the
        tenant bucket, completes the backend upload (S3
        ``CompleteMultipartUpload`` with the ``{part_number, etag}`` list; GCS
        chained ``compose`` in part-number order + temp cleanup), then heads the
        assembled object.
        """

        self._reject_multipart_when_encrypted()
        self._validate_key(session.key)

        if not parts:
            raise exc.validation(
                "complete_upload requires at least one part",
            )

        _reject_duplicate_part_numbers(parts)

        bucket = await self._resolved_bucket()

        client_parts = [
            ObjectStoragePartInfo(
                part_number=p.part_number,
                etag=p.etag,
                size=p.size,
            )
            for p in sorted(parts, key=lambda p: p.part_number)
        ]

        async with self.client.client():
            await self.client.complete_multipart_upload(
                bucket=bucket,
                key=session.key,
                upload_id=session.upload_id,
                parts=client_parts,
                content_type=session.content_type,
                sse=self.sse,
            )

            h = await self.client.head_object(bucket=bucket, key=session.key)

        return ObjectHead(
            content_type=h.content_type,
            size=h.size,
            etag=h.etag,
            last_modified=h.last_modified,
            metadata=dict(h.metadata),
            tags=dict(h.tags),
        )

    # ....................... #

    async def abort_upload(self, session: UploadSession) -> None:
        """Discard an unfinished session and free its in-progress data."""

        self._reject_multipart_when_encrypted()
        self._validate_key(session.key)
        bucket = await self._resolved_bucket()

        async with self.client.client():
            await self.client.abort_multipart_upload(
                bucket=bucket,
                key=session.key,
                upload_id=session.upload_id,
            )

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

    with suppress(Exception):  # nosec B110
        import magic

        if ct_magic := magic.from_buffer(data, mime=True):
            return ct_magic

    return _guess_content_type_from_name(filename)
