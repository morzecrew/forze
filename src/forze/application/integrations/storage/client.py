"""Unified object-storage client port and value objects for S3/GCS integrations."""

import math
from collections.abc import Awaitable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timedelta
from typing import (
    Final,
    Literal,
    Protocol,
    final,
)

import attrs

from forze.application.contracts.storage import (
    OVERWRITE_PRECONDITION_FAILED_CODE,
    RANGE_NOT_SATISFIABLE_CODE,
    PresignedUrl,
)
from forze.base.exceptions import CoreException, exc

# ----------------------- #

PRESIGN_MAX_EXPIRY: Final[timedelta] = timedelta(days=7)
"""Hard upper bound on presigned-URL lifetimes shared by S3 (SigV4) and GCS (V4)."""

# ....................... #


def presign_expiry_seconds(
    expires_in: timedelta,
    *,
    max_expiry: timedelta | None = PRESIGN_MAX_EXPIRY,
) -> int:
    """Validate a presign expiry window and convert it to whole seconds.

    Sub-second windows round **up**, so any positive ``expires_in`` yields at
    least one second (backends take integral seconds).

    :param expires_in: Requested URL lifetime.
    :param max_expiry: Hard backend cap (defaults to the 7-day S3/GCS limit);
        ``None`` disables the cap check.
    :returns: The lifetime in whole seconds.
    :raises CoreException: ``validation`` when ``expires_in`` is not positive
        or exceeds *max_expiry*.
    """

    if expires_in <= timedelta(0):
        raise exc.validation(
            f"Presigned URL expiry must be positive, got {expires_in!r}",
        )

    if max_expiry is not None and expires_in > max_expiry:
        raise exc.validation(
            f"Presigned URL expiry {expires_in!r} exceeds the backend cap of {max_expiry!r}",
        )

    return math.ceil(expires_in.total_seconds())


# ....................... #


def normalize_list_window(limit: int | None, offset: int | None) -> tuple[int, int]:
    """Validate and default an object-listing window to ``(limit, offset)``.

    :param limit: Requested max items (``None`` means effectively unbounded).
    :param offset: Requested start offset (``None`` means ``0``).
    :returns: ``(effective_limit, effective_offset)``.
    :raises CoreException: ``validation`` when ``limit <= 0`` or ``offset < 0``.
    """

    if limit is not None and limit <= 0:
        raise exc.validation("limit must be > 0")

    if offset is not None and offset < 0:
        raise exc.validation("offset must be >= 0")

    return (limit if limit is not None else 10_000_000), (offset or 0)


# ....................... #


def validate_range(start: int, end: int | None) -> None:
    """Validate an inclusive byte range request shared by all backends.

    :raises CoreException: ``validation`` when ``start < 0`` or, when *end* is
        given, ``end < start``.
    """

    if start < 0:
        raise exc.validation(f"Range start must be >= 0, got {start}")

    if end is not None and end < start:
        raise exc.validation(
            f"Range end {end} must be >= start {start}",
        )


# ....................... #


def build_range_header(start: int, end: int | None) -> str:
    """Build an HTTP ``Range`` header value for an inclusive byte range."""

    if end is None:
        return f"bytes={start}-"

    return f"bytes={start}-{end}"


# ....................... #


def unsatisfiable_range(start: int, total: int) -> CoreException:
    """Build the precondition error for a range whose start is past the object end.

    Mirrors the backend HTTP 416 (Range Not Satisfiable) response.
    """

    return exc.precondition(
        f"Requested range start {start} is beyond object size {total}",
        code=RANGE_NOT_SATISFIABLE_CODE,
    )


# ....................... #


def overwrite_precondition_failed(key: str) -> CoreException:
    """Build the conflict for a conditional write whose match token no longer holds.

    Mirrors the backend HTTP 412 (Precondition Failed) response on a write carrying
    ``If-Match`` / ``ifGenerationMatch``: the object at *key* was replaced by concurrent
    traffic between the caller's read and the write's visibility point. Shared by the
    S3/GCS clients and the mock adapter so the mismatch surfaces under one code.
    """

    return exc.conflict(
        f"Conditional write to {key!r} failed: the object changed since it was read",
        code=OVERWRITE_PRECONDITION_FAILED_CODE,
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ObjectStorageSSE:
    """Backend-neutral server-side-encryption (SSE/CMEK) request descriptor.

    Threaded by the adapter from per-route config to the storage client on the
    direct-upload paths (upload, copy, presign, multipart create). This is the
    **at-rest** axis — the *backend* encrypts the bytes it stores — and is
    orthogonal to (and combinable with) the adapter's client-side envelope
    encryption (``cipher``), which still refuses direct-upload flows.

    Fields are interpreted per backend:

    - **S3** — ``mode`` selects ``"s3"`` (SSE-S3, ``AES256``) or ``"kms"``
      (SSE-KMS, ``aws:kms`` with ``key_id`` as the KMS key id). ``"none"`` is
      the off sentinel (no SSE params sent).
    - **GCS** — ``key_id`` is the CMEK ``kmsKeyName`` for per-object encryption
      (``upload``/``compose``); ``mode`` is ignored (GCS has no SSE-S3 analog,
      Google-managed default encryption is always on). ``None``/empty ``key_id``
      means the Google-managed default.

    A ``None`` descriptor (the kwarg default everywhere) means "no SSE
    requested" — behavior is unchanged from before SSE existed.
    """

    mode: Literal["none", "s3", "kms"] = "none"
    """SSE mode (S3 semantics). ``"none"`` sends no SSE params."""

    key_id: str | None = None
    """KMS/CMEK key identifier (S3 ``SSEKMSKeyId`` / GCS ``kmsKeyName``)."""

    # ....................... #

    @property
    def requested(self) -> bool:
        """Whether any SSE was actually requested (mode set or a key present)."""

        return self.mode != "none" or bool(self.key_id)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ObjectBody:
    """An object's bytes plus the metadata the same ``GET`` already returned.

    Returned by the client ``GET`` methods (:meth:`ObjectStorageClientPort.download_bytes`,
    :meth:`~ObjectStorageClientPort.download_bytes_conditional`,
    :meth:`~ObjectStorageClientPort.download_range_bytes`). A single backend
    ``GET`` already carries the content type and user metadata, so the body
    surfaces them instead of forcing the adapter to issue a separate
    ``head_object`` round-trip. :attr:`metadata` may be empty for objects
    written through a presigned ``PUT`` (which carry no envelope) and for ranged
    reads; consumers must tolerate that.
    """

    data: bytes
    """Raw object bytes (the requested range slice for a ranged read)."""

    content_type: str = "application/octet-stream"
    """MIME type reported by the ``GET`` response."""

    metadata: Mapping[str, str] = attrs.field(factory=dict[str, str])
    """User-defined metadata from the ``GET`` response (may be empty)."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ObjectStorageHead:
    """Metadata returned by an object head/metadata request."""

    content_type: str = "application/octet-stream"
    """MIME type of the object."""

    metadata: Mapping[str, str] = attrs.field(factory=dict[str, str])
    """User-defined metadata key-value pairs."""

    size: int = 0
    """Content length in bytes."""

    last_modified: datetime | None = None
    """Timestamp of the last modification."""

    etag: str = ""
    """Entity tag with surrounding quotes stripped when applicable."""

    tags: Mapping[str, str] = attrs.field(factory=dict[str, str])
    """Object tags when the backend surfaces them on a head request.

    Population depends on the ``include_tags`` flag of
    :meth:`ObjectStorageClientPort.head_object`: with ``include_tags=False``
    (the default) backends that get tags for free still include them (GCS
    round-trips tags via namespaced custom metadata; the mock adapter stores
    them in-memory), but S3 head responses do not carry tags, so this mapping
    may be empty even when the object carries tags. With ``include_tags=True``
    the mapping is guaranteed to be populated — S3 issues a separate
    ``GetObjectTagging`` call to fetch them.
    """


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ObjectStorageListedObject:
    """Minimal object descriptor returned by :meth:`ObjectStorageClientPort.list_objects`."""

    key: str
    """Object key (blob name)."""

    tags: Mapping[str, str] = attrs.field(factory=dict[str, str])
    """Object tags when requested via ``include_tags=True`` on
    :meth:`ObjectStorageClientPort.list_objects`.

    Listing APIs do not return tags natively, so this mapping is empty unless
    the caller asked for the tag guarantee (S3 then fans out
    ``GetObjectTagging`` per listed object). Backends whose tags only travel
    on head metadata (GCS, mock) leave this empty either way — their tags
    surface on :class:`ObjectStorageHead` instead.
    """


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ObjectStoragePartInfo:
    """One already-uploaded part as reported by the backend on resume.

    Returned by :meth:`ObjectStorageClientPort.list_multipart_parts`. Mirrors
    S3 ``ListParts`` entries; on GCS it is synthesized from the temp part
    objects (``part_number`` + ``size``, ``etag`` best-effort).
    """

    part_number: int
    """1-indexed part position."""

    etag: str = ""
    """Backend ETag for the part (S3); may be empty on GCS."""

    size: int = 0
    """Part size in bytes when the backend reports it; ``0`` otherwise."""


# ....................... #


class ObjectStorageClientPort(Protocol):
    """Operations implemented by storage clients."""

    def close(self) -> Awaitable[None]: ...  # pragma: no cover

    def client(self) -> AbstractAsyncContextManager[object]: ...  # pragma: no cover

    def health(self) -> Awaitable[tuple[str, bool]]: ...  # pragma: no cover

    def bucket_exists(self, bucket: str) -> Awaitable[bool]: ...  # pragma: no cover

    def create_bucket(self, bucket: str) -> Awaitable[None]: ...  # pragma: no cover

    def ensure_bucket(self, bucket: str) -> Awaitable[None]:
        """Create *bucket* when it does not exist (idempotent create-if-missing)."""
        ...  # pragma: no cover

    def object_exists(self, bucket: str, key: str) -> Awaitable[bool]: ...  # pragma: no cover

    def upload_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        tags: dict[str, str] | None = None,
        sse: ObjectStorageSSE | None = None,
    ) -> Awaitable[None]:
        """Upload raw bytes to *key* in *bucket*.

        When *sse* requests server-side encryption the backend encrypts the
        stored bytes at rest (S3 SSE-S3/SSE-KMS, GCS per-object CMEK); ``None``
        leaves the bucket's default encryption in effect.
        """
        ...  # pragma: no cover

    def download_bytes(
        self,
        bucket: str,
        key: str,
    ) -> Awaitable[ObjectBody]:
        """Download an object's full body plus its already-fetched metadata.

        A single backend ``GET`` carries the content type and user metadata, so
        the returned :class:`ObjectBody` surfaces them — the adapter no longer
        needs a separate ``head_object`` round-trip. :attr:`ObjectBody.metadata`
        is empty for objects written through a presigned ``PUT`` (no envelope).
        """
        ...  # pragma: no cover

    def download_range_bytes(
        self,
        bucket: str,
        key: str,
        *,
        start: int,
        end: int | None = None,
    ) -> Awaitable[tuple[ObjectBody, str, int]]:
        """Download an inclusive byte range of an object.

        Issues a ranged ``GET`` (HTTP ``Range: bytes=start-end``, ``end``
        inclusive; ``end=None`` reads to EOF). An unsatisfiable range (``start``
        past the object size) raises a precondition error mirroring the backend
        416 response.

        :returns: ``(body, content_range, total_size)`` where *body* carries the
            range slice and its content type (``body.metadata`` may be empty for
            ranges), *content_range* is the satisfied ``bytes start-end/total``,
            and *total_size* the full object size.
        """
        ...  # pragma: no cover

    def download_bytes_conditional(
        self,
        bucket: str,
        key: str,
        *,
        if_none_match: str | None = None,
        if_modified_since: datetime | None = None,
    ) -> Awaitable[ObjectBody | None]:
        """Conditionally download an object, returning ``None`` when unchanged.

        Passes ``If-None-Match`` / ``If-Modified-Since`` to the backend; a
        not-modified / precondition-failed response (S3/GCS map ``304``/``412``)
        becomes ``None``. Any other failure propagates.

        :returns: an :class:`ObjectBody` (bytes + content type + already-fetched
            metadata) when the object changed, else ``None``.
        """
        ...  # pragma: no cover

    def copy_object(
        self,
        bucket: str,
        src_key: str,
        dst_key: str,
        *,
        sse: ObjectStorageSSE | None = None,
    ) -> Awaitable[None]:
        """Server-side copy *src_key* to *dst_key* within *bucket*.

        S3 ``CopyObject`` (single-copy capped at 5 GiB), GCS object rewrite
        (handles large objects). Same-bucket only.

        When *sse* is set the destination is (re-)encrypted at rest under the
        route's SSE: S3 ``CopyObject`` re-encrypts the destination with the
        supplied SSE params; GCS rewrites under the per-object CMEK key.
        """
        ...  # pragma: no cover

    def put_object_tags(
        self,
        bucket: str,
        key: str,
        tags: Mapping[str, str],
    ) -> Awaitable[None]:
        """Replace an object's tags with *tags* (full replacement).

        S3 ``PutObjectTagging``; GCS rewrites the namespaced tag custom metadata.
        """
        ...  # pragma: no cover

    def delete_object(
        self,
        bucket: str,
        key: str,
    ) -> Awaitable[None]: ...  # pragma: no cover

    def list_objects(
        self,
        bucket: str,
        prefix: str | None = None,
        *,
        limit: int | None = None,
        offset: int | None = None,
        include_tags: bool = False,
    ) -> Awaitable[tuple[list[ObjectStorageListedObject], int]]:
        """List objects under *prefix* with an offset/limit window.

        ``include_tags`` is a **guarantee, not a filter**: with ``False``
        (default) tags on the returned descriptors may be absent — backends
        that get them for free still include them; with ``True`` tags are
        guaranteed populated, and backends needing extra calls (S3:
        ``GetObjectTagging`` per object) pay them.
        """
        ...  # pragma: no cover

    def head_object(
        self,
        bucket: str,
        key: str,
        *,
        include_tags: bool = False,
    ) -> Awaitable[ObjectStorageHead]:
        """Fetch object metadata without downloading the body.

        ``include_tags`` is a **guarantee, not a filter**: with ``False``
        (default) :attr:`ObjectStorageHead.tags` may be absent — backends
        that get tags for free still include them; with ``True`` the tags
        mapping is guaranteed populated, and backends needing an extra call
        (S3: ``GetObjectTagging``) pay it.
        """
        ...  # pragma: no cover

    def presign_download_url(
        self,
        bucket: str,
        key: str,
        *,
        expires_in: timedelta,
    ) -> Awaitable[PresignedUrl]:
        """Sign a time-limited ``GET`` URL for *key* in *bucket*.

        Signing is local on both backends (no API round-trip); the URL grants
        unauthenticated read access until expiry — see
        :class:`~forze.application.contracts.storage.PresignedUrl` for the
        trust model. ``expires_in`` must be positive and within
        :data:`PRESIGN_MAX_EXPIRY` (the shared 7-day S3/GCS cap).
        """
        ...  # pragma: no cover

    def presign_upload_url(
        self,
        bucket: str,
        key: str,
        *,
        expires_in: timedelta,
        content_type: str | None = None,
        sse: ObjectStorageSSE | None = None,
    ) -> Awaitable[PresignedUrl]:
        """Sign a time-limited ``PUT`` URL for *key* in *bucket*.

        Signing is local on both backends (no API round-trip); the URL grants
        unauthenticated write access until expiry — see
        :class:`~forze.application.contracts.storage.PresignedUrl` for the
        trust model. When ``content_type`` is given the signature binds it and
        the returned :attr:`PresignedUrl.headers` carries the header the
        client must send. ``expires_in`` must be positive and within
        :data:`PRESIGN_MAX_EXPIRY` (the shared 7-day S3/GCS cap).

        When *sse* is set, **S3** binds the SSE headers into the signature and
        returns them in :attr:`PresignedUrl.headers` so the uploader sends them
        verbatim (mandatory for SSE-KMS; portable for SSE-S3). **GCS** cannot
        carry a CMEK header on a raw signed ``PUT``: per-object CMEK applies
        only on the app-path ``upload``/``compose``; for presigned PUTs CMEK
        rides the bucket's default-encryption config (set out-of-band on the
        bucket), so no SSE header is added.
        """
        ...  # pragma: no cover

    # ....................... #
    # Resumable multipart upload primitives.
    #
    # These are the raw backend calls behind
    # :class:`~forze.application.contracts.storage.StorageUploadSessionPort`.
    # The adapter does key validation, tenant-bucket resolution, and VO
    # mapping; the client does the raw backend work and stays divergent
    # (S3 native UploadId+ETags vs GCS temp-keys+compose) behind a uniform
    # signature.

    def create_multipart_upload(
        self,
        bucket: str,
        key: str,
        *,
        content_type: str | None = None,
        metadata: Mapping[str, str] | None = None,
        sse: ObjectStorageSSE | None = None,
    ) -> Awaitable[str]:
        """Open a multipart upload and return its backend upload id.

        *metadata* is the object's user metadata. Like *content_type*, it is bound
        where the backend allows: **S3** binds it here (``CreateMultipartUpload``);
        **GCS** has no native session, so it applies metadata to the composed
        destination in :meth:`complete_multipart_upload` instead.

        S3 ``CreateMultipartUpload`` returns an ``UploadId``. GCS has no
        native session, so the client returns a generated temp part-key
        namespace token (the upload id) that the other multipart primitives
        interpret.

        When *sse* is set, **S3** binds it on ``CreateMultipartUpload`` and the
        parts inherit it — the per-part presigned ``UploadPart`` URLs carry no
        SSE headers (see :meth:`presign_multipart_part`). **GCS** assembles the
        object via ``compose`` at completion, where per-object CMEK applies; the
        ``sse`` here is recorded but the presigned part PUTs cannot carry it.

        :returns: The backend-specific upload id (opaque to the caller).
        """
        ...  # pragma: no cover

    def upload_multipart_part(
        self,
        bucket: str,
        key: str,
        *,
        upload_id: str,
        part_number: int,
        data: bytes,
        sse: ObjectStorageSSE | None = None,
    ) -> Awaitable[ObjectStoragePartInfo]:
        """Upload one part's bytes directly (app-mediated, not via a presigned URL).

        The presign path (:meth:`presign_multipart_part`) has an outside HTTP client
        ``PUT`` the part bytes; this uploads bytes the application itself holds — needed
        when each part must be transformed first (client-side chunked encryption). S3
        issues ``UploadPart`` and returns the part ``ETag``; GCS writes the temp part
        object the compose-at-complete assembles. *part_number* is 1-indexed.

        :returns: The part's :class:`ObjectStoragePartInfo` (with backend ETag/size) to
            carry into :meth:`complete_multipart_upload`.
        """
        ...  # pragma: no cover

    def presign_multipart_part(
        self,
        bucket: str,
        key: str,
        *,
        upload_id: str,
        part_number: int,
        expires_in: timedelta,
    ) -> Awaitable[PresignedUrl]:
        """Sign a time-limited ``PUT`` URL for one part of a multipart upload.

        S3 signs ``upload_part`` (``Bucket``/``Key``/``UploadId``/``PartNumber``).
        GCS signs a plain ``PUT`` to the temp part object addressed by
        ``upload_id`` + ``part_number``. ``part_number`` is 1-indexed;
        ``expires_in`` is validated like every presign.
        """
        ...  # pragma: no cover

    def list_multipart_parts(
        self,
        bucket: str,
        key: str,
        *,
        upload_id: str,
    ) -> Awaitable[list[ObjectStoragePartInfo]]:
        """List the parts already uploaded for an in-progress multipart upload.

        S3 ``ListParts``; GCS lists the temp part objects of the namespace.
        Used to resume an interrupted upload (presign only the missing parts).
        """
        ...  # pragma: no cover

    def complete_multipart_upload(
        self,
        bucket: str,
        key: str,
        *,
        upload_id: str,
        parts: Sequence[ObjectStoragePartInfo],
        content_type: str | None = None,
        metadata: Mapping[str, str] | None = None,
        sse: ObjectStorageSSE | None = None,
        if_match: str | None = None,
    ) -> Awaitable[None]:
        """Assemble the uploaded parts into the final object.

        *metadata* is consumed by **GCS** only (the composed destination carries no
        metadata from the temp parts); **S3** binds it at
        :meth:`create_multipart_upload` and ignores it here — the same split
        *content_type* already follows.

        S3 ``CompleteMultipartUpload`` (requires ``{PartNumber, ETag}`` per
        part, ascending). GCS chained ``compose`` of the temp parts in
        ascending ``part_number`` order into *key*, then deletes the temps
        (compose takes at most 32 sources per call, so larger sets chain).

        *content_type* is consumed by **GCS** only: it has no native session, so
        the type bound at ``begin_upload`` is applied to the final
        ``compose``/copy destination (the temp parts carry none). S3 inherits it
        from ``CreateMultipartUpload`` (set at begin time) and ignores it here.

        *sse* is likewise consumed by **GCS** only: it carries the per-object
        CMEK ``kmsKeyName`` for the final destination; S3 inherits the upload's
        encryption from ``CreateMultipartUpload`` and ignores *sse* here.

        *if_match* makes the completion **conditional on the destination's
        current ETag**: the assembled object only replaces *key* while the
        stored object still carries that ETag, closing the window in which a
        concurrent delete would be silently undone by the completion. **S3**
        sends ``If-Match`` on ``CompleteMultipartUpload`` (server-enforced:
        412 on mismatch, 404 when the target vanished). **GCS** resolves the
        destination's current metadata, refuses on an ETag mismatch, and
        passes the resolved generation as ``ifGenerationMatch`` on the final
        ``compose``/rewrite so real GCS enforces the condition atomically at
        the visibility point. A failed condition raises ``conflict`` with code
        :data:`~forze.application.contracts.storage.OVERWRITE_PRECONDITION_FAILED_CODE`;
        a vanished destination raises ``not_found``. ``None`` completes
        unconditionally (the historical behavior).
        """
        ...  # pragma: no cover

    def abort_multipart_upload(
        self,
        bucket: str,
        key: str,
        *,
        upload_id: str,
    ) -> Awaitable[None]:
        """Abort an in-progress multipart upload and free its parts.

        S3 ``AbortMultipartUpload``; GCS deletes the temp part objects.
        Best-effort idempotent.
        """
        ...  # pragma: no cover
