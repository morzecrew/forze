"""Unified object-storage client port and value objects for S3/GCS integrations."""

import math
from datetime import datetime, timedelta
from typing import AsyncContextManager, Awaitable, Final, Mapping, Protocol, final

import attrs

from forze.application.contracts.storage import PresignedUrl
from forze.base.exceptions import exc

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
            f"Presigned URL expiry {expires_in!r} exceeds the backend cap "
            f"of {max_expiry!r}",
        )

    return math.ceil(expires_in.total_seconds())


# ....................... #


def normalize_list_window(limit: int | None, offset: int | None) -> tuple[int, int]:
    """Validate and default an object-listing window to ``(limit, offset)``.

    :param limit: Requested max items (``None`` means effectively unbounded).
    :param offset: Requested start offset (``None`` means ``0``).
    :returns: ``(effective_limit, effective_offset)``.
    :raises CoreException: When ``limit <= 0`` or ``offset < 0``.
    """

    if limit is not None and limit <= 0:
        raise exc.internal("limit must be > 0")

    if offset is not None and offset < 0:
        raise exc.internal("offset must be >= 0")

    return (limit if limit is not None else 10_000_000), (offset or 0)


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


class ObjectStorageClientPort(Protocol):
    """Operations implemented by storage clients."""

    def close(self) -> Awaitable[None]: ...  # pragma: no cover

    def client(self) -> AsyncContextManager[object]: ...  # pragma: no cover

    def health(self) -> Awaitable[tuple[str, bool]]: ...  # pragma: no cover

    def bucket_exists(self, bucket: str) -> Awaitable[bool]: ...  # pragma: no cover

    def create_bucket(self, bucket: str) -> Awaitable[None]: ...  # pragma: no cover

    def ensure_bucket(self, bucket: str) -> Awaitable[None]:
        """Create *bucket* when it does not exist (idempotent create-if-missing)."""
        ...  # pragma: no cover

    def object_exists(
        self, bucket: str, key: str
    ) -> Awaitable[bool]: ...  # pragma: no cover

    def upload_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        tags: dict[str, str] | None = None,
    ) -> Awaitable[None]: ...  # pragma: no cover

    def download_bytes(
        self,
        bucket: str,
        key: str,
    ) -> Awaitable[bytes]: ...  # pragma: no cover

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
    ) -> Awaitable[PresignedUrl]:
        """Sign a time-limited ``PUT`` URL for *key* in *bucket*.

        Signing is local on both backends (no API round-trip); the URL grants
        unauthenticated write access until expiry — see
        :class:`~forze.application.contracts.storage.PresignedUrl` for the
        trust model. When ``content_type`` is given the signature binds it and
        the returned :attr:`PresignedUrl.headers` carries the header the
        client must send. ``expires_in`` must be positive and within
        :data:`PRESIGN_MAX_EXPIRY` (the shared 7-day S3/GCS cap).
        """
        ...  # pragma: no cover
