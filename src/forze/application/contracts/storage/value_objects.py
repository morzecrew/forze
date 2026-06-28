from datetime import datetime
from typing import Literal, Mapping, final

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _InternalMetadata:
    """Optional metadata for an object."""

    filename: str
    """Original filename associated with the object."""

    created_at: datetime
    """Backend timestamp when the object was created."""

    size: int
    """Object size in bytes."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class UploadedObject:
    """Value object for an uploaded object."""

    filename: str
    """Original filename associated with the upload."""

    data: bytes
    """Raw bytes payload to store."""

    description: str | None = None
    """Optional human-readable description."""

    tags: Mapping[str, str] | None = None
    """Optional tags associated with the object."""

    prefix: str | None = None
    """Optional key prefix (folder-like namespace)."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DownloadedObject:
    """Value object for a downloaded object."""

    data: bytes
    """Raw object payload."""

    content_type: str
    """MIME content type of the downloaded data."""

    filename: str
    """Original filename associated with the downloaded data."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ObjectMetadata(_InternalMetadata):
    """Value object for an object metadata."""

    description: str | None = None
    """Optional human-readable description."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StoredObject(_InternalMetadata):
    """Value object for a stored object."""

    key: str
    """Opaque storage key used to retrieve the object later."""

    content_type: str
    """MIME content type of the stored data."""

    tags: Mapping[str, str] | None = None
    """Optional tags associated with the object."""

    description: str | None = None
    """Optional human-readable description."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ObjectHead:
    """Honest head/metadata view of a stored object.

    Distinct from :class:`ObjectMetadata` / :class:`StoredObject`, whose
    ``filename`` / ``created_at`` fields are *envelope*-shaped (they describe
    the metadata convention :meth:`StorageCommandPort.upload` writes). An
    object stored through a presigned direct ``PUT`` carries **no** such
    envelope, yet still has a head — content type, byte size, ETag, last-mod
    time, and whatever user metadata/tags the uploader (or backend) set. This
    value object mirrors exactly what a backend ``HEAD`` request honestly
    returns, so it works for both enveloped and raw objects.
    """

    content_type: str = "application/octet-stream"
    """MIME content type of the object."""

    size: int = 0
    """Content length in bytes."""

    etag: str = ""
    """Entity tag (surrounding quotes stripped when applicable); empty when the
    backend does not surface one."""

    last_modified: datetime | None = None
    """Backend last-modification timestamp, or ``None`` when unavailable."""

    metadata: Mapping[str, str] = attrs.field(factory=dict[str, str])
    """User-defined metadata key-value pairs as the backend stores them
    (the raw envelope keys for enveloped objects; whatever the uploader set
    for raw presigned uploads)."""

    tags: Mapping[str, str] = attrs.field(factory=dict[str, str])
    """Object tags. Population follows the ``include_tags`` guarantee of
    :meth:`StorageQueryPort.head`: with ``False`` (default) this may be empty on
    backends that need an extra call to fetch tags (S3); with ``True`` it is
    guaranteed populated. Backends that round-trip tags for free (GCS, mock)
    include them either way."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RangedDownload:
    """A partial-content download satisfying a byte-range request."""

    data: bytes
    """The bytes of the satisfied range (``end`` inclusive per HTTP)."""

    content_type: str
    """MIME content type of the object."""

    content_range: str
    """The satisfied range as an HTTP ``Content-Range`` value, e.g.
    ``bytes 0-499/1234`` (``start-end/total``)."""

    total_size: int
    """Full object size in bytes (the ``total`` in :attr:`content_range`)."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PresignedUrl:
    """A time-limited URL granting direct (unauthenticated) object access.

    **Trust model.** Anyone holding :attr:`url` can perform :attr:`method` on
    the object until :attr:`expires_at` — the URL *is* the credential. Treat
    it like a secret: hand it only to the intended client, prefer short
    expiries, and never log it (:attr:`url` is excluded from ``repr`` so the
    value object itself is safe to log/trace).

    The backend signature may also bind request headers (e.g. a constrained
    ``Content-Type`` on uploads); :attr:`headers` carries everything the
    client **must** send verbatim for the request to verify.
    """

    url: str = attrs.field(repr=False)
    """The presigned URL. The bearer credential itself — excluded from
    ``repr`` so accidental logging of the value object does not leak it."""

    method: Literal["GET", "PUT"]
    """HTTP method the signature authorizes."""

    expires_at: datetime
    """UTC instant after which the URL stops verifying."""

    headers: Mapping[str, str] = attrs.field(factory=dict[str, str])
    """Headers the client **must** send with the request (the signature binds
    them); empty when the request needs no extra headers."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class UploadSession:
    """A resumable multipart-upload session against a single object key.

    Returned by :meth:`StorageUploadSessionPort.begin_upload` and threaded back
    into every later call (presign / list / complete / abort). It is the
    **handle** the application persists (or hands to the client) to resume or
    finish an upload that may span many independently-uploaded parts.

    **Backend divergence (internal).** The :attr:`upload_id` carries whatever
    the backend needs to address the in-progress upload — an S3 ``UploadId`` on
    native multipart, or a temp part-key namespace token on GCS compose-based
    multipart. The application must treat it as opaque.

    **Credential.** :attr:`upload_id` is excluded from ``repr``: an S3
    ``UploadId`` grants the ability to add/complete parts on the key, and the
    GCS namespace token names where part bytes land — neither should leak into
    logs or traces.
    """

    key: str
    """Final object key the assembled upload lands at (validated against
    traversal, exactly like :meth:`StorageCommandPort.upload`'s keys)."""

    upload_id: str = attrs.field(repr=False)
    """Opaque, backend-specific upload credential (S3 ``UploadId`` / GCS temp
    namespace token). Excluded from ``repr`` — treat it as a secret. The
    application **must** persist it (or hand it to the client) to resume,
    complete, or abort the session later; it is not otherwise recoverable."""

    bucket: str | None = None
    """Resolved physical bucket the upload targets, when the adapter chooses to
    surface it; ``None`` leaves bucket resolution implicit (the adapter
    re-resolves the tenant bucket on each call)."""

    content_type: str | None = None
    """MIME type bound at :meth:`StorageUploadSessionPort.begin_upload` time, or
    ``None`` when not specified."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class UploadPart:
    """One part of a multipart upload.

    Parts are 1-indexed and may be uploaded **in parallel and out of order**.
    On completion the backend assembles them in ascending :attr:`part_number`
    order regardless of upload order.

    **Backend divergence (internal).** On S3 native multipart each uploaded
    part returns an :attr:`etag` (in the ``PUT`` response header) that
    ``CompleteMultipartUpload`` requires — so the application must carry the
    ETag back from the client. On GCS compose-based multipart parts are
    addressed by name/order, not ETag, so :attr:`etag` is unused there and may
    be empty. :attr:`size` is informational (populated by
    :meth:`StorageUploadSessionPort.list_parts` on resume; ignored on the way
    in).
    """

    part_number: int = attrs.field(validator=attrs.validators.ge(1))
    """1-indexed position of this part (``>= 1``). Parts assemble in ascending
    order on completion."""

    etag: str = ""
    """Entity tag the backend returned for this uploaded part (S3: required by
    ``CompleteMultipartUpload``, carried back from the client's part ``PUT``
    response; GCS: unused, may be empty)."""

    size: int = 0
    """Part size in bytes when known (surfaced by :meth:`list_parts` on resume);
    ``0`` when unknown or not yet uploaded."""
