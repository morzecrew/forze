"""Storage-specific request and response DTOs."""

from datetime import datetime, timedelta

from pydantic import Field

from forze.domain.models import BaseDTO
from forze_kits.dto.paginated import Pagination

# ----------------------- #


class UploadObjectRequestDTO(BaseDTO):
    """Request payload for uploading an object to storage."""

    filename: str
    """Original filename for the object."""

    data: bytes
    """Raw bytes payload to store."""

    description: str | None = None
    """Optional human-readable description."""

    tags: dict[str, str] | None = None
    """Optional tags to associate with the object."""

    prefix: str | None = None
    """Optional key prefix (folder-like namespace)."""


# ....................... #


class ListObjectsRequestDTO(Pagination):
    """Request payload for listing objects in storage."""

    prefix: str | None = None
    """Optional key prefix filter."""


# ....................... #


class StoredObjectDTO(BaseDTO):
    """DTO for a stored object returned over HTTP or handlers."""

    key: str
    filename: str
    created_at: datetime
    size: int
    content_type: str
    description: str | None = None
    tags: dict[str, str] | None = None


# ....................... #


class ListedObjects(BaseDTO):
    """Paginated listing response for storage objects."""

    hits: list[StoredObjectDTO]
    """Objects for the current page."""

    page: int
    """One-based page number."""

    size: int
    """Page size (number of records per page)."""

    count: int
    """Total number of matching objects."""


# ....................... #
# Presigned-URL DTOs (direct upload/download, app out of the data path).


class PresignDownloadRequestDTO(BaseDTO):
    """Request payload for minting a presigned download (GET) URL."""

    key: str
    """Storage key of the object to download."""

    expires_in: timedelta = Field(gt=timedelta(0), le=timedelta(days=7))
    """How long the minted URL stays valid (positive; backends cap at 7 days)."""


# ....................... #


class PresignUploadRequestDTO(BaseDTO):
    """Request payload for minting a presigned upload (PUT) URL."""

    key: str
    """Storage key to upload to."""

    expires_in: timedelta = Field(gt=timedelta(0), le=timedelta(days=7))
    """How long the minted URL stays valid (positive; backends cap at 7 days)."""

    content_type: str | None = None
    """Optional MIME type to bind into the signature (echoed in ``headers``)."""


# ....................... #


class PresignedUrlDTO(BaseDTO):
    """A minted presigned URL returned to the client.

    .. warning::

        :attr:`url` **is a bearer credential** — anyone holding it can perform
        :attr:`method` on the object until :attr:`expires_at`. It is part of the
        response body the client needs, but it must **never be logged**. The
        access-log middleware logs only request path/status/duration (never the
        response body), so the URL does not leak there; do not add it to any
        descriptor example, trace attribute, or log line. Prefer short
        ``expires_in`` windows.
    """

    url: str
    """The presigned URL — the bearer credential. Returned to the client, never
    logged (see the class warning)."""

    method: str
    """HTTP method the signature authorizes (``GET`` / ``PUT``)."""

    expires_at: datetime
    """UTC instant after which the URL stops verifying."""

    headers: dict[str, str]
    """Headers the client **must** send verbatim with the request."""


# ....................... #
# Multipart resumable-upload DTOs (the client round-trips the session handle).


class UploadSessionDTO(BaseDTO):
    """A multipart upload session handle round-tripped between app and client.

    The :attr:`upload_id` is the resume/complete credential the client carries
    back into every later call (presign-part / list-parts / complete / abort).
    Treat it as a secret: it grants the ability to add and complete parts on
    :attr:`key`.
    """

    key: str
    """Final object key the assembled upload lands at."""

    upload_id: str
    """Opaque, backend-specific upload credential — persist/round-trip it to
    resume, complete, or abort the session later (not otherwise recoverable)."""

    bucket: str | None = None
    """Resolved physical bucket the upload targets, when surfaced."""

    content_type: str | None = None
    """MIME type bound at begin time, or ``None`` when not specified."""


# ....................... #


class UploadPartDTO(BaseDTO):
    """One multipart part the client carries back from its direct part PUT."""

    part_number: int = Field(ge=1)
    """1-indexed position of this part (``>= 1``)."""

    etag: str = ""
    """Entity tag the backend returned for the part (S3 requires it on complete;
    GCS leaves it empty)."""

    size: int = 0
    """Part size in bytes when known (surfaced by list-parts on resume)."""


# ....................... #


class PresignPartRequestDTO(BaseDTO):
    """Request payload for minting a presigned URL for one multipart part."""

    session: UploadSessionDTO
    """The session handle returned by ``begin_upload`` (round-tripped)."""

    part_number: int = Field(ge=1)
    """1-indexed part position to presign (``>= 1``)."""

    expires_in: timedelta = Field(gt=timedelta(0), le=timedelta(days=7))
    """How long the part-upload URL stays valid (positive; capped at 7 days)."""


# ....................... #


class UploadSessionRequestDTO(BaseDTO):
    """Request payload carrying just a session handle (list-parts / abort)."""

    session: UploadSessionDTO
    """The session handle returned by ``begin_upload`` (round-tripped)."""


# ....................... #


class CompleteUploadRequestDTO(BaseDTO):
    """Request payload for completing a multipart upload."""

    session: UploadSessionDTO
    """The session handle returned by ``begin_upload`` (round-tripped)."""

    parts: list[UploadPartDTO]
    """The parts to assemble (ascending ``part_number``; S3 needs the etags)."""


# ....................... #


class BeginUploadRequestDTO(BaseDTO):
    """Request payload for opening a multipart upload session."""

    key: str
    """Final object key the assembled upload lands at."""

    content_type: str | None = None
    """Optional MIME type bound to the final object."""


# ....................... #


class ListedPartsDTO(BaseDTO):
    """The parts already uploaded for a session (resume primitive)."""

    parts: list[UploadPartDTO]
    """The already-uploaded parts (ascending ``part_number``)."""


# ....................... #


class ObjectHeadDTO(BaseDTO):
    """Honest head view of the assembled object returned on completion."""

    content_type: str
    """MIME content type of the object."""

    size: int
    """Content length in bytes."""

    etag: str
    """Entity tag (empty when the backend surfaces none)."""

    last_modified: datetime | None = None
    """Backend last-modification timestamp, or ``None`` when unavailable."""

    metadata: dict[str, str] = Field(default_factory=dict)
    """User-defined metadata key-value pairs as the backend stores them."""

    tags: dict[str, str] = Field(default_factory=dict)
    """Object tags (population follows the backend's ``include_tags`` guarantee)."""
