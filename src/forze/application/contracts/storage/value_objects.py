from datetime import datetime
from typing import Literal, Mapping, final

import attrs
import msgspec

# ----------------------- #


class _InternalMetadata(msgspec.Struct):
    """Optional metadata for an object."""

    filename: str
    """Original filename associated with the object."""

    created_at: datetime
    """Backend timestamp when the object was created."""

    size: int
    """Object size in bytes."""


# ....................... #


class UploadedObject(msgspec.Struct):
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


class DownloadedObject(msgspec.Struct):
    """Value object for a downloaded object."""

    data: bytes
    """Raw object payload."""

    content_type: str
    """MIME content type of the downloaded data."""

    filename: str
    """Original filename associated with the downloaded data."""


# ....................... #


class ObjectMetadata(_InternalMetadata):
    """Value object for an object metadata."""

    description: str | None = None
    """Optional human-readable description."""


# ....................... #


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
