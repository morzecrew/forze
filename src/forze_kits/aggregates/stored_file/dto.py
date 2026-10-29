"""Stored-file request and response DTOs."""

from datetime import timedelta
from uuid import UUID

import attrs
from pydantic import Field

from forze.application.contracts.querying import QuerySortExpression
from forze.application.contracts.storage import StreamedDownload
from forze.domain.models import BaseDTO
from forze_kits.aggregates.storage.dto import PresignedUrlDTO
from forze_kits.domain.stored_file import StoredFileRead
from forze_kits.dto.paginated import Pagination
from forze_kits.dto.querying import OptionalFilterExpression

# ----------------------- #


class StoredFileIdDTO(BaseDTO):
    """Primary key for a stored-file row."""

    id: UUID
    """Stored-file document id."""


# ....................... #


class StoredFileIdRevDTO(StoredFileIdDTO):
    """Primary key and revision for optimistic updates."""

    rev: int
    """Expected revision."""


# ....................... #


class UploadStoredFileRequestDTO(BaseDTO):
    """Request payload for uploading a stored file."""

    filename: str
    """Original filename."""

    data: bytes
    """Raw bytes to store."""

    description: str | None = None
    """Optional description."""

    prefix: str | None = None
    """Optional logical key prefix."""

    tags: dict[str, str] | None = None
    """Optional tags."""


# ....................... #


class ListStoredFilesRequestDTO(Pagination):
    """List stored files with optional filters."""

    prefix: str | None = None
    """Filter by exact logical prefix."""

    include_deleted: bool = False
    """When ``False`` (default), exclude soft-deleted rows."""

    include_pending: bool = True
    """When ``False``, exclude ``pending`` and ``failed`` rows."""

    filters: OptionalFilterExpression = None
    """Additional filter expression merged with kit defaults."""

    sorts: QuerySortExpression | None = None
    """Optional sort expression."""


# ....................... #


class StoredFileDownloadDTO(BaseDTO):
    """Download response with blob bytes and metadata."""

    file: StoredFileRead
    """Stored-file metadata."""

    data: bytes
    """Downloaded object bytes."""

    content_type: str
    """MIME type of the downloaded data."""

    filename: str
    """Original filename."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class StoredFileStream:
    """A ready stored file's metadata and a bounded-memory stream of its bytes.

    Not a JSON DTO: :attr:`stream` carries a single-use async iterator that the transport
    (e.g. a FastAPI ``StreamingResponse``) consumes after the operation returns.
    """

    file: StoredFileRead
    """Stored-file metadata."""

    stream: StreamedDownload
    """The object's chunks, content type and cache validators; its ``filename`` is the stored
    file's."""


# ....................... #


class DownloadStoredFileRangeRequestDTO(StoredFileIdDTO):
    """Request payload for reading an inclusive byte range of a stored file."""

    start: int = Field(ge=0)
    """First byte offset to read (inclusive)."""

    end: int | None = Field(default=None, ge=0)
    """Last byte offset to read (inclusive); ``None`` reads to the end of the file."""


# ....................... #


class StoredFileRangeDTO(BaseDTO):
    """An inclusive byte range of a ready stored file, with its metadata."""

    file: StoredFileRead
    """Stored-file metadata; its ``filename`` is the name to save the download under."""

    data: bytes
    """The range's bytes."""

    content_type: str
    """MIME type of the file."""

    content_range: str
    """The satisfied range as an HTTP ``Content-Range`` value (``bytes start-end/total``)."""

    total_size: int
    """The whole file's size in bytes."""


# ....................... #


class PresignStoredFileDownloadRequestDTO(StoredFileIdDTO):
    """Request payload for minting a presigned download (GET) URL for a stored file."""

    expires_in: timedelta = Field(gt=timedelta(0), le=timedelta(days=7))
    """How long the minted URL stays valid (positive; backends cap at 7 days)."""


# ....................... #


class StoredFilePresignedDownloadDTO(BaseDTO):
    """A presigned download URL for a ready stored file, with its metadata.

    :attr:`presigned` holds a bearer credential: return it to the client, never log it (see
    :class:`~forze_kits.aggregates.storage.PresignedUrlDTO`).
    """

    file: StoredFileRead
    """Stored-file metadata; its ``filename`` is the name to save the download under."""

    presigned: PresignedUrlDTO
    """The minted URL, its method, expiry and the headers the client must send."""
