"""Stored-file request and response DTOs."""

from uuid import UUID

from forze.application.contracts.querying import (
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.domain.models import BaseDTO
from forze_kits.domain.stored_file import StoredFileRead
from forze_kits.dto.paginated import Pagination

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

    filters: QueryFilterExpression | None = None  # type: ignore[valid-type]
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
