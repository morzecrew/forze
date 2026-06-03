"""Fixed domain, read, and command models for the stored-file kit."""

from datetime import datetime
from enum import StrEnum
from typing import final
from uuid import UUID

from pydantic import Field

from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

# ----------------------- #


@final
class StoredFileStatus(StrEnum):
    """Lifecycle status of a stored-file document row."""

    PENDING = "pending"
    """Metadata row exists; blob upload not yet completed."""

    READY = "ready"
    """Blob is stored and metadata is complete."""

    FAILED = "failed"
    """Blob upload or ready transition failed after the pending row was committed."""

    DELETED = "deleted"
    """Soft-deleted; blob purge may still be in progress or completed."""


# ....................... #


class StoredFileDocument(Document):
    """Domain model for a stored-file aggregate."""

    filename: str
    """Original filename."""

    content_type: str = "application/octet-stream"
    """MIME type; refined after blob upload when ready."""

    size: int = 0
    """Object size in bytes."""

    storage_key: str | None = None
    """Opaque object-storage key; set when status becomes ``ready``."""

    prefix: str | None = None
    """Optional logical key prefix (folder-like namespace)."""

    description: str | None = None
    """Optional human-readable description."""

    tags: dict[str, str] | None = None
    """Optional string tags."""

    status: StoredFileStatus = StoredFileStatus.PENDING
    """Current lifecycle status."""


# ....................... #


class StoredFileRead(BaseDTO):
    """Read model for a stored-file aggregate."""

    id: UUID
    """Document primary key."""

    rev: int
    """Optimistic revision."""

    created_at: datetime
    """Creation timestamp."""

    last_update_at: datetime
    """Last update timestamp."""

    filename: str
    """Original filename."""

    content_type: str
    """MIME type."""

    size: int
    """Object size in bytes."""

    storage_key: str | None = None
    """Opaque object-storage key when ``ready``."""

    prefix: str | None = None
    """Optional logical key prefix."""

    description: str | None = None
    """Optional description."""

    tags: dict[str, str] | None = None
    """Optional tags."""

    status: StoredFileStatus
    """Current lifecycle status."""


# ....................... #


class StoredFileCreateCmd(CreateDocumentCmd):
    """Create command for a pending stored-file row."""

    filename: str
    """Original filename."""

    content_type: str = Field(default="application/octet-stream")
    """MIME type placeholder until upload completes."""

    size: int = 0
    """Expected size in bytes (typically ``len(data)`` from the upload request)."""

    prefix: str | None = None
    """Optional logical key prefix."""

    description: str | None = None
    """Optional description."""

    tags: dict[str, str] | None = None
    """Optional tags."""

    status: StoredFileStatus = StoredFileStatus.PENDING
    """Initial status; always ``pending`` for kit uploads."""


# ....................... #


class StoredFileUpdateCmd(BaseDTO):
    """Update command for stored-file lifecycle transitions."""

    filename: str | None = None
    """Optional filename change (rare; kit defaults to immutable after create)."""

    content_type: str | None = None
    """MIME type after upload."""

    size: int | None = None
    """Size after upload."""

    storage_key: str | None = None
    """Storage key after upload."""

    prefix: str | None = None
    """Optional prefix update."""

    description: str | None = None
    """Optional description update."""

    tags: dict[str, str] | None = None
    """Optional tags update."""

    status: StoredFileStatus | None = None
    """Lifecycle status transition."""
