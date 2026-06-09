"""Outbox payload models for stored-file integration events."""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel

from .models import StoredFileRead, StoredFileStatus

# ----------------------- #


class StoredFileOutboxPayload(BaseModel):
    """Superset outbox payload for all stored-file integration events.

    Consumers should use ``event_type`` to interpret which fields are set.
    """

    file_id: UUID
    """Stored-file document id."""

    filename: str | None = None
    """Original filename."""

    prefix: str | None = None
    """Logical key prefix."""

    storage_key: str | None = None
    """Object storage key when available."""

    size: int | None = None
    """Size in bytes."""

    content_type: str | None = None
    """MIME type."""

    status: StoredFileStatus | None = None
    """Lifecycle status at event time."""

    # ....................... #

    @classmethod
    def from_read(cls, file: StoredFileRead) -> StoredFileOutboxPayload:
        """Build payload from a :class:`StoredFileRead`."""

        return cls(
            file_id=file.id,
            filename=file.filename,
            prefix=file.prefix,
            storage_key=file.storage_key,
            size=file.size,
            content_type=file.content_type,
            status=file.status,
        )
