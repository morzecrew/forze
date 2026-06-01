"""Storage-specific request and response DTOs."""

from datetime import datetime

from forze_kits.dto.paginated import Pagination
from forze.domain.models import BaseDTO

# ----------------------- #


class UploadObjectRequestDTO(BaseDTO):
    """Request payload for uploading an object to storage."""

    filename: str
    """Original filename for the object."""

    data: bytes
    """Raw bytes payload to store."""

    description: str | None = None
    """Optional human-readable description."""

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
