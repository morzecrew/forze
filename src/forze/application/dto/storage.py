"""Storage-specific request DTOs."""


from forze.domain.models import BaseDTO

from .paginated import Pagination

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
