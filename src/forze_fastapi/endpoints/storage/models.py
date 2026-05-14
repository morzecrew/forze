from fastapi import UploadFile
from pydantic import BaseModel

from forze.domain.models import BaseDTO

# ----------------------- #


class StorageObjectKeyPath(BaseDTO):
    """Path parameters for routes that address one object by storage key."""

    key: str
    """Object key; use a ``{key:path}`` path template when keys may contain ``/``."""


# ....................... #


class StorageUploadFormBody(BaseModel):
    """Multipart form body for HTTP uploads (maps to :class:`~forze.application.dto.UploadObjectRequestDTO`)."""

    file: UploadFile
    """File payload."""

    description: str | None = None
    """Optional description."""

    prefix: str | None = None
    """Optional key prefix (folder-like namespace)."""
