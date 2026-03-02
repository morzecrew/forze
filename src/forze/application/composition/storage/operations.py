"""Storage operation identifiers for object storage usecases."""

from enum import StrEnum
from typing import final

# ----------------------- #


@final
class StorageOperation(StrEnum):
    """Logical operation identifiers for storage usecases.

    Used as keys in registries when wiring storage-related usecases (upload,
    list, download, delete). Values are dot-prefixed for namespacing.
    """

    UPLOAD = "storage.upload"
    """Upload an object to a bucket."""

    LIST = "storage.list"
    """List objects in a bucket."""

    DOWNLOAD = "storage.download"
    """Download an object from a bucket."""

    DELETE = "storage.delete"
    """Delete an object from a bucket."""
