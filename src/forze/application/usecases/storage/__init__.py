"""Storage operation identifiers for storage usecases.

Provides :class:`StorageOperation` enum used to route storage-related workflows.
Concrete storage usecases (upload, list, download, delete) are wired elsewhere.
"""

from enum import StrEnum

# ----------------------- #


class StorageOperation(StrEnum):
    """Logical operation identifiers for storage usecases.

    Used to dispatch storage workflows (e.g. in routers or registries) to the
    appropriate handler. Values match common object-storage operations.
    """

    UPLOAD = "upload"
    """Upload a file to a bucket."""

    LIST = "list"
    """List objects in a bucket."""

    DOWNLOAD = "download"
    """Download an object from a bucket."""

    DELETE = "delete"
    """Delete an object from a bucket."""
