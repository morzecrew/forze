"""Storage operation kernel suffixes for usecase registration and resolution."""

from enum import StrEnum
from typing import final

# ----------------------- #


@final
class StorageKernelOp(StrEnum):
    """Kernel segments (suffix only) for storage usecase operation keys."""

    UPLOAD = "upload"
    """Upload an object to a bucket."""

    LIST = "list"
    """List objects in a bucket."""

    DOWNLOAD = "download"
    """Download an object from a bucket."""

    DELETE = "delete"
    """Delete an object from a bucket."""
