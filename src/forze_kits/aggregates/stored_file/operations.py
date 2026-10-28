"""Stored-file operation kernel suffixes."""

from enum import StrEnum
from typing import final

# ----------------------- #


@final
class StoredFileKernelOp(StrEnum):
    """Kernel segments (suffix only) for stored-file usecase operation keys."""

    UPLOAD = "upload"
    """Create a pending stored-file row (blob upload runs after commit)."""

    DOWNLOAD = "download"
    """Download blob bytes for a ready stored file (whole body, fully buffered)."""

    DOWNLOAD_STREAM = "download_stream"
    """Open a bounded-memory download stream for a ready stored file."""

    PRESIGN_DOWNLOAD = "presign_download"
    """Mint a time-limited URL for downloading a ready stored file directly (read grant)."""

    DELETE = "delete"
    """Soft-delete a stored file."""

    GET = "get"
    """Fetch stored-file metadata."""

    LIST = "list"
    """List stored files with optional prefix filter."""

    SEARCH = "search"
    """Full-text search over filename and description."""
