"""Stored-file operation kernel suffixes."""

from enum import StrEnum
from typing import final

# ----------------------- #


@final
class StoredFileKernelOp(StrEnum):
    """Kernel segments (suffix only) for stored-file usecase operation keys."""

    UPLOAD = "upload"
    """Create a pending stored-file row (blob upload runs after commit)."""

    INITIATE_UPLOAD = "initiate_upload"
    """Create a pending row and hand back a direct upload: a presigned PUT or a multipart
    session (opt-in)."""

    PRESIGN_UPLOAD_PART = "presign_upload_part"
    """Mint a presigned PUT for one part of a direct multipart upload (opt-in)."""

    COMPLETE_UPLOAD = "complete_upload"
    """Verify a direct upload against the row and mark it ready, or fail it (opt-in)."""

    DOWNLOAD = "download"
    """Download blob bytes for a ready stored file (whole body, fully buffered)."""

    DOWNLOAD_STREAM = "download_stream"
    """Open a bounded-memory download stream for a ready stored file (opt-in)."""

    DOWNLOAD_RANGE = "download_range"
    """Read an inclusive byte range of a ready stored file (opt-in)."""

    PRESIGN_DOWNLOAD = "presign_download"
    """Mint a time-limited URL for downloading a ready stored file directly (opt-in)."""

    DELETE = "delete"
    """Soft-delete a stored file."""

    GET = "get"
    """Fetch stored-file metadata."""

    LIST = "list"
    """List stored files with optional prefix filter."""

    SEARCH = "search"
    """Full-text search over filename and description."""
