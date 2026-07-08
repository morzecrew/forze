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
    """Download an object from a bucket (whole body, fully buffered)."""

    HEAD = "head"
    """Fetch an object's metadata (size / etag / content-type / last-modified) without its body."""

    DOWNLOAD_STREAM = "download_stream"
    """Open a bounded-memory download stream for an object (no whole-object buffering)."""

    DOWNLOAD_RANGE = "download_range"
    """Fetch a byte range of an object via a backend-ranged read (powers HTTP Range requests)."""

    DELETE = "delete"
    """Delete an object from a bucket."""

    PRESIGN_DOWNLOAD = "presign_download"
    """Mint a time-limited URL for downloading an object directly (read grant)."""

    PRESIGN_UPLOAD = "presign_upload"
    """Mint a time-limited URL for uploading an object directly (write grant)."""

    BEGIN_UPLOAD = "begin_upload"
    """Open a resumable multipart upload session."""

    PRESIGN_PART = "presign_part"
    """Mint a time-limited URL for uploading one multipart part directly."""

    LIST_PARTS = "list_parts"
    """List the parts already uploaded for a multipart session (resume primitive)."""

    COMPLETE_UPLOAD = "complete_upload"
    """Assemble the uploaded parts into the final object."""

    ABORT_UPLOAD = "abort_upload"
    """Discard an unfinished multipart upload session."""
