"""Stored-file composition: facades, factories, wiring, and operation identifiers."""

from forze_kits.domain.stored_file.events import StoredFileOutboxPayload

from .dto import (
    DownloadStoredFileRangeRequestDTO,
    ListStoredFilesRequestDTO,
    PresignStoredFileDownloadRequestDTO,
    StoredFileDownloadDTO,
    StoredFileIdDTO,
    StoredFileIdRevDTO,
    StoredFilePresignedDownloadDTO,
    StoredFileRangeDTO,
    StoredFileStream,
    UploadStoredFileRequestDTO,
)
from .facades import StoredFileFacade
from .factories import build_stored_file_registry
from .handlers import (
    DownloadStoredFile,
    DownloadStoredFileRange,
    DownloadStoredFileStream,
    GetStoredFile,
    ListStoredFiles,
    PresignStoredFileDownload,
    SearchStoredFiles,
    SoftDeleteStoredFile,
    UploadStoredFile,
)
from .operations import StoredFileKernelOp
from .stages import (
    stored_file_complete_upload_after_commit_factory,
    stored_file_outbox_flush_factory,
    stored_file_purge_blob_after_commit_factory,
)
from .wiring import bind_stored_file_writes, freeze_stored_file_registry

# ----------------------- #

__all__ = [
    "StoredFileFacade",
    "StoredFileKernelOp",
    "StoredFileOutboxPayload",
    "bind_stored_file_writes",
    "build_stored_file_registry",
    "freeze_stored_file_registry",
    "stored_file_complete_upload_after_commit_factory",
    "stored_file_outbox_flush_factory",
    "stored_file_purge_blob_after_commit_factory",
    "DownloadStoredFileRangeRequestDTO",
    "ListStoredFilesRequestDTO",
    "PresignStoredFileDownloadRequestDTO",
    "StoredFileDownloadDTO",
    "StoredFileIdDTO",
    "StoredFileIdRevDTO",
    "StoredFilePresignedDownloadDTO",
    "StoredFileRangeDTO",
    "StoredFileStream",
    "UploadStoredFileRequestDTO",
    "UploadStoredFile",
    "SearchStoredFiles",
    "SoftDeleteStoredFile",
    "GetStoredFile",
    "DownloadStoredFile",
    "DownloadStoredFileRange",
    "DownloadStoredFileStream",
    "PresignStoredFileDownload",
    "ListStoredFiles",
]
