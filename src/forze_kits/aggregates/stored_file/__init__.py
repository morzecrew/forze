"""Stored-file composition: facades, factories, wiring, and operation identifiers."""

from forze_kits.domain.stored_file.events import StoredFileOutboxPayload

from .dto import (
    CompleteStoredFileUploadRequestDTO,
    DownloadStoredFileRangeRequestDTO,
    InitiateStoredFileUploadRequestDTO,
    ListStoredFilesRequestDTO,
    PresignStoredFileDownloadRequestDTO,
    PresignStoredFileUploadPartRequestDTO,
    StoredFileDownloadDTO,
    StoredFileIdDTO,
    StoredFileIdRevDTO,
    StoredFilePresignedDownloadDTO,
    StoredFileRangeDTO,
    StoredFileStream,
    StoredFileUploadSessionDTO,
    UploadStoredFileRequestDTO,
)
from .facades import StoredFileFacade
from .factories import build_stored_file_registry
from .handlers import (
    CompleteStoredFileUpload,
    DownloadStoredFile,
    DownloadStoredFileRange,
    DownloadStoredFileStream,
    GetStoredFile,
    InitiateStoredFileUpload,
    ListStoredFiles,
    PresignStoredFileDownload,
    PresignStoredFileUploadPart,
    SearchStoredFiles,
    SoftDeleteStoredFile,
    UploadStoredFile,
)
from .lifecycle import DEFAULT_PENDING_MAX_AGE, stored_file_pending_sweep_lifecycle_step
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
    "DEFAULT_PENDING_MAX_AGE",
    "bind_stored_file_writes",
    "build_stored_file_registry",
    "freeze_stored_file_registry",
    "stored_file_complete_upload_after_commit_factory",
    "stored_file_outbox_flush_factory",
    "stored_file_pending_sweep_lifecycle_step",
    "stored_file_purge_blob_after_commit_factory",
    "CompleteStoredFileUploadRequestDTO",
    "DownloadStoredFileRangeRequestDTO",
    "InitiateStoredFileUploadRequestDTO",
    "ListStoredFilesRequestDTO",
    "PresignStoredFileDownloadRequestDTO",
    "PresignStoredFileUploadPartRequestDTO",
    "StoredFileDownloadDTO",
    "StoredFileIdDTO",
    "StoredFileIdRevDTO",
    "StoredFilePresignedDownloadDTO",
    "StoredFileRangeDTO",
    "StoredFileStream",
    "StoredFileUploadSessionDTO",
    "UploadStoredFileRequestDTO",
    "UploadStoredFile",
    "SearchStoredFiles",
    "SoftDeleteStoredFile",
    "GetStoredFile",
    "DownloadStoredFile",
    "DownloadStoredFileRange",
    "DownloadStoredFileStream",
    "PresignStoredFileDownload",
    "InitiateStoredFileUpload",
    "PresignStoredFileUploadPart",
    "CompleteStoredFileUpload",
    "ListStoredFiles",
]
