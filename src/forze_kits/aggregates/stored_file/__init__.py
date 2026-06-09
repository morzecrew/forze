"""Stored-file composition: facades, factories, wiring, and operation identifiers."""

from forze_kits.domain.stored_file.events import StoredFileOutboxPayload

from .dto import (
    ListStoredFilesRequestDTO,
    StoredFileDownloadDTO,
    StoredFileIdDTO,
    StoredFileIdRevDTO,
    UploadStoredFileRequestDTO,
)
from .facades import StoredFileFacade
from .factories import build_stored_file_registry
from .handlers import (
    DownloadStoredFile,
    GetStoredFile,
    ListStoredFiles,
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
from .wiring import freeze_stored_file_registry

# ----------------------- #

__all__ = [
    "StoredFileFacade",
    "StoredFileKernelOp",
    "StoredFileOutboxPayload",
    "build_stored_file_registry",
    "freeze_stored_file_registry",
    "stored_file_complete_upload_after_commit_factory",
    "stored_file_outbox_flush_factory",
    "stored_file_purge_blob_after_commit_factory",
    "ListStoredFilesRequestDTO",
    "StoredFileDownloadDTO",
    "StoredFileIdDTO",
    "StoredFileIdRevDTO",
    "UploadStoredFileRequestDTO",
    "UploadStoredFile",
    "SearchStoredFiles",
    "SoftDeleteStoredFile",
    "GetStoredFile",
    "DownloadStoredFile",
    "ListStoredFiles",
]
