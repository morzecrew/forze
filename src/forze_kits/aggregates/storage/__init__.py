"""Storage composition: facades, factories, and operation identifiers."""

from .dto import (
    BeginUploadRequestDTO,
    CompleteUploadRequestDTO,
    ListedObjects,
    ListedPartsDTO,
    ListObjectsRequestDTO,
    ObjectHeadDTO,
    PresignDownloadRequestDTO,
    PresignedUrlDTO,
    PresignPartRequestDTO,
    PresignUploadRequestDTO,
    StoredObjectDTO,
    UploadObjectRequestDTO,
    UploadPartDTO,
    UploadSessionDTO,
    UploadSessionRequestDTO,
)
from .facades import StorageFacade
from .factories import build_storage_registry
from .handlers import (
    AbortUpload,
    BeginUpload,
    CompleteUpload,
    DeleteObject,
    DownloadObject,
    ListObjects,
    ListParts,
    PresignDownload,
    PresignPart,
    PresignUpload,
    UploadObject,
)
from .operations import StorageKernelOp

# ----------------------- #

__all__ = [
    "StorageFacade",
    "StorageKernelOp",
    "build_storage_registry",
    "UploadObjectRequestDTO",
    "ListObjectsRequestDTO",
    "ListedObjects",
    "StoredObjectDTO",
    "PresignDownloadRequestDTO",
    "PresignUploadRequestDTO",
    "PresignedUrlDTO",
    "BeginUploadRequestDTO",
    "PresignPartRequestDTO",
    "UploadSessionRequestDTO",
    "CompleteUploadRequestDTO",
    "UploadSessionDTO",
    "UploadPartDTO",
    "ListedPartsDTO",
    "ObjectHeadDTO",
    "DeleteObject",
    "DownloadObject",
    "ListObjects",
    "UploadObject",
    "PresignDownload",
    "PresignUpload",
    "BeginUpload",
    "PresignPart",
    "ListParts",
    "CompleteUpload",
    "AbortUpload",
]
