"""Storage composition: facades, factories, and operation identifiers."""

from .dto import (
    ListedObjects,
    ListObjectsRequestDTO,
    StoredObjectDTO,
    UploadObjectRequestDTO,
)
from .facades import StorageFacade
from .factories import build_storage_registry
from .handlers import DeleteObject, DownloadObject, ListObjects, UploadObject
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
    "DeleteObject",
    "DownloadObject",
    "ListObjects",
    "UploadObject",
]
