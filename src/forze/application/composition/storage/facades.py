from typing import Generic, TypeVar

import attrs

from forze.application.dto import ListObjectsRequestDTO, UploadObjectRequestDTO
from forze.application.execution import UsecasesFacade, facade_op
from forze.application.usecases.storage import (
    DeleteObject,
    DownloadObject,
    ListObjects,
    UploadObject,
)

from .operations import StorageOperation

# ----------------------- #

U = TypeVar("U", bound=UploadObjectRequestDTO, default=UploadObjectRequestDTO)
L = TypeVar("L", bound=ListObjectsRequestDTO, default=ListObjectsRequestDTO)


@attrs.define(slots=True, kw_only=True, frozen=True)
class StorageDTOs(Generic[U, L]):
    """DTO type mapping for storage operations."""

    upload: type[U] | None = attrs.field(default=None)
    """Upload request DTO type."""

    list: type[L] | None = attrs.field(default=None)
    """List request DTO type."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class StorageUsecasesFacade(UsecasesFacade, Generic[U, L]):
    """Typed facade for storage usecases."""

    upload = facade_op(StorageOperation.UPLOAD, uc=UploadObject)
    """Upload object usecase."""

    list = facade_op(StorageOperation.LIST, uc=ListObjects)
    """List objects usecase."""

    download = facade_op(StorageOperation.DOWNLOAD, uc=DownloadObject)
    """Download object usecase."""

    delete = facade_op(StorageOperation.DELETE, uc=DeleteObject)
    """Delete object usecase."""
