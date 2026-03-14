from typing import Generic, Optional, TypeVar

import attrs

from forze.application.execution import UsecasesFacade, facade_op
from forze.application.usecases.storage import (
    DeleteObject,
    DeleteObjectArgs,
    DownloadObject,
    DownloadObjectArgs,
    ListObjects,
    ListObjectsArgs,
    UploadObject,
    UploadObjectArgs,
)

from .operations import StorageOperation

# ----------------------- #

U = TypeVar("U", bound=UploadObjectArgs, default=UploadObjectArgs)
L = TypeVar("L", bound=ListObjectsArgs, default=ListObjectsArgs)
D = TypeVar("D", bound=DownloadObjectArgs, default=DownloadObjectArgs)
X = TypeVar("X", bound=DeleteObjectArgs, default=DeleteObjectArgs)


@attrs.define(slots=True, kw_only=True, frozen=True)
class StorageDTOs(Generic[U, L, D, X]):
    """DTO type mapping for storage operations."""

    upload: Optional[type[U]] = None
    """Upload request DTO type."""

    list: Optional[type[L]] = None
    """List request DTO type."""

    download: Optional[type[D]] = None
    """Download request DTO type."""

    delete: Optional[type[X]] = None
    """Delete request DTO type."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class StorageUsecasesFacade(UsecasesFacade, Generic[U, L, D, X]):
    """Typed facade for storage usecases."""

    upload = facade_op(StorageOperation.UPLOAD, uc=UploadObject[U])
    """Upload object usecase."""

    list = facade_op(StorageOperation.LIST, uc=ListObjects[L])
    """List objects usecase."""

    download = facade_op(StorageOperation.DOWNLOAD, uc=DownloadObject[D])
    """Download object usecase."""

    delete = facade_op(StorageOperation.DELETE, uc=DeleteObject[X])
    """Delete object usecase."""
