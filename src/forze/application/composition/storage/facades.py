import attrs

from forze.application.execution import (
    FacadeOperationDescriptor,
    UsecasesFacade,
    namespaced_facade,
)
from forze.application.usecases.storage import (
    DeleteObject,
    DownloadObject,
    ListObjects,
    UploadObject,
)

from .operations import StorageKernelOp

# ----------------------- #


@namespaced_facade
@attrs.define(slots=True, kw_only=True, frozen=True)
class StorageUsecasesFacade(UsecasesFacade):
    """Typed facade for storage usecases."""

    upload = FacadeOperationDescriptor(
        StorageKernelOp.UPLOAD,
        uc=UploadObject,
    )
    """Upload object usecase."""

    list = FacadeOperationDescriptor(
        StorageKernelOp.LIST,
        uc=ListObjects,
    )
    """List objects usecase."""

    download = FacadeOperationDescriptor(
        StorageKernelOp.DOWNLOAD,
        uc=DownloadObject,
    )
    """Download object usecase."""

    delete = FacadeOperationDescriptor(
        StorageKernelOp.DELETE,
        uc=DeleteObject,
    )
    """Delete object usecase."""
