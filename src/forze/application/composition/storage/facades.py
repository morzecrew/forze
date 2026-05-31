import attrs

from forze.application.execution.operations.facade import (
    OperationFacade,
    facade_op,
    namespaced_facade,
)
from forze.application.handlers.storage import (
    DeleteObject,
    DownloadObject,
    ListObjects,
    UploadObject,
)

from .operations import StorageKernelOp

# ----------------------- #


@namespaced_facade
@attrs.define(slots=True, kw_only=True, frozen=True)
class StorageFacade(OperationFacade):
    """Typed facade for storage usecases."""

    upload = facade_op(
        StorageKernelOp.UPLOAD,
        uc=UploadObject,
    )
    """Upload object usecase."""

    list = facade_op(
        StorageKernelOp.LIST,
        uc=ListObjects,
    )
    """List objects usecase."""

    download = facade_op(
        StorageKernelOp.DOWNLOAD,
        uc=DownloadObject,
    )
    """Download object usecase."""

    delete = facade_op(
        StorageKernelOp.DELETE,
        uc=DeleteObject,
    )
    """Delete object usecase."""
