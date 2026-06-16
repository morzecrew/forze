import attrs

from forze.application.execution.operations.facade import (
    OperationFacade,
    facade_op,
    namespaced_facade,
)
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

    presign_download = facade_op(
        StorageKernelOp.PRESIGN_DOWNLOAD,
        uc=PresignDownload,
    )
    """Mint a presigned download URL usecase."""

    presign_upload = facade_op(
        StorageKernelOp.PRESIGN_UPLOAD,
        uc=PresignUpload,
    )
    """Mint a presigned upload URL usecase."""

    begin_upload = facade_op(
        StorageKernelOp.BEGIN_UPLOAD,
        uc=BeginUpload,
    )
    """Open a multipart upload session usecase."""

    presign_part = facade_op(
        StorageKernelOp.PRESIGN_PART,
        uc=PresignPart,
    )
    """Mint a presigned multipart-part URL usecase."""

    list_parts = facade_op(
        StorageKernelOp.LIST_PARTS,
        uc=ListParts,
    )
    """List multipart session parts usecase."""

    complete_upload = facade_op(
        StorageKernelOp.COMPLETE_UPLOAD,
        uc=CompleteUpload,
    )
    """Complete a multipart upload usecase."""

    abort_upload = facade_op(
        StorageKernelOp.ABORT_UPLOAD,
        uc=AbortUpload,
    )
    """Abort a multipart upload session usecase."""
