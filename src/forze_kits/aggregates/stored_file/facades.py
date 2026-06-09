import attrs

from forze.application.execution.operations.facade import (
    OperationFacade,
    facade_op,
    namespaced_facade,
)

from .handlers import (
    DownloadStoredFile,
    GetStoredFile,
    ListStoredFiles,
    SearchStoredFiles,
    SoftDeleteStoredFile,
    UploadStoredFile,
)
from .operations import StoredFileKernelOp

# ----------------------- #


@namespaced_facade
@attrs.define(slots=True, kw_only=True, frozen=True)
class StoredFileFacade(OperationFacade):
    """Typed facade for stored-file use cases."""

    upload = facade_op(
        StoredFileKernelOp.UPLOAD,
        uc=UploadStoredFile,
    )
    """Upload a file (pending row; blob completes after commit)."""

    download = facade_op(
        StoredFileKernelOp.DOWNLOAD,
        uc=DownloadStoredFile,
    )
    """Download blob bytes."""

    delete = facade_op(
        StoredFileKernelOp.DELETE,
        uc=SoftDeleteStoredFile,
    )
    """Soft-delete a stored file."""

    get = facade_op(
        StoredFileKernelOp.GET,
        uc=GetStoredFile,
    )
    """Fetch metadata."""

    list = facade_op(
        StoredFileKernelOp.LIST,
        uc=ListStoredFiles,
    )
    """List stored files."""

    search = facade_op(
        StoredFileKernelOp.SEARCH,
        uc=SearchStoredFiles,
    )
    """Search by filename and description."""
