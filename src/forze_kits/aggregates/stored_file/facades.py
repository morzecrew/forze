from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import attrs

from forze.application.execution.operations.facade import (
    OperationFacade,
    facade_op,
    namespaced_facade,
)
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

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
from .operations import StoredFileKernelOp

if TYPE_CHECKING:
    from forze.application.execution.operations.run import ResolvedOperation

# ----------------------- #

_OPT_IN: Mapping[StrKey, str] = {
    StoredFileKernelOp.DOWNLOAD_STREAM: "stream_downloads",
    StoredFileKernelOp.DOWNLOAD_RANGE: "range_downloads",
    StoredFileKernelOp.PRESIGN_DOWNLOAD: "presign_downloads",
    StoredFileKernelOp.INITIATE_UPLOAD: "direct_uploads",
    StoredFileKernelOp.PRESIGN_UPLOAD_PART: "direct_uploads",
    StoredFileKernelOp.COMPLETE_UPLOAD: "direct_uploads",
}
"""Opt-in operations → the ``StoredFileKitSpec`` field that registers each."""


@namespaced_facade
@attrs.define(slots=True, kw_only=True, frozen=True)
class StoredFileFacade(OperationFacade):
    """Typed facade for stored-file use cases."""

    # ....................... #

    def resolve(self, op: StrKey) -> "ResolvedOperation[Any, Any]":
        """Resolve an operation, refusing an opt-in one the registry was built without."""

        key = self.namespace.key(op) if self.namespace is not None else op

        if op in _OPT_IN and key not in self.registry.handlers:
            raise exc.configuration(
                f"Stored-file operation {key!r} is not registered: build the registry from "
                f"StoredFileKitSpec({_OPT_IN[op]}=True) and guard it before exposing it.",
            )

        return super().resolve(op)

    # ....................... #

    upload = facade_op(
        StoredFileKernelOp.UPLOAD,
        uc=UploadStoredFile,
    )
    """Upload a file (pending row; blob completes after commit)."""

    initiate_upload = facade_op(
        StoredFileKernelOp.INITIATE_UPLOAD,
        uc=InitiateStoredFileUpload,
    )
    """Start a direct upload: a pending row plus a presigned PUT or a multipart session
    (opt-in: ``direct_uploads``)."""

    presign_upload_part = facade_op(
        StoredFileKernelOp.PRESIGN_UPLOAD_PART,
        uc=PresignStoredFileUploadPart,
    )
    """Presign one part of a direct multipart upload (opt-in: ``direct_uploads``)."""

    complete_upload = facade_op(
        StoredFileKernelOp.COMPLETE_UPLOAD,
        uc=CompleteStoredFileUpload,
    )
    """Verify a direct upload and mark the file ready (opt-in: ``direct_uploads``)."""

    download = facade_op(
        StoredFileKernelOp.DOWNLOAD,
        uc=DownloadStoredFile,
    )
    """Download blob bytes (whole body, buffered)."""

    download_stream = facade_op(
        StoredFileKernelOp.DOWNLOAD_STREAM,
        uc=DownloadStoredFileStream,
    )
    """Open a bounded-memory download stream (opt-in: ``stream_downloads``)."""

    download_range = facade_op(
        StoredFileKernelOp.DOWNLOAD_RANGE,
        uc=DownloadStoredFileRange,
    )
    """Read an inclusive byte range (opt-in: ``range_downloads``)."""

    presign_download = facade_op(
        StoredFileKernelOp.PRESIGN_DOWNLOAD,
        uc=PresignStoredFileDownload,
    )
    """Mint a presigned download URL (opt-in: ``presign_downloads``)."""

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
