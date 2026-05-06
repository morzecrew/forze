import attrs

from forze.application.execution import UsecasesFacade, facade_op
from forze.application.usecases.storage import (
    DeleteObject,
    DownloadObject,
    ListObjects,
    UploadObject,
)

from .operations import StorageOperation

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class StorageUsecasesFacade(UsecasesFacade):
    """Typed facade for storage usecases."""

    upload = facade_op(
        StorageOperation.UPLOAD,
        uc=UploadObject,
    )
    """Upload object usecase."""

    list = facade_op(
        StorageOperation.LIST,
        uc=ListObjects,
    )
    """List objects usecase."""

    download = facade_op(
        StorageOperation.DOWNLOAD,
        uc=DownloadObject,
    )
    """Download object usecase."""

    delete = facade_op(
        StorageOperation.DELETE,
        uc=DeleteObject,
    )
    """Delete object usecase."""
