from forze.application.execution import UsecaseRegistry
from forze.application.usecases.storage import (
    DeleteObject,
    DownloadObject,
    ListObjects,
    UploadObject,
)

from .operations import StorageOperation

# ----------------------- #


def build_storage_registry(bucket: str) -> UsecaseRegistry:
    """Build a usecase registry for storage operations in a bucket."""

    return UsecaseRegistry(
        {
            StorageOperation.UPLOAD: lambda ctx: UploadObject(
                ctx=ctx,
                storage=ctx.storage(bucket),
            ),
            StorageOperation.LIST: lambda ctx: ListObjects(
                ctx=ctx,
                storage=ctx.storage(bucket),
            ),
            StorageOperation.DOWNLOAD: lambda ctx: DownloadObject(
                ctx=ctx,
                storage=ctx.storage(bucket),
            ),
            StorageOperation.DELETE: lambda ctx: DeleteObject(
                ctx=ctx,
                storage=ctx.storage(bucket),
            ),
        }
    )
