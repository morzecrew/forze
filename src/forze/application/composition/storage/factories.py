from forze.application.contracts.storage import StorageSpec
from forze.application.execution import UsecaseRegistry
from forze.application.usecases.storage import (
    DeleteObject,
    DownloadObject,
    ListObjects,
    UploadObject,
)

from .operations import StorageOperation

# ----------------------- #


def build_storage_registry(spec: StorageSpec) -> UsecaseRegistry:
    """Build a usecase registry for storage operations in a bucket."""

    return UsecaseRegistry(
        {
            StorageOperation.UPLOAD: lambda ctx: UploadObject(
                ctx=ctx,
                storage=ctx.storage(spec),
            ),
            StorageOperation.LIST: lambda ctx: ListObjects(
                ctx=ctx,
                storage=ctx.storage(spec),
            ),
            StorageOperation.DOWNLOAD: lambda ctx: DownloadObject(
                ctx=ctx,
                storage=ctx.storage(spec),
            ),
            StorageOperation.DELETE: lambda ctx: DeleteObject(
                ctx=ctx,
                storage=ctx.storage(spec),
            ),
        }
    )
