from forze.application.contracts.storage import StorageSpec
from forze.application.execution import (
    OperationNamespace,
    UsecaseRegistry,
    operation_namespace_for,
)
from forze.application.usecases.storage import (
    DeleteObject,
    DownloadObject,
    ListObjects,
    UploadObject,
)

from .operations import StorageKernelOp

# ----------------------- #


def build_storage_registry(
    spec: StorageSpec,
    *,
    namespace: OperationNamespace | None = None,
) -> UsecaseRegistry:
    """Build a usecase registry for storage operations in a bucket."""

    ops = namespace or operation_namespace_for(spec)

    return UsecaseRegistry(
        {
            StorageKernelOp.UPLOAD: lambda ctx: UploadObject(
                ctx=ctx,
                storage=ctx.storage(spec),
            ),
            StorageKernelOp.LIST: lambda ctx: ListObjects(
                ctx=ctx,
                storage=ctx.storage(spec),
            ),
            StorageKernelOp.DOWNLOAD: lambda ctx: DownloadObject(
                ctx=ctx,
                storage=ctx.storage(spec),
            ),
            StorageKernelOp.DELETE: lambda ctx: DeleteObject(
                ctx=ctx,
                storage=ctx.storage(spec),
            ),
        },
        namespace=ops,
    )
