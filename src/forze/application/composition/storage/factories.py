from forze.application.contracts.storage import StorageSpec
from forze.application.execution.registry import OperationRegistry
from forze.application.handlers.storage import (
    DeleteObject,
    DownloadObject,
    ListObjects,
    UploadObject,
)
from forze.base.primitives import StrKeyNamespace

from .operations import StorageKernelOp

# ----------------------- #


def build_storage_registry(
    spec: StorageSpec,
    *,
    ns: StrKeyNamespace | None = None,
) -> OperationRegistry:
    """Build a usecase registry for storage operations in a bucket."""

    ns = ns or StrKeyNamespace(prefix=spec.name)

    return OperationRegistry(
        handlers={
            ns.key(StorageKernelOp.UPLOAD): lambda ctx: UploadObject(
                storage=ctx.storage(spec),
            ),
            ns.key(StorageKernelOp.LIST): lambda ctx: ListObjects(
                storage=ctx.storage(spec),
            ),
            ns.key(StorageKernelOp.DOWNLOAD): lambda ctx: DownloadObject(
                storage=ctx.storage(spec),
            ),
            ns.key(StorageKernelOp.DELETE): lambda ctx: DeleteObject(
                storage=ctx.storage(spec),
            ),
        }
    )
