from forze.application.contracts.storage import StorageSpec
from forze.application.execution.operations import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from .dto import (
    ListedObjects,
    ListObjectsRequestDTO,
    StoredObjectDTO,
    UploadObjectRequestDTO,
)
from .handlers import (
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

    ns = ns or spec.default_namespace

    reg = OperationRegistry(
        handlers={
            ns.key(StorageKernelOp.UPLOAD): lambda ctx: UploadObject(
                storage=ctx.storage.command(spec),
            ),
            ns.key(StorageKernelOp.LIST): lambda ctx: ListObjects(
                storage=ctx.storage.query(spec),
            ),
            ns.key(StorageKernelOp.DOWNLOAD): lambda ctx: DownloadObject(
                storage=ctx.storage.query(spec),
            ),
            ns.key(StorageKernelOp.DELETE): lambda ctx: DeleteObject(
                storage=ctx.storage.command(spec),
            ),
        }
    )

    # LIST and DOWNLOAD only acquire the read (query) storage port.
    reg = reg.bind(
        StorageKernelOp.LIST, StorageKernelOp.DOWNLOAD, namespace=ns
    ).as_query().finish()

    # DOWNLOAD/DELETE take a raw storage key and DOWNLOAD returns bytes, so they carry
    # no JSON-schema DTO — the description still places them in the catalog.
    return reg.set_descriptors(
        {
            StorageKernelOp.UPLOAD: OperationDescriptor(
                input_type=UploadObjectRequestDTO,
                output_type=StoredObjectDTO,
                description="Upload an object to the bucket and return its metadata.",
            ),
            StorageKernelOp.LIST: OperationDescriptor(
                input_type=ListObjectsRequestDTO,
                output_type=ListedObjects,
                description="List objects in the bucket (optional prefix filter).",
            ),
            StorageKernelOp.DOWNLOAD: OperationDescriptor(
                description="Download an object's bytes by storage key.",
            ),
            StorageKernelOp.DELETE: OperationDescriptor(
                description="Delete an object from the bucket by storage key.",
            ),
        },
        namespace=ns,
    )
