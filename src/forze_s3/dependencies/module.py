from forze.application.contracts.storage import (
    StorageDepKey,
    StorageDepPort,
    StoragePort,
)
from forze.application.execution import Deps, DepsModule, ExecutionContext
from forze.base.typing import conforms_to

from ..adapters import S3StorageAdapter
from ..kernel.platform import S3Client
from .keys import S3ClientDepKey

# ----------------------- #


@conforms_to(StorageDepPort)
def s3_storage(context: ExecutionContext, bucket: str) -> StoragePort:
    s3_client = context.dep(S3ClientDepKey)

    return S3StorageAdapter(client=s3_client, bucket=bucket)


# ....................... #


def s3_module(client: S3Client) -> DepsModule:
    def module() -> Deps:
        return Deps(
            {
                S3ClientDepKey: client,
                StorageDepKey: s3_storage,
            }
        )

    return module
