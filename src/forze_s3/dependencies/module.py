from forze.application.kernel.context import ExecutionContext
from forze.application.kernel.deps import Deps
from forze.application.kernel.deps.storage import StorageDepKey, StorageDepPort
from forze.application.kernel.ports import StoragePort
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


def s3_module(client: S3Client) -> Deps:
    return Deps(
        {
            S3ClientDepKey: client,
            StorageDepKey: s3_storage,
        }
    )
