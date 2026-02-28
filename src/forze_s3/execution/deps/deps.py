from forze.application.contracts.storage import (
    StorageDepPort,
    StoragePort,
)
from forze.application.execution import ExecutionContext
from forze.base.typing import conforms_to

from ...adapters import S3StorageAdapter
from .keys import S3ClientDepKey

# ----------------------- #


@conforms_to(StorageDepPort)
def s3_storage(context: ExecutionContext, bucket: str) -> StoragePort:
    s3_client = context.dep(S3ClientDepKey)

    return S3StorageAdapter(client=s3_client, bucket=bucket)
