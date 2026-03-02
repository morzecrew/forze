"""Factory function for S3 storage port adapter."""

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
    """Build a S3-backed storage port for the given bucket.

    :param context: Execution context for resolving the S3 client.
    :param bucket: Bucket name for object storage operations.
    :returns: Storage port backed by :class:`S3StorageAdapter`.
    """
    s3_client = context.dep(S3ClientDepKey)

    return S3StorageAdapter(client=s3_client, bucket=bucket)
